/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs;

import static org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status.SUCCESS;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.BlockWrite;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.InvalidEncryptionKeyException;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeader;
import org.apache.hadoop.hdfs.protocol.datatransfer.PipelineAck;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.util.ByteArrayManager;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.tracing.Span;
import org.apache.hadoop.tracing.SpanContext;
import org.apache.hadoop.tracing.TraceScope;
import org.apache.hadoop.tracing.Tracer;

import org.apache.hadoop.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheLoader;
import org.apache.hadoop.thirdparty.com.google.common.cache.LoadingCache;
import org.apache.hadoop.thirdparty.com.google.common.cache.RemovalListener;
import org.apache.hadoop.thirdparty.com.google.common.cache.RemovalNotification;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

/*********************************************************************
 *
 * The DataStreamer class is responsible for sending data packets to the
 * datanodes in the pipeline. It retrieves a new blockid and block locations
 * from the namenode, and starts streaming packets to the pipeline of
 * Datanodes. Every packet has a sequence number associated with
 * it. When all the packets for a block are sent out and acks for each
 * if them are received, the DataStreamer closes the current block.
 *
 * The DataStreamer thread picks up packets from the dataQueue, sends it to
 * the first datanode in the pipeline and moves it from the dataQueue to the
 * ackQueue. The ResponseProcessor receives acks from the datanodes. When an
 * successful ack for a packet is received from all datanodes, the
 * ResponseProcessor removes the corresponding packet from the ackQueue.
 *
 * In case of error, all outstanding packets are moved from ackQueue. A new
 * pipeline is setup by eliminating the bad datanode from the original
 * pipeline. The DataStreamer now starts sending packets from the dataQueue.
 *
 *********************************************************************/

// HDFS 客户端（DFSOutputStream）中极其核心的内部类，它继承自 Daemon（守护线程）。
// 它负责将文件数据以**数据包（Packet）的形式通过流水线（Pipeline）**发送到 DataNode。
// DataStreamer 的主要职责是在后台处理繁重的数据传输任务，实现“写数据”与“写流程管理”的解耦：
// 流水线管理：向 NameNode 申请新数据块（Block）及其存放的 DataNode 列表，并建立 TCP 连接形成传输流水线。
// 数据异步发送：从 dataQueue 队列中取出数据包，发送给流水线中的第一个 DataNode，随后将其移动到 ackQueue 等待确认。
// 响应处理：启动并管理 ResponseProcessor 线程，接收来自 DataNode 链条末端的 ACK 确认信号。
// 错误恢复（Pipeline Recovery）：如果传输中某个 DataNode 发生故障，它负责剔除坏节点、申请新节点（或减量运行）、重新建立流水线并重发 ackQueue 中未被确认的数据。
// 心跳与状态维护：在没有数据发送时发送心跳包，确保连接不被中断。
@InterfaceAudience.Private
class DataStreamer extends Daemon {
  static final Logger LOG = LoggerFactory.getLogger(DataStreamer.class);

  private class RefetchEncryptionKeyPolicy {
    private int fetchEncryptionKeyTimes = 0;
    private InvalidEncryptionKeyException lastException;
    private final DatanodeInfo src;

    RefetchEncryptionKeyPolicy(DatanodeInfo src) {
      this.src = src;
    }
    boolean continueRetryingOrThrow() throws InvalidEncryptionKeyException {
      if (fetchEncryptionKeyTimes >= 2) {
        // hit the same exception twice connecting to the node, so
        // throw the exception and exclude the node.
        throw lastException;
      }
      // Don't exclude this node just yet.
      // Try again with a new encryption key.
      LOG.info("Will fetch a new encryption key and retry, "
          + "encryption key was invalid when connecting to "
          + this.src + ": ", lastException);
      // The encryption key used is invalid.
      dfsClient.clearDataEncryptionKey();
      return true;
    }

    /**
     * Record a connection exception.
     */
    void recordFailure(final InvalidEncryptionKeyException e)
        throws InvalidEncryptionKeyException {
      fetchEncryptionKeyTimes++;
      lastException = e;
    }
  }

  private class StreamerStreams implements java.io.Closeable {
    private Socket sock = null;
    private DataOutputStream out = null;
    private DataInputStream in = null;

    StreamerStreams(final DatanodeInfo src,
        final long writeTimeout, final long readTimeout,
        final Token<BlockTokenIdentifier> blockToken)
        throws IOException {
      sock = createSocketForPipeline(src, 2, dfsClient);

      OutputStream unbufOut = NetUtils.getOutputStream(sock, writeTimeout);
      InputStream unbufIn = NetUtils.getInputStream(sock, readTimeout);
      IOStreamPair saslStreams = dfsClient.saslClient
          .socketSend(sock, unbufOut, unbufIn, dfsClient, blockToken, src);
      unbufOut = saslStreams.out;
      unbufIn = saslStreams.in;
      out = new DataOutputStream(new BufferedOutputStream(unbufOut,
          DFSUtilClient.getSmallBufferSize(dfsClient.getConfiguration())));
      in = new DataInputStream(unbufIn);
    }

    void sendTransferBlock(final DatanodeInfo[] targets,
        final StorageType[] targetStorageTypes,
        final String[] targetStorageIDs,
        final Token<BlockTokenIdentifier> blockToken) throws IOException {
      //send the TRANSFER_BLOCK request
      new Sender(out).transferBlock(block.getCurrentBlock(), blockToken,
          dfsClient.clientName, targets, targetStorageTypes,
          targetStorageIDs);
      out.flush();
      //ack
      BlockOpResponseProto transferResponse = BlockOpResponseProto
          .parseFrom(PBHelperClient.vintPrefixed(in));
      if (SUCCESS != transferResponse.getStatus()) {
        throw new IOException("Failed to add a datanode. Response status: "
            + transferResponse.getStatus());
      }
    }

    @Override
    public void close() throws IOException {
      IOUtils.closeStream(in);
      IOUtils.closeStream(out);
      IOUtils.closeSocket(sock);
    }
  }

  static class BlockToWrite {
    private ExtendedBlock currentBlock;

    BlockToWrite(ExtendedBlock block) {
      setCurrentBlock(block);
    }

    synchronized ExtendedBlock getCurrentBlock() {
      return currentBlock == null ? null : new ExtendedBlock(currentBlock);
    }

    synchronized long getNumBytes() {
      return currentBlock == null ? 0 : currentBlock.getNumBytes();
    }

    synchronized void setCurrentBlock(ExtendedBlock block) {
      currentBlock = (block == null || block.getLocalBlock() == null) ?
          null : new ExtendedBlock(block);
    }

    synchronized void setNumBytes(long numBytes) {
      assert currentBlock != null;
      currentBlock.setNumBytes(numBytes);
    }

    synchronized void setGenerationStamp(long generationStamp) {
      assert currentBlock != null;
      currentBlock.setGenerationStamp(generationStamp);
    }

    @Override
    public synchronized String toString() {
      return currentBlock == null ? "null" : currentBlock.toString();
    }
  }

  /**
   * Create a socket for a write pipeline
   *
   * @param first the first datanode
   * @param length the pipeline length
   * @param client client
   * @return the socket connected to the first datanode
   */
  static Socket createSocketForPipeline(final DatanodeInfo first,
      final int length, final DFSClient client) throws IOException {
    final DfsClientConf conf = client.getConf();
    final String dnAddr = first.getXferAddr(conf.isConnectToDnViaHostname());
    LOG.debug("Connecting to datanode {}", dnAddr);
    final InetSocketAddress isa = NetUtils.createSocketAddr(dnAddr);
    final Socket sock = client.socketFactory.createSocket();
    final int timeout = client.getDatanodeReadTimeout(length);
    NetUtils.connect(sock, isa, client.getRandomLocalInterfaceAddr(),
        conf.getSocketTimeout());
    sock.setTcpNoDelay(conf.getDataTransferTcpNoDelay());
    sock.setSoTimeout(timeout);
    sock.setKeepAlive(true);
    if (conf.getSocketSendBufferSize() > 0) {
      sock.setSendBufferSize(conf.getSocketSendBufferSize());
    }
    LOG.debug("Send buf size {}", sock.getSendBufferSize());
    return sock;
  }

  /**
   * if this file is lazy persist
   *
   * @param stat the HdfsFileStatus of a file
   * @return if this file is lazy persist
   */
  static boolean isLazyPersist(HdfsFileStatus stat) {
    return stat.getStoragePolicy() == HdfsConstants.MEMORY_STORAGE_POLICY_ID;
  }

  /**
   * release a list of packets to ByteArrayManager
   *
   * @param packets packets to be release
   * @param bam ByteArrayManager
   */
  private static void releaseBuffer(List<DFSPacket> packets, ByteArrayManager bam) {
    for(DFSPacket p : packets) {
      p.releaseBuffer(bam);
    }
    packets.clear();
  }

  class LastExceptionInStreamer extends ExceptionLastSeen {
    /**
     * Check if there already is an exception.
     */
    @Override
    synchronized void check(boolean resetToNull) throws IOException {
      final IOException thrown = get();
      if (thrown != null) {
        if (LOG.isTraceEnabled()) {
          // wrap and print the exception to know when the check is called
          LOG.trace("Got Exception while checking, " + DataStreamer.this,
              new Throwable(thrown));
        }
        super.check(resetToNull);
      }
    }
  }

  enum ErrorType {
    NONE, INTERNAL, EXTERNAL
  }

  static class ErrorState {
    ErrorType error = ErrorType.NONE;
    private int badNodeIndex = -1;
    private boolean waitForRestart = true;
    private int restartingNodeIndex = -1;
    private long restartingNodeDeadline = 0;
    private final long datanodeRestartTimeout;

    ErrorState(long datanodeRestartTimeout) {
      this.datanodeRestartTimeout = datanodeRestartTimeout;
    }

    synchronized void resetInternalError() {
      if (hasInternalError()) {
        error = ErrorType.NONE;
      }
      badNodeIndex = -1;
      restartingNodeIndex = -1;
      restartingNodeDeadline = 0;
      waitForRestart = true;
    }

    synchronized void reset() {
      error = ErrorType.NONE;
      badNodeIndex = -1;
      restartingNodeIndex = -1;
      restartingNodeDeadline = 0;
      waitForRestart = true;
    }

    synchronized boolean hasInternalError() {
      return error == ErrorType.INTERNAL;
    }

    synchronized boolean hasExternalError() {
      return error == ErrorType.EXTERNAL;
    }

    synchronized boolean hasError() {
      return error != ErrorType.NONE;
    }

    synchronized boolean hasDatanodeError() {
      return error == ErrorType.INTERNAL && isNodeMarked();
    }

    synchronized void setInternalError() {
      this.error = ErrorType.INTERNAL;
    }

    synchronized void setExternalError() {
      if (!hasInternalError()) {
        this.error = ErrorType.EXTERNAL;
      }
    }

    synchronized void setBadNodeIndex(int index) {
      this.badNodeIndex = index;
    }

    synchronized int getBadNodeIndex() {
      return badNodeIndex;
    }

    synchronized int getRestartingNodeIndex() {
      return restartingNodeIndex;
    }

    synchronized void initRestartingNode(int i, String message,
        boolean shouldWait) {
      restartingNodeIndex = i;
      if (shouldWait) {
        restartingNodeDeadline = Time.monotonicNow() + datanodeRestartTimeout;
        // If the data streamer has already set the primary node
        // bad, clear it. It is likely that the write failed due to
        // the DN shutdown. Even if it was a real failure, the pipeline
        // recovery will take care of it.
        badNodeIndex = -1;
      } else {
        this.waitForRestart = false;
      }
      LOG.info(message);
    }

    synchronized boolean isRestartingNode() {
      return restartingNodeIndex >= 0;
    }

    synchronized boolean isNodeMarked() {
      return badNodeIndex >= 0 || (isRestartingNode() && doWaitForRestart());
    }

    /**
     * This method is used when no explicit error report was received, but
     * something failed. The first node is a suspect or unsure about the cause
     * so that it is marked as failed.
     */
    synchronized void markFirstNodeIfNotMarked() {
      // There should be no existing error and no ongoing restart.
      if (!isNodeMarked()) {
        badNodeIndex = 0;
      }
    }

    synchronized void adjustState4RestartingNode() {
      // Just took care of a node error while waiting for a node restart
      if (restartingNodeIndex >= 0) {
        // If the error came from a node further away than the restarting
        // node, the restart must have been complete.
        if (badNodeIndex > restartingNodeIndex) {
          restartingNodeIndex = -1;
        } else if (badNodeIndex < restartingNodeIndex) {
          // the node index has shifted.
          restartingNodeIndex--;
        } else if (waitForRestart) {
          throw new IllegalStateException("badNodeIndex = " + badNodeIndex
              + " = restartingNodeIndex = " + restartingNodeIndex);
        }
      }

      if (!isRestartingNode()) {
        error = ErrorType.NONE;
      }
      badNodeIndex = -1;
    }

    synchronized void checkRestartingNodeDeadline(DatanodeInfo[] nodes) {
      if (restartingNodeIndex >= 0) {
        if (error == ErrorType.NONE) {
          throw new IllegalStateException("error=false while checking" +
              " restarting node deadline");
        }

        // check badNodeIndex
        if (badNodeIndex == restartingNodeIndex) {
          // ignore, if came from the restarting node
          badNodeIndex = -1;
        }
        // not within the deadline
        if (Time.monotonicNow() >= restartingNodeDeadline) {
          // expired. declare the restarting node dead
          restartingNodeDeadline = 0;
          final int i = restartingNodeIndex;
          restartingNodeIndex = -1;
          LOG.warn("Datanode " + i + " did not restart within "
              + datanodeRestartTimeout + "ms: " + nodes[i]);
          // Mark the restarting node as failed. If there is any other failed
          // node during the last pipeline construction attempt, it will not be
          // overwritten/dropped. In this case, the restarting node will get
          // excluded in the following attempt, if it still does not come up.
          if (badNodeIndex == -1) {
            badNodeIndex = i;
          }
        }
      }
    }

    boolean doWaitForRestart() {
      return waitForRestart;
    }
  }
  // 标识当前 Streamer 是否已被显式关闭。
  private volatile boolean streamerClosed = false;
  // 该对象实时维护着当前正在写入的数据块（Block）的元数据状态。
  protected final BlockToWrite block; // its length is number of bytes acked
  // 访问 DataNode 的安全通行证（Access Token）
  // 在开启了 Kerberos 认证或安全检查的 Hadoop 集群中，客户端不能直接操作 DataNode。NameNode 会在分配数据块时发放一个 Token。
  protected Token<BlockTokenIdentifier> accessToken;
  // 指向流水线第一个节点的 DataOutputStream，用于写入数据。
  private DataOutputStream blockStream;
  // 指向数据流向管道（Pipeline）中第一个 DataNode 的输入流。
  // 核心任务是接收从 DataNode 链路反向传回的所有响应信息。
  // ACK 确认机制：在 HDFS 写入过程中，数据包（Packet）是单向向下游 DataNode 发送的，而确认信号（ACK）则是沿着管道逆流而上的。
  // blockReplyStream 负责读取这些 ACK 包，并将结果交给 ResponseProcessor 线程解析。
  private DataInputStream blockReplyStream;
  // 专门负责异步读取 DataNode 回传的 ACK
  private ResponseProcessor response = null;
  private final Object nodesLock = new Object();
  // 当前流水线中的 DataNode 列表（由 NameNode 分配）。
  private volatile DatanodeInfo[] nodes = null; // list of targets for current block
  // 记录当前数据块（Block）所在的介质存储类型
  // 异构存储支持：HDFS 支持多种存储介质（如 SSD、DISK、ARCHIVE 或 RAM_DISK）。该数组的大小与流水线中的 DataNode 数量一致。
  private volatile StorageType[] storageTypes = null;
  // 记录流水线中每个 DataNode 上具体的存储槽位（Storage Slot）ID
  private volatile String[] storageIDs = null;
  // 集中管理数据流传输过程中的错误状态和故障恢复信息。
  private final ErrorState errorState;
  // 构建阶段：如 PIPELINE_SETUP_CREATE（创建）、DATA_STREAMING（传输中）、PIPELINE_CLOSE（关闭）。
  private volatile BlockConstructionStage stage;  // block construction stage
  // 记录当前数据块（Block）中已经发送出去的字节总数。
  // 每当一个数据包（Packet）成功通过网络流发送给第一个 DataNode 时，此值会更新。注意，它表示的是“已发出”，并不代表下游 DataNode 已经“已确认（Acked）”。
  protected long bytesSent = 0; // number of bytes that've been sent
  // 标识该文件是否采用了延迟持久化策略。
  // 如果文件的存储策略是 RAM_DISK（内存存储），此值为 true。这意味着数据会优先写入 DataNode 的内存，随后再异步持久化到磁盘。
  private final boolean isLazyPersistFile;
  // 记录上一次发送数据包的时间戳（以毫秒为单位）。
  private long lastPacket;

  /** Nodes have been used in the pipeline before and have failed. */
  // 黑名单列表，记录在当前流水线中已经发生故障的 DataNode。
  // 在进行流水线恢复（Pipeline Recovery）时，failed 列表中的节点会被排除在候选节点之外，避免再次连接到已知的不可用节点。
  private final List<DatanodeInfo> failed = new ArrayList<>();
  /** Restarting Nodes */
  // 记录当前正在重启中的 DataNode。
  private List<DatanodeInfo> restartingNodes = new ArrayList<>();
  /** The times have retried to recover pipeline, for the same packet. */
  // 针对同一个数据包尝试恢复流水线的次数。
  private volatile int pipelineRecoveryCount = 0;
  /** Has the current block been hflushed? */
  // 标识当前 Block 是否执行过 hflush 或 hsync 操作。
  private boolean isHflushed = false;
  /** Append on an existing block? */
  // 标识当前操作是否为**追加（Append）**模式
  // 如果是对已有文件进行追加写入，而非创建新文件，该值为 true。这会影响流水线建立阶段（Stage）的选择
  private final boolean isAppend;
  // 生成数据包的唯一序列号。
  // 每个 DFSPacket 都有一个递增的序列号。DataNode 会回传对应的序列号来确认（ACK）数据已接收。
  private long currentSeqno = 0;
  // 记录最后一个**进入发送队列（dataQueue）**的数据包序列号。
  private long lastQueuedSeqno = -1;
  // 记录最后一个**收到成功确认（ACK）**的数据包序列号。
  // 通过对比 lastQueuedSeqno 和 lastAckedSeqno，DataStreamer 可以知道当前有多少数据还在“飞行中（In-flight）”未被确认。
  private long lastAckedSeqno = -1;
  // 当前数据块内**已写入（写入 dataQueue）**的字节偏移量。
  // 用于判断当前 Block 是否已写满。如果 bytesCurBlock 达到了 BlockSize，Streamer 就会准备结束当前 Block 并申请下一个。
  private long bytesCurBlock = 0; // bytes written in current block
  // 记录传输过程中遇到的最后一个异常，供客户端主线程查询。
  private final LastExceptionInStreamer lastException = new LastExceptionInStreamer();
  // 维护指向流水线（Pipeline）中第一个 DataNode 的 TCP 套接字连接。
  private Socket s;
  // DFSClient 是 HDFS 客户端的上下文对象，包含了全局配置信息（DfsClientConf）、RPC 代理（用于与 NameNode 通信）以及文件系统的状态。
  protected final DFSClient dfsClient;
  // 作用：当前正在写入的文件在 HDFS 中的完整路径（例如 /user/data/file.txt）。
  protected final String src;
  /** Only for DataTransferProtocol.writeBlock(..) */
  // 用于生成和验证数据校验和的校验器（如 CRC32 或 CRC32C）
  final DataChecksum checksum4WriteBlock;
  // 进度回调接口。
  final Progressable progress;
  // 文件的元数据状态快照。
  // 包含了文件的 Block 大小、副本数、权限、存储策略等信息。DataStreamer 需要这些信息来决定何时切分 Block。
  protected final HdfsFileStatus stat;
  // appending to existing partial block
  // 标识当前是否正在处理追加过程中的残缺 Chunk。
  // 如果追加写入的起始位置不在一个完整 Chunk（通常 512 字节）的边界上，系统需要特殊处理这个“尾部”数据，确保校验和能正确衔接。
  private volatile boolean appendChunk = false;
  // both dataQueue and ackQueue are protected by dataQueue lock
  // 数据队列
  // 存放等待发送到 DataNode 的数据包（DFSPacket）。
  protected final LinkedList<DFSPacket> dataQueue = new LinkedList<>();
  // 记录每个数据包的发送时间戳。
  private final Map<Long, Long> packetSendTime = new HashMap<>();
  // 确认队列
  // 存放已发送但尚未收到所有 DataNode 确认的数据包。
  private final LinkedList<DFSPacket> ackQueue = new LinkedList<>();
  // 配置 DataNode 端对该文件数据的缓存预读策略。
  // 决定 DataNode 在写入数据时，是否应该将其放入操作系统的 Page Cache（预读/丢弃）。这通常用于优化大文件的顺序读写性能。
  private final AtomicReference<CachingStrategy> cachingStrategy;
  // 为了减少高频率写入时频繁创建/销毁 byte[] 导致的 GC 压力，HDFS 使用内存池来复用缓冲区。DataStreamer 在数据发送完成后会通过它回收缓冲区。
  private final ByteArrayManager byteArrayManager;
  //persist blocks on namenode
  // 标识是否需要向 NameNode 持久化数据块状态。
  // 当文件处于写入过程中，特别是发生异常或强制调用 hsync 时，客户端会要求 NameNode 更新并记录当前已分配 Block 的状态。
  private final AtomicBoolean persistBlocks = new AtomicBoolean(false);
  // 故障注入开关。
  // 手动将其设为 true 可以模拟数据包发送失败的场景，用以验证 DataStreamer 的容错恢复逻辑是否健壮。
  private boolean failPacket = false;
  // 慢 IO 日志阈值。
  // 如果一个数据包的发送和确认耗时超过这个阈值（毫秒），客户端会在日志中记录警告，帮助管理员识别网络或磁盘性能瓶颈。
  private final long dfsclientSlowLogThresholdMs;
  // 人工限速/延迟注入。
  private long artificialSlowdown = 0;
  // List of congested data nodes. The stream will back off if the DataNodes
  // are congested
  // 记录当前处于拥塞状态的 DataNode。
  // 如果 DataNode 返回的 ACK 响应中包含拥塞标志（ECN 等），客户端会将该节点加入此列表。
  private final List<DatanodeInfo> congestedNodes = new ArrayList<>();
  // 记录每个节点被判定为“慢节点”的次数。
  private final Map<DatanodeInfo, Integer> slowNodeMap = new HashMap<>();
  // 拥塞退避的平均时间和最大时间。
  // 当检测到拥塞时，DataStreamer 会根据这些参数计算一个随机的休眠时间（退避算法），主动降低发送频率。
  private int congestionBackOffMeanTimeInMs;
  private int congestionBackOffMaxTimeInMs;
  private int lastCongestionBackoffTime;
  // 流水线恢复的最大重试次数。
  private int maxPipelineRecoveryRetries;
  // 慢节点转坏节点阈值。
  // 如果一个节点连续多次被判定为慢节点，超过此阈值后，客户端可能会主动将其视为故障节点（Bad Node），从而触发 Pipeline 恢复来替换它。
  private int markSlowNodeAsBadNodeThreshold;
  // 在当前文件的写入周期内，那些由于故障或超时被剔除的节点会被存入其中，以防在申请新 Block 或重建流水线时再次选中它们。
  protected final LoadingCache<DatanodeInfo, DatanodeInfo> excludedNodes;
  // 倾向节点列表（Preferred Nodes）。
  // 用户在创建流时可以指定希望将数据存储在哪些特定节点上。NameNode 在分配块时会尽量（但不保证）满足这一倾向。
  private final String[] favoredNodes;
  // 申请块时的标志位。
  // 用于向 NameNode 申请新块时传递额外信息，例如 NO_LOCAL_WRITE（不优先写本地）或 IGNORE_QUOTA（忽略配额检查，某些特殊操作使用）
  private final EnumSet<AddBlockFlag> addBlockFlags;
  // 主要任务是将客户端的配置信息、文件的元数据以及各种容错策略装载到 Streamer 线程中。
  private DataStreamer(HdfsFileStatus stat, ExtendedBlock block,
                       DFSClient dfsClient, String src,
                       Progressable progress, DataChecksum checksum,
                       AtomicReference<CachingStrategy> cachingStrategy,
                       ByteArrayManager byteArrayManage,
                       boolean isAppend, String[] favoredNodes,
                       EnumSet<AddBlockFlag> flags) {
    // 将传入的 ExtendedBlock 包装成可同步更新的 BlockToWrite 对象
    this.block = new BlockToWrite(block);
    this.dfsClient = dfsClient;
    // 记录文件在 HDFS 中的路径，用于日志输出
    this.src = src;
    this.progress = progress;
    this.stat = stat;
    this.checksum4WriteBlock = checksum;
    this.cachingStrategy = cachingStrategy;
    // 关联内存池管理器，用于高效复用字节数组缓冲区
    this.byteArrayManager = byteArrayManage;
    // 通过文件存储策略判断是否为“延迟持久化”（写入 RAM_DISK）
    this.isLazyPersistFile = isLazyPersist(stat);
    this.isAppend = isAppend;
    this.favoredNodes = favoredNodes;
    final DfsClientConf conf = dfsClient.getConf();
    // 设置慢 IO 警告阈值，超过此值会打印 "Slow DFS write" 日志
    this.dfsclientSlowLogThresholdMs = conf.getSlowIoWarningThresholdMs();
    // 初始化排除节点缓存，定义了发生故障的节点在多长时间内不再被选中
    this.excludedNodes = initExcludedNodes(conf.getExcludedNodesCacheExpiry());
    this.errorState = new ErrorState(conf.getDatanodeRestartTimeout());
    this.addBlockFlags = flags;
    this.maxPipelineRecoveryRetries = conf.getMaxPipelineRecoveryRetries();
    this.markSlowNodeAsBadNodeThreshold = conf.getMarkSlowNodeAsBadNodeThreshold();
    // 读取退避随机时间的平均值（Mean Time）
    congestionBackOffMeanTimeInMs = dfsClient.getConfiguration().getInt(
        HdfsClientConfigKeys.DFS_CLIENT_CONGESTION_BACKOFF_MEAN_TIME,
        HdfsClientConfigKeys.DFS_CLIENT_CONGESTION_BACKOFF_MEAN_TIME_DEFAULT);
    // 读取退避随机时间的最大值（Max Time）
    congestionBackOffMaxTimeInMs = dfsClient.getConfiguration().getInt(
        HdfsClientConfigKeys.DFS_CLIENT_CONGESTION_BACKOFF_MAX_TIME,
        HdfsClientConfigKeys.DFS_CLIENT_CONGESTION_BACKOFF_MAX_TIME_DEFAULT);
    // 如果平均时间 <= 0，打印警告并准备使用默认值
    if (congestionBackOffMeanTimeInMs <= 0) {
      LOG.warn("Configuration: {} is not appropriate, using default value: {}",
          HdfsClientConfigKeys.DFS_CLIENT_CONGESTION_BACKOFF_MEAN_TIME,
          HdfsClientConfigKeys.DFS_CLIENT_CONGESTION_BACKOFF_MEAN_TIME_DEFAULT);
    }
    // 如果最大时间 <= 0，同上
    if (congestionBackOffMaxTimeInMs <= 0) {
      LOG.warn("Configuration: {} is not appropriate, using default value: {}",
          HdfsClientConfigKeys.DFS_CLIENT_CONGESTION_BACKOFF_MAX_TIME,
          HdfsClientConfigKeys.DFS_CLIENT_CONGESTION_BACKOFF_MAX_TIME_DEFAULT);
    }
    // 如果最大时间小于平均时间，这在数学逻辑上是错误的，打印警告
    if (congestionBackOffMaxTimeInMs < congestionBackOffMeanTimeInMs) {
      LOG.warn("Configuration: {} can not less than {}, using their default values.",
          HdfsClientConfigKeys.DFS_CLIENT_CONGESTION_BACKOFF_MAX_TIME,
          HdfsClientConfigKeys.DFS_CLIENT_CONGESTION_BACKOFF_MEAN_TIME);
    }
    // 最终保险：如果上述任何一个配置无效，强制将两者统一设为系统默认值
    if (congestionBackOffMeanTimeInMs <= 0 || congestionBackOffMaxTimeInMs <= 0 ||
        congestionBackOffMaxTimeInMs < congestionBackOffMeanTimeInMs) {
      congestionBackOffMeanTimeInMs =
          HdfsClientConfigKeys.DFS_CLIENT_CONGESTION_BACKOFF_MEAN_TIME_DEFAULT;
      congestionBackOffMaxTimeInMs =
          HdfsClientConfigKeys.DFS_CLIENT_CONGESTION_BACKOFF_MAX_TIME_DEFAULT;
    }

  }

  /**
   * construction with tracing info
   */
  DataStreamer(HdfsFileStatus stat, ExtendedBlock block, DFSClient dfsClient,
               String src, Progressable progress, DataChecksum checksum,
               AtomicReference<CachingStrategy> cachingStrategy,
               ByteArrayManager byteArrayManage, String[] favoredNodes,
               EnumSet<AddBlockFlag> flags) {
    this(stat, block, dfsClient, src, progress, checksum, cachingStrategy,
        byteArrayManage, false, favoredNodes, flags);
    stage = BlockConstructionStage.PIPELINE_SETUP_CREATE;
  }

  /**
   * Construct a data streamer for appending to the last partial block
   * @param lastBlock last block of the file to be appended
   * @param stat status of the file to be appended
   */
  DataStreamer(LocatedBlock lastBlock, HdfsFileStatus stat, DFSClient dfsClient,
               String src, Progressable progress, DataChecksum checksum,
               AtomicReference<CachingStrategy> cachingStrategy,
               ByteArrayManager byteArrayManage) {
    this(stat, lastBlock.getBlock(), dfsClient, src, progress, checksum, cachingStrategy,
        byteArrayManage, true, null, null);
    stage = BlockConstructionStage.PIPELINE_SETUP_APPEND;
    bytesSent = block.getNumBytes();
    accessToken = lastBlock.getBlockToken();
  }

  /**
   * Set pipeline in construction
   *
   * @param lastBlock the last block of a file
   * @throws IOException
   */

  // 更新当前用于写入数据的 DataNode 列表及其相关的存储信息。
  void setPipelineInConstruction(LocatedBlock lastBlock) throws IOException {
    // setup pipeline to append to the last block XXX retries??
    setPipeline(lastBlock);
    if (nodes.length < 1) {
      throw new IOException("Unable to retrieve blocks locations " +
          " for last block " + block + " of file " + src);
    }
  }

  void setAccessToken(Token<BlockTokenIdentifier> t) {
    this.accessToken = t;
  }
  // 更新当前用于写入数据的 DataNode 列表及其相关的存储信息。
  private void setPipeline(LocatedBlock lb) {
    setPipeline(lb.getLocations(), lb.getStorageTypes(), lb.getStorageIDs());
  }

  // 更新当前用于写入数据的 DataNode 列表及其相关的存储信息。
  // 这通常发生在块刚开始写入、或者中途某个 DataNode 发生故障需要更换管道（Pipeline Recovery）时。
  private void setPipeline(DatanodeInfo[] nodes, StorageType[] storageTypes,
                           String[] storageIDs) {
    synchronized (nodesLock) {
      this.nodes = nodes;
    }
    this.storageTypes = storageTypes;
    this.storageIDs = storageIDs;
  }

  /**
   * Initialize for data streaming
   */
  // 当写管道（Pipeline）建立完毕，准备正式开始发送数据包（Packets）之前，会调用此方法进行状态初始化。
  private void initDataStreaming() {
    // 设置当前线程的名称。
    this.setName("DataStreamer for file " + src +
        " block " + block);
    if (LOG.isDebugEnabled()) {
      LOG.debug("nodes {} storageTypes {} storageIDs {}",
          Arrays.toString(nodes),
          Arrays.toString(storageTypes),
          Arrays.toString(storageIDs));
    }
    // HDFS 写入是异步的。DataStreamer 负责发包，而 ResponseProcessor 负责接收来自 DataNode 的 ACK（确认字符）。
    // 它被初始化为监听这一组 nodes 的反馈。
    response = new ResponseProcessor(nodes);
    response.start();
    stage = BlockConstructionStage.DATA_STREAMING;
    lastPacket = Time.monotonicNow();
  }
  // 当一个数据块（Block）写满、写入完成或因为异常需要终止当前块的写入时，会调用此方法来释放资源并重置状态。
  protected void endBlock() {
    LOG.debug("Closing old block {}", block);
    this.setName("DataStreamer for file " + src);
    closeResponder();
    closeStream();
    setPipeline(null, null, null);
    stage = BlockConstructionStage.PIPELINE_SETUP_CREATE;
  }

  private boolean shouldStop() {
    return streamerClosed || errorState.hasError() || !dfsClient.clientRunning;
  }

  /*
   * streamer thread is the only thread that opens streams to datanode,
   * and closes them. Any error recovery is also done by this thread.
   */
  // 负责从本地队列抓取数据包并跨网络发送。
  @Override
  public void run() {
    TraceScope scope = null;
    // 只要 Streamer 没关闭且 DFS 客户端还在运行，就持续循环。
    while (!streamerClosed && dfsClient.clientRunning) {
      // if the Responder encountered an error, shutdown Responder
      if (errorState.hasError()) {
        closeResponder();
      }

      DFSPacket one;
      try {
        // process datanode IO errors if any
        // 如果之前有错误，这里会尝试重建 Pipeline。
        boolean doSleep = processDatanodeOrExternalError();

        synchronized (dataQueue) {
          // wait for a packet to be sent.
          // 如果在数据传输阶段且空闲，会计算心跳间隔并通过 wait(timeout) 定时唤醒发送心跳包，保持连接不被 DataNode 断开。
          while ((!shouldStop() && dataQueue.isEmpty()) || doSleep) {
            long timeout = 1000;
            if (stage == BlockConstructionStage.DATA_STREAMING) {
              timeout = sendHeartbeat();
            }
            try {
              dataQueue.wait(timeout);
            } catch (InterruptedException  e) {
              LOG.debug("Thread interrupted", e);
            }
            doSleep = false;
          }
          if (shouldStop()) {
            continue;
          }
          // get packet to be sent.
          one = dataQueue.getFirst(); // regular data packet
          SpanContext[] parents = one.getTraceParents();
          if (parents != null && parents.length > 0) {
            // The original code stored multiple parents in the DFSPacket, and
            // use them ALL here when creating a new Span. We only use the
            // last one FOR NOW. Moreover, we don't activate the Span for now.
            scope = dfsClient.getTracer().
                newScope("dataStreamer", parents[0], false);
            //scope.getSpan().setParents(parents);
          }
        }

        // The DataStreamer has to release the dataQueue before sleeping,
        // otherwise it will cause the ResponseProcessor to accept the ACK delay.
        try {
          backOffIfNecessary();
        } catch (InterruptedException e) {
          LOG.debug("Thread interrupted", e);
        }

        // get new block from namenode.
        LOG.debug("stage={}, {}", stage, this);
        // 创建新块：如果是新块开始，调用 nextBlockOutputStream 向 NameNode 申请新块并连接 DataNode。
        if (stage == BlockConstructionStage.PIPELINE_SETUP_CREATE) {
          LOG.debug("Allocating new block: {}", this);
          setPipeline(nextBlockOutputStream());
          initDataStreaming();
        } else if (stage == BlockConstructionStage.PIPELINE_SETUP_APPEND) {
          // 追加/恢复：如果是 Append 操作或故障恢复，调用相应的 setup 方法重建管道。
          LOG.debug("Append to block {}", block);
          setupPipelineForAppendOrRecovery();
          if (streamerClosed) {
            continue;
          }
          initDataStreaming();
        }

        long lastByteOffsetInBlock = one.getLastByteOffsetBlock();
        if (lastByteOffsetInBlock > stat.getBlockSize()) {
          throw new IOException("BlockSize " + stat.getBlockSize() +
              " < lastByteOffsetInBlock, " + this + ", " + one);
        }
        // 如果是块的最后一个包，需要先确保之前发出的所有包都已经收到了 ACK（确认），然后将状态改为 PIPELINE_CLOSE。
        if (one.isLastPacketInBlock()) {
          // wait for all data packets have been successfully acked
          waitForAllAcks();
          if(shouldStop()) {
            continue;
          }
          stage = BlockConstructionStage.PIPELINE_CLOSE;
        }

        // send the packet
        SpanContext spanContext = null;
        // 数据包从待发送队列 dataQueue 移出，放入等待确认队列 ackQueue。
        // 计时：记录发送时间，用于后续计算 DataNode 的响应速度（慢磁盘检测）。
        synchronized (dataQueue) {
          // move packet from dataQueue to ackQueue
          if (!one.isHeartbeatPacket()) {
            if (scope != null) {
              one.setSpan(scope.span());
              spanContext = scope.span().getContext();
              scope.close();
            }
            scope = null;
            dataQueue.removeFirst();
            ackQueue.addLast(one);
            packetSendTime.put(one.getSeqno(), Time.monotonicNowNanos());
            dataQueue.notifyAll();
          }
        }

        LOG.debug("{} sending {}", this, one);

        // write out data to remote datanode
        try (TraceScope ignored = dfsClient.getTracer().
            newScope("DataStreamer#writeTo", spanContext)) {
          // 真正的 IO 操作，将字节流写入到 Socket 输出流中。
          sendPacket(one);
        } catch (IOException e) {
          // HDFS-3398 treat primary DN is down since client is unable to
          // write to primary DN. If a failed or restarting node has already
          // been recorded by the responder, the following call will have no
          // effect. Pipeline recovery can handle only one node error at a
          // time. If the primary node fails again during the recovery, it
          // will be taken out then.
          errorState.markFirstNodeIfNotMarked();
          throw e;
        }

        // update bytesSent
        long tmpBytesSent = one.getLastByteOffsetBlock();
        if (bytesSent < tmpBytesSent) {
          bytesSent = tmpBytesSent;
        }

        if (shouldStop()) {
          continue;
        }

        // Is this block full?
        // 如果当前块写完了，清理资源，准备下一个块。
        if (one.isLastPacketInBlock()) {
          // wait for the close packet has been acked
          try {
            waitForAllAcks();
          } catch (IOException ioe) {
            // No need to do a close recovery if the last packet was acked.
            // i.e. ackQueue is empty.  waitForAllAcks() can get an exception
            // (e.g. connection reset) while sending a heartbeat packet,
            // if the DN sends the final ack and closes the connection.
            synchronized (dataQueue) {
              if (!ackQueue.isEmpty()) {
                throw ioe;
              }
            }
          }
          if (shouldStop()) {
            continue;
          }

          endBlock();
        }
        if (progress != null) { progress.progress(); }

        // This is used by unit test to trigger race conditions.
        if (artificialSlowdown != 0 && dfsClient.clientRunning) {
          Thread.sleep(artificialSlowdown);
        }
      } catch (Throwable e) {
        // Log warning if there was a real error.
        if (!errorState.isRestartingNode()) {
          // Since their messages are descriptive enough, do not always
          // log a verbose stack-trace WARN for quota exceptions.
          if (e instanceof QuotaExceededException) {
            LOG.debug("DataStreamer Quota Exception", e);
          } else {
            LOG.warn("DataStreamer Exception", e);
          }
        }
        lastException.set(e);
        assert !(e instanceof NullPointerException);
        errorState.setInternalError();
        if (!errorState.isNodeMarked()) {
          // Not a datanode issue
          streamerClosed = true;
        }
      } finally {
        if (scope != null) {
          scope.close();
          scope = null;
        }
      }
    }
    closeInternal();
  }
  // 等待所有的包确认
  private void waitForAllAcks() throws IOException {
    // wait until all data packets have been successfully acked
    synchronized (dataQueue) {
      while (!shouldStop() && !ackQueue.isEmpty()) {
        try {
          // wait for acks to arrive from datanodes
          dataQueue.wait(sendHeartbeat());
        } catch (InterruptedException  e) {
          LOG.debug("Thread interrupted ", e);
        }
      }
    }
  }
  // 执行物理网络 IO 的核心方法。它将内存中封装好的数据包真正推送到 DataNode 管道中。
  private void sendPacket(DFSPacket packet) throws IOException {
    // write out data to remote datanode
    try {
      packet.writeTo(blockStream);
      blockStream.flush();
    } catch (IOException e) {
      // HDFS-3398 treat primary DN is down since client is unable to
      // write to primary DN. If a failed or restarting node has already
      // been recorded by the responder, the following call will have no
      // effect. Pipeline recovery can handle only one node error at a
      // time. If the primary node fails again during the recovery, it
      // will be taken out then.
      errorState.markFirstNodeIfNotMarked();
      throw e;
    }
    lastPacket = Time.monotonicNow();
  }

  private long sendHeartbeat() throws IOException {
    final long heartbeatInterval = dfsClient.getConf().getSocketTimeout()/2;
    long timeout = heartbeatInterval - (Time.monotonicNow() - lastPacket);
    if (timeout <= 0) {
      sendPacket(createHeartbeatPacket());
      timeout = heartbeatInterval;
    }
    return timeout;
  }

  private void closeInternal() {
    closeResponder();       // close and join
    closeStream();
    streamerClosed = true;
    release();
    synchronized (dataQueue) {
      dataQueue.notifyAll();
    }
  }

  /**
   * release the DFSPackets in the two queues
   *
   */
  void release() {
    synchronized (dataQueue) {
      releaseBuffer(dataQueue, byteArrayManager);
      releaseBuffer(ackQueue, byteArrayManager);
    }
  }

  /**
   * wait for the ack of seqno
   *
   * @param seqno the sequence number to be acked
   * @throws IOException
   */
  void waitForAckedSeqno(long seqno) throws IOException {
    try (TraceScope ignored = dfsClient.getTracer().
        newScope("waitForAckedSeqno")) {
      LOG.debug("{} waiting for ack for: {}", this, seqno);
      int dnodes;
      synchronized (nodesLock) {
        dnodes = nodes != null ? nodes.length : 3;
      }
      int writeTimeout = dfsClient.getDatanodeWriteTimeout(dnodes);
      long begin = Time.monotonicNowNanos();
      try {
        synchronized (dataQueue) {
          while (!streamerClosed) {
            checkClosed();
            if (lastAckedSeqno >= seqno) {
              break;
            }
            try {
              dataQueue.wait(1000); // when we receive an ack, we notify on
              long duration = Time.monotonicNowNanos() - begin;
              if (TimeUnit.NANOSECONDS.toMillis(duration) > writeTimeout) {
                LOG.error("No ack received, took {}ms (threshold={}ms). "
                    + "File being written: {}, block: {}, "
                    + "Write pipeline datanodes: {}.",
                    TimeUnit.NANOSECONDS.toMillis(duration), writeTimeout, src, block, nodes);
                throw new InterruptedIOException("No ack received after " +
                    TimeUnit.NANOSECONDS.toSeconds(duration) + "s and a timeout of " +
                    writeTimeout / 1000 + "s");
              }
              // dataQueue
            } catch (InterruptedException ie) {
              throw new InterruptedIOException(
                  "Interrupted while waiting for data to be acknowledged by pipeline");
            }
          }
        }
        checkClosed();
      } catch (ClosedChannelException cce) {
        LOG.debug("Closed channel exception", cce);
      }
      long duration = Time.monotonicNowNanos() - begin;
      if (TimeUnit.NANOSECONDS.toMillis(duration) > dfsclientSlowLogThresholdMs) {
        LOG.warn("Slow waitForAckedSeqno took {}ms (threshold={}ms). File being"
                + " written: {}, block: {}, Write pipeline datanodes: {}.",
            TimeUnit.NANOSECONDS.toMillis(duration), dfsclientSlowLogThresholdMs,
            src, block, nodes);
      }
    }
  }

  /**
   * wait for space of dataQueue and queue the packet
   *
   * @param packet  the DFSPacket to be queued
   * @throws IOException
   */
  void waitAndQueuePacket(DFSPacket packet) throws IOException {
    synchronized (dataQueue) {
      try {
        // If queue is full, then wait till we have enough space
        boolean firstWait = true;
        try {
          while (!streamerClosed && dataQueue.size() + ackQueue.size() >
              dfsClient.getConf().getWriteMaxPackets()) {
            if (firstWait) {
              Span span = Tracer.getCurrentSpan();
              if (span != null) {
                span.addTimelineAnnotation("dataQueue.wait");
              }
              firstWait = false;
            }
            try {
              dataQueue.wait();
            } catch (InterruptedException e) {
              // If we get interrupted while waiting to queue data, we still need to get rid
              // of the current packet. This is because we have an invariant that if
              // currentPacket gets full, it will get queued before the next writeChunk.
              //
              // Rather than wait around for space in the queue, we should instead try to
              // return to the caller as soon as possible, even though we slightly overrun
              // the MAX_PACKETS length.
              Thread.currentThread().interrupt();
              break;
            }
          }
        } finally {
          Span span = Tracer.getCurrentSpan();
          if ((span != null) && (!firstWait)) {
            span.addTimelineAnnotation("end.wait");
          }
        }
        checkClosed();
        queuePacket(packet);
      } catch (ClosedChannelException cce) {
        LOG.debug("Closed channel exception", cce);
      }
    }
  }

  /*
   * close the streamer, should be called only by an external thread
   * and only after all data to be sent has been flushed to datanode.
   *
   * Interrupt this data streamer if force is true
   *
   * @param force if this data stream is forced to be closed
   */
  void close(boolean force) {
    streamerClosed = true;
    synchronized (dataQueue) {
      dataQueue.notifyAll();
    }
    if (force) {
      this.interrupt();
    }
  }

  void setStreamerAsClosed() {
    streamerClosed = true;
  }

  private void checkClosed() throws IOException {
    if (streamerClosed) {
      lastException.throwException4Close();
    }
  }
  // 关闭相应处理线程
  private void closeResponder() {
    if (response != null) {
      try {
        response.close();
        response.join();
      } catch (InterruptedException  e) {
        LOG.debug("Thread interrupted", e);
        Thread.currentThread().interrupt();
      } finally {
        response = null;
      }
    }
  }
  // 关闭输出流、出入流和socket
  void closeStream() {
    final MultipleIOException.Builder b = new MultipleIOException.Builder();

    if (blockStream != null) {
      try {
        blockStream.close();
      } catch (IOException e) {
        b.add(e);
      } finally {
        blockStream = null;
      }
    }
    if (blockReplyStream != null) {
      try {
        blockReplyStream.close();
      } catch (IOException e) {
        b.add(e);
      } finally {
        blockReplyStream = null;
      }
    }
    if (null != s) {
      try {
        s.close();
      } catch (IOException e) {
        b.add(e);
      } finally {
        s = null;
      }
    }

    final IOException ioe = b.build();
    if (ioe != null) {
      lastException.set(ioe);
    }
  }

  /**
   * Examine whether it is worth waiting for a node to restart.
   * @param index the node index
   */
  boolean shouldWaitForRestart(int index) {
    // Only one node in the pipeline.
    if (nodes.length == 1) {
      return true;
    }

    /*
     * Treat all nodes as remote for test when skip enabled.
     */
    if (DFSClientFaultInjector.get().skipRollingRestartWait()) {
      return false;
    }

    // Is it a local node?
    InetAddress addr = null;
    try {
      addr = InetAddress.getByName(nodes[index].getIpAddr());
    } catch (java.net.UnknownHostException e) {
      // we are passing an ip address. this should not happen.
      assert false;
    }

    return addr != null && NetUtils.isLocalAddress(addr);
  }

  //
  // Processes responses from the datanodes.  A packet is removed
  // from the ackQueue when its response arrives.
  // 核心职责是维护写操作的可信度。
  // 当 DataStreamer 线程把数据包（Packet）顺着 DataNode 流水线发出去后，它不会等待，而是继续发下一个包。而 ResponseProcessor 就像是一个“售后客服”：
  // 接收确认 (ACK)：读取 DataNode 返回的 PipelineAck。
  // 确认成功：如果包被成功接收，将其从等待队列 (ackQueue) 中移除，释放内存。
  // 监控性能：识别流水线中哪些 DataNode 变慢了或发生了拥塞。
  // 触发容错：如果某个 DataNode 返回错误或掉线，它负责记录故障位置并关闭自己，从而通知 DataStreamer 进行管道恢复（Pipeline Recovery）。
  private class ResponseProcessor extends Daemon {
    // 标识响应处理器是否已关闭。使用 volatile 确保多线程间的可见性。
    private volatile boolean responderClosed = false;
    // 当前流水线中的 DataNode 目标列表。用于在报错时精确定位是哪个节点出了问题。
    private DatanodeInfo[] targets = null;
    // 标记当前处理的包是否是该数据块的最后一个包。如果是，处理完后线程将结束。
    private boolean isLastPacketInBlock = false;

    ResponseProcessor (DatanodeInfo[] targets) {
      this.targets = targets;
    }
    // 线程的主体逻辑，采用 while 循环不断处理 ACK：
    @Override
    public void run() {

      setName("ResponseProcessor for block " + block);
      PipelineAck ack = new PipelineAck();

      TraceScope scope = null;
      while (!responderClosed && dfsClient.clientRunning && !isLastPacketInBlock) {
        // process responses from datanodes.
        try {
          // read an ack from the pipeline
          ack.readFields(blockReplyStream);
          if (ack.getSeqno() != DFSPacket.HEART_BEAT_SEQNO) {
            Long begin = packetSendTime.get(ack.getSeqno());
            if (begin != null) {
              // 计算从发包到收到回执的时间，如果超过阈值（dfsclientSlowLogThresholdMs），打印警告日志。
              long duration = Time.monotonicNowNanos() - begin;
              if (TimeUnit.NANOSECONDS.toMillis(duration) > dfsclientSlowLogThresholdMs) {
                LOG.info("Slow ReadProcessor read fields for block " + block
                    + " took " + TimeUnit.NANOSECONDS.toMillis(duration) + "ms (threshold="
                    + dfsclientSlowLogThresholdMs + "ms); ack: " + ack
                    + ", targets: " + Arrays.asList(targets));
              }
            }
          }

          LOG.debug("DFSClient {}", ack);

          long seqno = ack.getSeqno();
          // processes response status from datanodes.
          ArrayList<DatanodeInfo> congestedNodesFromAck = new ArrayList<>();
          ArrayList<DatanodeInfo> slownodesFromAck = new ArrayList<>();
          // 遍历所有 DataNode 的回复状态。
          for (int i = ack.getNumOfReplies()-1; i >=0  && dfsClient.clientRunning; i--) {
            final Status reply = PipelineAck.getStatusFromHeader(ack
                .getHeaderFlag(i));
            // 拥塞控制：检查是否有节点返回 CONGESTED 状态，并更新 congestedNodes 列表。
            if (PipelineAck.getECNFromHeader(ack.getHeaderFlag(i)) ==
                PipelineAck.ECN.CONGESTED) {
              congestedNodesFromAck.add(targets[i]);
            }
            // 慢节点检测：检查是否有节点标记为 SLOW。
            if (PipelineAck.getSLOWFromHeader(ack.getHeaderFlag(i)) ==
                PipelineAck.SLOW.SLOW) {
              slownodesFromAck.add(targets[i]);
            }
            // Restart will not be treated differently unless it is
            // the local node or the only one in the pipeline.
            // 故障处理：如果节点返回 RESTART 或非 SUCCESS 状态，调用 errorState.setBadNodeIndex(i) 记录坏节点索引，并抛出 IOException 终止循环。
            if (PipelineAck.isRestartOOBStatus(reply)) {
              final String message = "Datanode " + i + " is restarting: "
                  + targets[i];
              errorState.initRestartingNode(i, message,
                  shouldWaitForRestart(i));
              throw new IOException(message);
            }
            // node error
            if (reply != SUCCESS) {
              errorState.setBadNodeIndex(i); // mark bad datanode
              throw new IOException("Bad response " + reply +
                  " for " + block + " from datanode " + targets[i]);
            }
          }

          if (!congestedNodesFromAck.isEmpty()) {
            synchronized (congestedNodes) {
              congestedNodes.clear();
              congestedNodes.addAll(congestedNodesFromAck);
            }
          } else {
            synchronized (congestedNodes) {
              congestedNodes.clear();
              lastCongestionBackoffTime = 0;
            }
          }

          if (slownodesFromAck.isEmpty()) {
            if (!slowNodeMap.isEmpty()) {
              slowNodeMap.clear();
            }
          } else {
            markSlowNode(slownodesFromAck);
            LOG.debug("SlowNodeMap content: {}.", slowNodeMap);
          }


          assert seqno != PipelineAck.UNKOWN_SEQNO :
              "Ack for unknown seqno should be a failed ack: " + ack;
          if (seqno == DFSPacket.HEART_BEAT_SEQNO) {  // a heartbeat ack
            continue;
          }

          // a success ack for a data packet
          DFSPacket one;
          synchronized (dataQueue) {
            one = ackQueue.getFirst();
          }
          // 检查 ACK 的编号是否与 ackQueue 中第一个包的编号一致。如果不一致，说明流乱序了，抛出异常。
          if (one.getSeqno() != seqno) {
            throw new IOException("ResponseProcessor: Expecting seqno " +
                one.getSeqno() + " for block " + block +
                " but received " + seqno);
          }
          isLastPacketInBlock = one.isLastPacketInBlock();

          // Fail the packet write for testing in order to force a
          // pipeline recovery.
          if (DFSClientFaultInjector.get().failPacket() &&
              isLastPacketInBlock) {
            failPacket = true;
            throw new IOException(
                "Failing the last packet for testing.");
          }

          // update bytesAcked
          // 更新已确认接收的字节数。
          block.setNumBytes(one.getLastByteOffsetBlock());

          synchronized (dataQueue) {
            if (one.getSpan() != null) {
              scope = new TraceScope(new Span());
              // TODO: Use scope = Tracer.curThreadTracer().activateSpan ?
              one.setSpan(null);
            }
            lastAckedSeqno = seqno;
            pipelineRecoveryCount = 0;
            ackQueue.removeFirst();
            packetSendTime.remove(seqno);
            dataQueue.notifyAll();

            one.releaseBuffer(byteArrayManager);
          }
        } catch (Throwable e) {
          if (!responderClosed) {
            lastException.set(e);
            errorState.setInternalError();
            errorState.markFirstNodeIfNotMarked();
            synchronized (dataQueue) {
              dataQueue.notifyAll();
            }
            if (!errorState.isRestartingNode()) {
              LOG.warn("Exception for " + block, e);
            }
            responderClosed = true;
          }
        } finally {
          if (scope != null) {
            scope.close();
          }
          scope = null;
        }
      }
    }
    // 精细化管理“慢节点”。
    // 它维护一个计数器。如果一个节点连续多次（达到 markSlowNodeAsBadNodeThreshold 次）被标记为 SLOW，
    // 该方法会直接将其视为“坏节点”（Bad Node），主动抛出异常强制触发管道切换，以提升写入性能。
    void markSlowNode(List<DatanodeInfo> slownodesFromAck) throws IOException {
      Set<DatanodeInfo> discontinuousNodes = new HashSet<>(slowNodeMap.keySet());
      for (DatanodeInfo slowNode : slownodesFromAck) {
        if (!slowNodeMap.containsKey(slowNode)) {
          slowNodeMap.put(slowNode, 1);
        } else {
          int oldCount = slowNodeMap.get(slowNode);
          slowNodeMap.put(slowNode, ++oldCount);
        }
        discontinuousNodes.remove(slowNode);
      }
      for (DatanodeInfo discontinuousNode : discontinuousNodes) {
        slowNodeMap.remove(discontinuousNode);
      }

      if (!slowNodeMap.isEmpty()) {
        for (Map.Entry<DatanodeInfo, Integer> entry : slowNodeMap.entrySet()) {
          if (entry.getValue() >= markSlowNodeAsBadNodeThreshold) {
            DatanodeInfo slowNode = entry.getKey();
            int index = getDatanodeIndex(slowNode);
            if (index >= 0) {
              errorState.setBadNodeIndex(index);
              throw new IOException("Receive reply from slowNode " + slowNode +
                  " for continuous " + markSlowNodeAsBadNodeThreshold +
                  " times, treating it as badNode");
            }
            slowNodeMap.remove(entry.getKey());
          }
        }
      }
    }

    void close() {
      responderClosed = true;
      this.interrupt();
    }

    int getDatanodeIndex(DatanodeInfo datanodeInfo) {
      for (int i = 0; i < targets.length; i++) {
        if (targets[i].equals(datanodeInfo)) {
          return i;
        }
      }
      return -1;
    }
  }

  private boolean shouldHandleExternalError(){
    return errorState.hasExternalError() && blockStream != null;
  }

  /**
   * If this stream has encountered any errors, shutdown threads
   * and mark the stream as closed.
   *
   * @return true if it should sleep for a while after returning.
   */
  // 最核心的**容错（Error Recovery）**方法。
  // 当 DataNode 宕机、网络闪断或发生外部错误时，该方法负责清理现场、重新建立管道并恢复数据传输。
  private boolean processDatanodeOrExternalError() throws IOException {
    // 检查是否真的发生了错误。如果既没有 DataNode 汇报的错误，也没有需要处理的外部错误，直接返回 false，不执行恢复逻辑。
    if (!errorState.hasDatanodeError() && !shouldHandleExternalError()) {
      return false;
    }
    LOG.debug("start process datanode/external error, {}", this);
    // 如果 response 线程还在运行，Streamer 就不能开始恢复操作。
    // 此时返回 true，告诉调用者（run 方法中的循环）需要 sleep 一会儿再重试，给 ResponseProcessor 留出清理资源和退出的时间。
    if (response != null) {
      LOG.info("Error Recovery for " + block +
          " waiting for responder to exit. ");
      return true;
    }
    // 物理断开与当前 DataNode 流水线的连接。
    closeStream();

    // move packets from ack queue to front of the data queue
    synchronized (dataQueue) {
      // ackQueue 里存放的是“已发送但尚未收到确认”的包。
      // 因为管道断了，这些包的确认状态不可信，所以将它们全部移回 dataQueue 的头部，以便在建立新管道后重新发送。
      dataQueue.addAll(0, ackQueue);
      ackQueue.clear();
      packetSendTime.clear();
    }

    // If we had to recover the pipeline more than the value
    // defined by maxPipelineRecoveryRetries in a row for the
    // same packet, this client likely has corrupt data or corrupting
    // during transmission.
    // 如果针对同一个包连续尝试恢复管道的次数超过了阈值（默认 5 次），则判定为不可修复的错误（可能是客户端本地数据损坏）。此时设置异常并关闭 Streamer
    if (!errorState.isRestartingNode() && ++pipelineRecoveryCount >
        maxPipelineRecoveryRetries) {
      LOG.warn("Error recovering pipeline for writing " +
          block + ". Already retried " + maxPipelineRecoveryRetries
          + " times for the same packet.");
      lastException.set(new IOException("Failing write. Tried pipeline " +
          "recovery " + maxPipelineRecoveryRetries
          + " times without success."));
      streamerClosed = true;
      return false;
    }
    // 执行真正的“手术”。
    setupPipelineForAppendOrRecovery();

    if (!streamerClosed && dfsClient.clientRunning) {
      // 如果在“关闭管道”（文件写完了，正在发最后一个包）阶段出错了，处理逻辑会有所不同。
      if (stage == BlockConstructionStage.PIPELINE_CLOSE) {

        // If we had an error while closing the pipeline, we go through a fast-path
        // where the BlockReceiver does not run. Instead, the DataNode just finalizes
        // the block immediately during the 'connect ack' process. So, we want to pull
        // the end-of-block packet from the dataQueue, since we don't actually have
        // a true pipeline to send it over.
        //
        // We also need to set lastAckedSeqno to the end-of-block Packet's seqno, so that
        // a client waiting on close() will be aware that the flush finished.
        synchronized (dataQueue) {
          // 如果此时块已经写完，DataNode 其实已经收到了所有数据，只是在确认阶段出错了。
          // HDFS 采用优化路径：直接将最后一个包从队列移除，并手动更新 lastAckedSeqno，宣告块写入成功，不再重新建立复杂的传输管道，直接调用 endBlock()。
          DFSPacket endOfBlockPacket = dataQueue.remove();  // remove the end of block packet
          // Close any trace span associated with this Packet
          Span span = endOfBlockPacket.getSpan();
          if (span != null) {
            span.finish();
            endOfBlockPacket.setSpan(null);
          }
          assert endOfBlockPacket.isLastPacketInBlock();
          assert lastAckedSeqno == endOfBlockPacket.getSeqno() - 1;
          lastAckedSeqno = endOfBlockPacket.getSeqno();
          pipelineRecoveryCount = 0;
          dataQueue.notifyAll();
        }
        endBlock();
      } else {
        initDataStreaming();
      }
    }

    return false;
  }

  void setHflush() {
    isHflushed = true;
  }

  private int findNewDatanode(final DatanodeInfo[] original
  ) throws IOException {
    if (nodes.length != original.length + 1) {
      throw new IOException(
          "Failed to replace a bad datanode on the existing pipeline "
              + "due to no more good datanodes being available to try. "
              + "(Nodes: current=" + Arrays.asList(nodes)
              + ", original=" + Arrays.asList(original) + "). "
              + "The current failed datanode replacement policy is "
              + dfsClient.dtpReplaceDatanodeOnFailure
              + ", and a client may configure this via '"
              + BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY
              + "' in its configuration.");
    }
    for(int i = 0; i < nodes.length; i++) {
      int j = 0;
      for(; j < original.length && !nodes[i].equals(original[j]); j++);
      if (j == original.length) {
        return i;
      }
    }
    throw new IOException("Failed: new datanode not found: nodes="
        + Arrays.asList(nodes) + ", original=" + Arrays.asList(original));
  }
  // 在 HDFS 写入过程中，如果某个 DataNode 发生故障，且配置策略要求替换该节点，addDatanode2ExistingPipeline 方法就会被调用。
  // 它的核心任务是：向 NameNode 申请一个新的 DataNode，并让管道中现有的一个健康节点将数据同步给这个新节点。
  private void addDatanode2ExistingPipeline() throws IOException {
    DataTransferProtocol.LOG.debug("lastAckedSeqno = {}", lastAckedSeqno);
      /*
       * Is data transfer necessary?  We have the following cases.
       *
       * Case 1: Failure in Pipeline Setup
       * - Append
       *    + Transfer the stored replica, which may be a RBW or a finalized.
       * - Create
       *    + If no data, then no transfer is required.
       *    + If there are data written, transfer RBW. This case may happens
       *      when there are streaming failure earlier in this pipeline.
       *
       * Case 2: Failure in Streaming
       * - Append/Create:
       *    + transfer RBW
       */
    // 如果当前不是追加（Append）模式，且没有任何数据包被确认过（lastAckedSeqno < 0），并且处于创建管道阶段。
    // 这意味着当前块还是空的，新加入的节点不需要从别人那里同步任何数据，直接返回即可。
    if (!isAppend && lastAckedSeqno < 0
        && stage == BlockConstructionStage.PIPELINE_SETUP_CREATE) {
      //no data have been written
      return;
    }

    int tried = 0;
    // 备份当前的健康节点列表
    final DatanodeInfo[] original = nodes;
    final StorageType[] originalTypes = storageTypes;
    final String[] originalIDs = storageIDs;
    IOException caughtException = null;
    //  排除已知的故障节点
    ArrayList<DatanodeInfo> exclude = new ArrayList<>(failed);
    while (tried < 3) {
      LocatedBlock lb;
      //get a new datanode
      // 向 NameNode 申请新节点
      // 告诉 NameNode 文件的路径、当前的节点列表和需要排除的故障节点，让 NameNode 根据机架感知策略选出一个最合适的新 DataNode。
      lb = dfsClient.namenode.getAdditionalDatanode(
          src, stat.getFileId(), block.getCurrentBlock(), nodes, storageIDs,
          exclude.toArray(new DatanodeInfo[exclude.size()]),
          1, dfsClient.clientName);
      // a new node was allocated by the namenode. Update nodes.
      setPipeline(lb);

      //find the new datanode
      final int d;
      try {
        //  在更新后的 nodes 数组中找到那个“新面孔”的索引
        d = findNewDatanode(original);
      } catch (IOException ioe) {
        // check the minimal number of nodes available to decide whether to
        // continue the write.

        //if live block location datanodes is greater than or equal to
        // HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.
        // MIN_REPLICATION threshold value, continue writing to the
        // remaining nodes. Otherwise throw exception.
        //
        // If HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.
        // MIN_REPLICATION is set to 0 or less than zero, an exception will be
        // thrown if a replacement could not be found.

        if (dfsClient.dtpReplaceDatanodeOnFailureReplication > 0 && nodes.length
            >= dfsClient.dtpReplaceDatanodeOnFailureReplication) {
          DFSClient.LOG.warn(
              "Failed to find a new datanode to add to the write pipeline,"
                  + " continue to write to the pipeline with " + nodes.length
                  + " nodes since it's no less than minimum replication: "
                  + dfsClient.dtpReplaceDatanodeOnFailureReplication
                  + " configured by "
                  + BlockWrite.ReplaceDatanodeOnFailure.MIN_REPLICATION
                  + ".", ioe);
          return;
        }
        throw ioe;
      }
      //transfer replica. pick a source from the original nodes
      // // 轮询选择一个现有的健康节点作为数据源
      final DatanodeInfo src = original[tried % original.length];
      // // 目标是新申请到的节点
      final DatanodeInfo[] targets = {nodes[d]};
      final StorageType[] targetStorageTypes = {storageTypes[d]};
      final String[] targetStorageIDs = {storageIDs[d]};

      try {
        // 它会向 src（现有的 DataNode）发送指令，让 src 启动一个临时的 DataCopy 进程
        // ，将其内存或磁盘中的副本数据（RBW - Replica Being Written）直接发送给 targets（新节点）。
        transfer(src, targets, targetStorageTypes, targetStorageIDs,
            lb.getBlockToken());
      } catch (IOException ioe) {
        DFSClient.LOG.warn("Error transferring data from " + src + " to " +
            nodes[d] + ": " + ioe.getMessage());
        caughtException = ioe;
        // add the allocated node to the exclude list.
        exclude.add(nodes[d]);
        setPipeline(original, originalTypes, originalIDs);
        tried++;
        continue;
      }
      return; // finished successfully
    }
    // All retries failed
    throw (caughtException != null) ? caughtException :
        new IOException("Failed to add a node");
  }

  private long computeTransferWriteTimeout() {
    return dfsClient.getDatanodeWriteTimeout(2);
  }
  private long computeTransferReadTimeout() {
    // transfer timeout multiplier based on the transfer size
    // One per 200 packets = 12.8MB. Minimum is 2.
    int multi = 2
        + (int) (bytesSent / dfsClient.getConf().getWritePacketSize()) / 200;
    return dfsClient.getDatanodeReadTimeout(multi);
  }
  // 实现数据迁移的物理执行者。
  // 它的核心任务是：连接一个现有的健康 DataNode（src），指令它将当前正在写入的数据块（Replica）拷贝给新加入的 DataNode（targets）。
  // src: 数据源节点，即当前 Pipeline 中拥有数据的健康 DataNode。
  //
  //targets: 目标节点列表（通常是新申请的那一个节点）。
  //
  //blockToken: 访问令牌，用于向 DataNode 证明客户端有权操作该数据块
  private void transfer(final DatanodeInfo src, final DatanodeInfo[] targets,
                        final StorageType[] targetStorageTypes,
                        final String[] targetStorageIDs,
                        final Token<BlockTokenIdentifier> blockToken)
      throws IOException {
    //transfer replica to the new datanode
    // 初始化一个针对加密密钥失效的重试策略。
    RefetchEncryptionKeyPolicy policy = new RefetchEncryptionKeyPolicy(src);
    do {
      StreamerStreams streams = null;
      try {
        final long writeTimeout = computeTransferWriteTimeout();
        final long readTimeout = computeTransferReadTimeout();
        // 建立与 src（源 DataNode） 的物理连接。
        streams = new StreamerStreams(src, writeTimeout, readTimeout,
            blockToken);
        // 发送 TRANSFER_BLOCK 指令。
        // 客户端并不亲自搬运数据，而是通过刚刚建立的连接向 src 发送一个协议请求，告诉 src：“请把你手中的这个 Block 转发给 targets 中的这些节点”。
        streams.sendTransferBlock(targets, targetStorageTypes,
            targetStorageIDs, blockToken);
        return;
      } catch (InvalidEncryptionKeyException e) {
        policy.recordFailure(e);
      } finally {
        IOUtils.closeStream(streams);
      }
    } while (policy.continueRetryingOrThrow());
  }

  /**
   * Open a DataStreamer to a DataNode pipeline so that
   * it can be written to.
   * This happens when a file is appended or data streaming fails
   * It keeps on trying until a pipeline is setup
   */
  // 当现有的数据流管道因为某个 DataNode 宕机而断开，或者用户想要对一个已有的文件进行追加（Append）写入时，这个方法会被触发。
  private void setupPipelineForAppendOrRecovery() throws IOException {
    // Check number of datanodes. Note that if there is no healthy datanode,
    // this must be internal error because we mark external error in striped
    // outputstream only when all the streamers are in the DATA_STREAMING stage
    // 检查当前内存中记录的 DataNode 列表是否为空。
    if (nodes == null || nodes.length == 0) {
      String msg = "Could not get block locations. " + "Source file \""
          + src + "\" - Aborting..." + this;
      LOG.warn(msg);
      lastException.set(new IOException(msg));
      streamerClosed = true;
      return;
    }
    setupPipelineInternal(nodes, storageTypes, storageIDs);
  }
  // 任务是在 DataNode 出现故障时，决定是等待重启、剔除坏节点、还是寻找替换节点，并最终重建物理传输通道。
  protected void setupPipelineInternal(DatanodeInfo[] datanodes,
      StorageType[] nodeStorageTypes, String[] nodeStorageIDs)
      throws IOException {
    boolean success = false;
    long newGS = 0L;
    // 只要管道没建立成功且 Streamer 没关闭，就会不断尝试。
    while (!success && !streamerClosed && dfsClient.clientRunning) {
      // 处理重启中的节点。如果发现某个 DataNode 正在重启，会根据策略选择等待或跳过。
      if (!handleRestartingDatanode()) {
        return;
      }

      final boolean isRecovery = errorState.hasInternalError();
      // 剔除坏节点。如果 errorState 中记录了故障节点的索引，将其从当前的 nodes 列表中彻底删除。
      if (!handleBadDatanode()) {
        return;
      }
      // 替换节点策略。如果剩余节点太少（低于副本数要求），此方法会决定是否向 NameNode 申请新的 DataNode 来填补空缺。
      handleDatanodeReplacement();

      // get a new generation stamp and an access token
      // 与 NameNode 通信更新元数据。
      final LocatedBlock lb = updateBlockForPipeline();
      newGS = lb.getBlock().getGenerationStamp();
      accessToken = lb.getBlockToken();

      // set up the pipeline again with the remaining nodes
      // 尝试与剩余的 DataNode 建立 TCP 连接。如果成功，success 变为 true。
      success = createBlockOutputStream(nodes, storageTypes, storageIDs, newGS,
          isRecovery);

      failPacket4Testing();

      errorState.checkRestartingNodeDeadline(nodes);
    } // while

    if (success) {
      updatePipeline(newGS);
    }
  }

  /**
   * Sleep if a node is restarting.
   * This process is repeated until the deadline or the node starts back up.
   * @return true if it should continue.
   */
  // 负责“温柔”地处理那些因为升级或维护而临时重启的节点。
  boolean handleRestartingDatanode() {
    // 检查是否有节点标记为重启。
    if (errorState.isRestartingNode()) {
      // 判断是否值得等待。如果不值得（超过了等待时限），则将其视为“坏节点”直接剔除。
      if (!errorState.doWaitForRestart()) {
        // If node is restarting and not worth to wait for restart then can go
        // ahead with error recovery considering it as bad node for now. Later
        // it should be able to re-consider the same node for future pipeline
        // updates.
        errorState.setBadNodeIndex(errorState.getRestartingNodeIndex());
        return true;
      }
      // 4 seconds or the configured deadline period, whichever is shorter.
      // This is the retry interval and recovery will be retried in this
      // interval until timeout or success.
      final long delay = Math.min(errorState.datanodeRestartTimeout, 4000L);
      try {
        // 如果决定等待，线程会进入休眠（默认最多 4 秒），然后再次尝试。这比直接剔除节点并重新复制数据（Replication）开销要小得多。
        Thread.sleep(delay);
      } catch (InterruptedException ie) {
        lastException.set(new IOException(
            "Interrupted while waiting for restarting "
            + nodes[errorState.getRestartingNodeIndex()]));
        streamerClosed = true;
        return false;
      }
    }
    return true;
  }

  /**
   * Remove bad node from list of nodes if badNodeIndex was set.
   * @return true if it should continue.
   */
  // 这是“外科手术”式的处理逻辑，负责从节点数组中移除故障节点。
  boolean handleBadDatanode() {
    final int badNodeIndex = errorState.getBadNodeIndex();
    if (badNodeIndex >= 0) {
      // 彻底失败检查：如果 nodes.length <= 1 且唯一的节点也坏了，直接报错中止写入。
      if (nodes.length <= 1) {
        lastException.set(new IOException("All datanodes "
            + Arrays.toString(nodes) + " are bad. Aborting..."));
        streamerClosed = true;
        return false;
      }

      String reason = "bad.";
      if (errorState.getRestartingNodeIndex() == badNodeIndex) {
        reason = "restarting.";
        restartingNodes.add(nodes[badNodeIndex]);
      }
      // 将被剔除的节点加入 failed 列表，防止稍后 handleDatanodeReplacement 再次选中它。
      LOG.warn("Error Recovery for " + block + " in pipeline "
          + Arrays.toString(nodes) + ": datanode " + badNodeIndex
          + "("+ nodes[badNodeIndex] + ") is " + reason);
      failed.add(nodes[badNodeIndex]);

      DatanodeInfo[] newnodes = new DatanodeInfo[nodes.length-1];
      arraycopy(nodes, newnodes, badNodeIndex);

      final StorageType[] newStorageTypes = new StorageType[newnodes.length];
      arraycopy(storageTypes, newStorageTypes, badNodeIndex);

      final String[] newStorageIDs = new String[newnodes.length];
      arraycopy(storageIDs, newStorageIDs, badNodeIndex);
      // 调用 setPipeline 更新 Streamer 内存中的目标地址。
      setPipeline(newnodes, newStorageTypes, newStorageIDs);

      errorState.adjustState4RestartingNode();
      lastException.clear();
    }
    return true;
  }

  /** Add a datanode if replace-datanode policy is satisfied. */
  // 决定了是否需要寻找一个新的 DataNode 来填补空缺，以维持副本数量。
  private void handleDatanodeReplacement() throws IOException {
    // 判断当前情况是否符合“替换 DataNode”的策略要求。
    if (dfsClient.dtpReplaceDatanodeOnFailure.satisfy(stat.getReplication(),
        nodes, isAppend, isHflushed)) {
      try {
        // addDatanode2ExistingPipeline 会向 NameNode 申请一个新的 DataNode 地址，
        // 并让当前管道中剩下的某个 DataNode 将已有的数据块内容复制（Copy）给这个新节点，从而完成管道的“热插拔”替换。
        addDatanode2ExistingPipeline();
      } catch(IOException ioe) {
        if (!dfsClient.dtpReplaceDatanodeOnFailure.isBestEffort()) {
          throw ioe;
        }
        LOG.warn("Failed to replace datanode."
            + " Continue with the remaining datanodes since "
            + BlockWrite.ReplaceDatanodeOnFailure.BEST_EFFORT_KEY
            + " is set to true.", ioe);
      }
    }
  }

  void failPacket4Testing() {
    if (failPacket) { // for testing
      failPacket = false;
      try {
        // Give DNs time to send in bad reports. In real situations,
        // good reports should follow bad ones, if client committed
        // with those nodes.
        Thread.sleep(2000);
      } catch (InterruptedException e) {
        LOG.debug("Thread interrupted", e);
      }
    }
  }

  private LocatedBlock updateBlockForPipeline() throws IOException {
    return dfsClient.namenode.updateBlockForPipeline(block.getCurrentBlock(),
        dfsClient.clientName);
  }

  void updateBlockGS(final long newGS) {
    block.setGenerationStamp(newGS);
  }

  /** update pipeline at the namenode */
  @VisibleForTesting
  public void updatePipeline(long newGS) throws IOException {
    final ExtendedBlock oldBlock = block.getCurrentBlock();
    // the new GS has been propagated to all DN, it should be ok to update the
    // local block state
    updateBlockGS(newGS);
    dfsClient.namenode.updatePipeline(dfsClient.clientName, oldBlock,
        block.getCurrentBlock(), nodes, storageIDs);
  }

  DatanodeInfo[] getExcludedNodes() {
    return excludedNodes.getAllPresent(excludedNodes.asMap().keySet())
            .keySet().toArray(DatanodeInfo.EMPTY_ARRAY);
  }

  /**
   * Open a DataStreamer to a DataNode so that it can be written to.
   * This happens when a file is created and each time a new block is allocated.
   * Must get block ID and the IDs of the destinations from the namenode.
   * Returns the list of target datanodes.
   */
  protected LocatedBlock nextBlockOutputStream() throws IOException {
    LocatedBlock lb;
    DatanodeInfo[] nodes;
    StorageType[] nextStorageTypes;
    String[] nextStorageIDs;
    int count = dfsClient.getConf().getNumBlockWriteRetry();
    boolean success;
    final ExtendedBlock oldBlock = block.getCurrentBlock();
    do {
      errorState.resetInternalError();
      lastException.clear();

      DatanodeInfo[] excluded = getExcludedNodes();
      lb = locateFollowingBlock(
          excluded.length > 0 ? excluded : null, oldBlock);
      block.setCurrentBlock(lb.getBlock());
      block.setNumBytes(0);
      bytesSent = 0;
      accessToken = lb.getBlockToken();
      nodes = lb.getLocations();
      nextStorageTypes = lb.getStorageTypes();
      nextStorageIDs = lb.getStorageIDs();

      // Connect to first DataNode in the list.
      success = createBlockOutputStream(nodes, nextStorageTypes, nextStorageIDs,
          0L, false);

      if (!success) {
        LOG.warn("Abandoning " + block);
        dfsClient.namenode.abandonBlock(block.getCurrentBlock(),
            stat.getFileId(), src, dfsClient.clientName);
        block.setCurrentBlock(null);
        final DatanodeInfo badNode = nodes[errorState.getBadNodeIndex()];
        LOG.warn("Excluding datanode " + badNode);
        excludedNodes.put(badNode, badNode);
      }
    } while (!success && --count >= 0);

    if (!success) {
      throw new IOException("Unable to create new block.");
    }
    return lb;
  }

  // connects to the first datanode in the pipeline
  // Returns true if success, otherwise return failure.
  // *建立物理传输管道（Pipeline）**的关键一步。它的主要任务是连接 Pipeline 中的第一个 DataNode，发送写块请求，并等待整条流水线确认就绪。
  boolean createBlockOutputStream(DatanodeInfo[] nodes,
      StorageType[] nodeStorageTypes, String[] nodeStorageIDs,
      long newGS, boolean recoveryFlag) {
    if (nodes.length == 0) {
      LOG.info("nodes are empty for write pipeline of " + block);
      return false;
    }
    String firstBadLink = "";
    boolean checkRestart = false;
    if (LOG.isDebugEnabled()) {
      LOG.debug("pipeline = " + Arrays.toString(nodes) + ", " + this);
    }

    // persist blocks on namenode on next flush
    // 标记需要在下次 flush 时持久化块元数据
    persistBlocks.set(true);

    int refetchEncryptionKey = 1;
    while (true) {
      boolean result = false;
      DataOutputStream out = null;
      try {
        assert null == s : "Previous socket unclosed";
        assert null == blockReplyStream : "Previous blockReplyStream unclosed";
        // 创建到 nodes[0] 的 TCP 连接。
        s = createSocketForPipeline(nodes[0], nodes.length, dfsClient);
        // 设置超时：根据 Pipeline 长度动态计算超时（节点越多，握手时间越长）。
        long writeTimeout = dfsClient.getDatanodeWriteTimeout(nodes.length);
        long readTimeout = dfsClient.getDatanodeReadTimeout(nodes.length);
        // 构建输出与输入流
        // out 用于向 DataNode 发送数据包和指令；blockReplyStream 用于接收 DataNode 返回的响应（ACK）。
        OutputStream unbufOut = NetUtils.getOutputStream(s, writeTimeout);
        InputStream unbufIn = NetUtils.getInputStream(s, readTimeout);
        IOStreamPair saslStreams = dfsClient.saslClient.socketSend(s,
            unbufOut, unbufIn, dfsClient, accessToken, nodes[0]);
        unbufOut = saslStreams.out;
        unbufIn = saslStreams.in;
        out = new DataOutputStream(new BufferedOutputStream(unbufOut,
            DFSUtilClient.getSmallBufferSize(dfsClient.getConfiguration())));
        blockReplyStream = new DataInputStream(unbufIn);

        //
        // Xmit header info to datanode
        //

        BlockConstructionStage bcs = recoveryFlag ?
            stage.getRecoveryStage() : stage;

        // We cannot change the block length in 'block' as it counts the number
        // of bytes ack'ed.
        ExtendedBlock blockCopy = block.getCurrentBlock();
        blockCopy.setNumBytes(stat.getBlockSize());

        boolean[] targetPinnings = getPinnings(nodes);
        // send the request
        // 客户端通过 Sender 发送 OP_WRITE_BLOCK 指令。
        // 包含所有节点的信息（nodes）。
        // 第一个节点收到后，会根据这个列表连接第二个节点，第二个连第三个，以此类推，形成串行流水线。
        new Sender(out).writeBlock(blockCopy, nodeStorageTypes[0], accessToken,
            dfsClient.clientName, nodes, nodeStorageTypes, null, bcs,
            nodes.length, block.getNumBytes(), bytesSent, newGS,
            checksum4WriteBlock, cachingStrategy.get(), isLazyPersistFile,
            (targetPinnings != null && targetPinnings[0]), targetPinnings,
            nodeStorageIDs[0], nodeStorageIDs);

        // receive ack for connect
        // 等待 DataNode 返回“握手成功”的确认。
        BlockOpResponseProto resp = BlockOpResponseProto.parseFrom(
            PBHelperClient.vintPrefixed(blockReplyStream));
        // 整个管道的状态。
        Status pipelineStatus = resp.getStatus();
        // 如果建立失败，这个字段指明是哪两个节点之间断开了。
        firstBadLink = resp.getFirstBadLink();

        // Got an restart OOB ack.
        // If a node is already restarting, this status is not likely from
        // the same node. If it is from a different node, it is not
        // from the local datanode. Thus it is safe to treat this as a
        // regular node error.
        // 检查是否有节点正在重启。如果是常规错误，checkBlockOpStatus 会抛出异常，进入 catch 块进行容错。
        if (PipelineAck.isRestartOOBStatus(pipelineStatus) &&
            !errorState.isRestartingNode()) {
          checkRestart = true;
          throw new IOException("A datanode is restarting.");
        }

        String logInfo = "ack with firstBadLink as " + firstBadLink;
        DataTransferProtoUtil.checkBlockOpStatus(resp, logInfo);

        assert null == blockStream : "Previous blockStream unclosed";
        //  正式赋值给 DataStreamer 的成员变量
        blockStream = out;
        result =  true; // success
        errorState.resetInternalError();
        lastException.clear();
        // remove all restarting nodes from failed nodes list
        failed.removeAll(restartingNodes);
        restartingNodes.clear();
      } catch (IOException ie) {
        if (!errorState.isRestartingNode()) {
          LOG.warn("Exception in createBlockOutputStream " + this, ie);
        }
        if (ie instanceof InvalidEncryptionKeyException &&
            refetchEncryptionKey > 0) {
          LOG.info("Will fetch a new encryption key and retry, "
              + "encryption key was invalid when connecting to "
              + nodes[0] + " : " + ie);
          // The encryption key used is invalid.
          refetchEncryptionKey--;
          dfsClient.clearDataEncryptionKey();
          // Don't close the socket/exclude this node just yet. Try again with
          // a new encryption key.
          continue;
        }

        // find the datanode that matches
        if (firstBadLink.length() != 0) {
          for (int i = 0; i < nodes.length; i++) {
            // NB: Unconditionally using the xfer addr w/o hostname
            if (firstBadLink.equals(nodes[i].getXferAddr())) {
              errorState.setBadNodeIndex(i);
              break;
            }
          }
        } else {
          assert !checkRestart;
          errorState.setBadNodeIndex(0);
        }

        final int i = errorState.getBadNodeIndex();
        // Check whether there is a restart worth waiting for.
        if (checkRestart) {
          errorState.initRestartingNode(i,
              "Datanode " + i + " is restarting: " + nodes[i],
              shouldWaitForRestart(i));
        }
        errorState.setInternalError();
        lastException.set(ie);
        result =  false;  // error
      } finally {
        if (!result) {
          IOUtils.closeSocket(s);
          s = null;
          IOUtils.closeStream(out);
          IOUtils.closeStream(blockReplyStream);
          blockReplyStream = null;
        }
      }
      return result;
    }
  }

  private boolean[] getPinnings(DatanodeInfo[] nodes) {
    if (favoredNodes == null) {
      return null;
    } else {
      boolean[] pinnings = new boolean[nodes.length];
      HashSet<String> favoredSet = new HashSet<>(Arrays.asList(favoredNodes));
      for (int i = 0; i < nodes.length; i++) {
        pinnings[i] = favoredSet.remove(nodes[i].getXferAddrWithHostname());
        LOG.debug("{} was chosen by name node (favored={}).",
            nodes[i].getXferAddrWithHostname(), pinnings[i]);
      }
      if (!favoredSet.isEmpty()) {
        // There is one or more favored nodes that were not allocated.
        LOG.warn("These favored nodes were specified but not chosen: "
            + favoredSet + " Specified favored nodes: "
            + Arrays.toString(favoredNodes));

      }
      return pinnings;
    }
  }

  private LocatedBlock locateFollowingBlock(DatanodeInfo[] excluded,
      ExtendedBlock oldBlock) throws IOException {
    return DFSOutputStream.addBlock(excluded, dfsClient, src, oldBlock,
        stat.getFileId(), favoredNodes, addBlockFlags);
  }

  /**
   * This function sleeps for a certain amount of time when the writing
   * pipeline is congested. The function calculates the time based on a
   * decorrelated filter.
   *
   * @see
   * <a href="http://www.awsarchitectureblog.com/2015/03/backoff.html">
   *   http://www.awsarchitectureblog.com/2015/03/backoff.html</a>
   */
  private void backOffIfNecessary() throws InterruptedException {
    int t = 0;
    synchronized (congestedNodes) {
      if (!congestedNodes.isEmpty()) {
        StringBuilder sb = new StringBuilder("DataNode");
        for (DatanodeInfo i : congestedNodes) {
          sb.append(' ').append(i);
        }
        int range = Math.abs(lastCongestionBackoffTime * 3 -
            congestionBackOffMeanTimeInMs);
        int base = Math.min(lastCongestionBackoffTime * 3,
            congestionBackOffMeanTimeInMs);
        t = Math.min(congestionBackOffMaxTimeInMs,
                     (int)(base + Math.random() * range));
        lastCongestionBackoffTime = t;
        sb.append(" are congested. Backing off for ").append(t).append(" ms");
        LOG.info(sb.toString());
        congestedNodes.clear();
      }
    }
    if (t != 0) {
      Thread.sleep(t);
    }
  }

  /**
   * get the block this streamer is writing to
   *
   * @return the block this streamer is writing to
   */
  ExtendedBlock getBlock() {
    return block.getCurrentBlock();
  }

  /**
   * return the target datanodes in the pipeline
   *
   * @return the target datanodes in the pipeline
   */
  DatanodeInfo[] getNodes() {
    return nodes;
  }

  String[] getStorageIDs() {
    return storageIDs;
  }

  BlockConstructionStage getStage() {
    return stage;
  }

  /**
   * return the token of the block
   *
   * @return the token of the block
   */
  Token<BlockTokenIdentifier> getBlockToken() {
    return accessToken;
  }

  ErrorState getErrorState() {
    return errorState;
  }

  /**
   * Put a packet to the data queue
   *
   * @param packet the packet to be put into the data queued
   */
  void queuePacket(DFSPacket packet) {
    synchronized (dataQueue) {
      if (packet == null) return;
      packet.addTraceParent(Tracer.getCurrentSpan());
      dataQueue.addLast(packet);
      lastQueuedSeqno = packet.getSeqno();
      LOG.debug("Queued {}, {}", packet, this);
      dataQueue.notifyAll();
    }
  }

  /**
   * For heartbeat packets, create buffer directly by new byte[]
   * since heartbeats should not be blocked.
   */
  private DFSPacket createHeartbeatPacket() {
    final byte[] buf = new byte[PacketHeader.PKT_MAX_HEADER_LEN];
    return new DFSPacket(buf, 0, 0, DFSPacket.HEART_BEAT_SEQNO, 0, false);
  }

  private static LoadingCache<DatanodeInfo, DatanodeInfo> initExcludedNodes(
      long excludedNodesCacheExpiry) {
    return CacheBuilder.newBuilder()
        .expireAfterWrite(excludedNodesCacheExpiry, TimeUnit.MILLISECONDS)
        .removalListener(new RemovalListener<DatanodeInfo, DatanodeInfo>() {
          @Override
          public void onRemoval(
              @Nonnull RemovalNotification<DatanodeInfo, DatanodeInfo>
                  notification) {
            LOG.info("Removing node " + notification.getKey()
                + " from the excluded nodes list");
          }
        }).build(new CacheLoader<DatanodeInfo, DatanodeInfo>() {
          @Override
          public DatanodeInfo load(DatanodeInfo key) throws Exception {
            return key;
          }
        });
  }

  private static <T> void arraycopy(T[] srcs, T[] dsts, int skipIndex) {
    System.arraycopy(srcs, 0, dsts, 0, skipIndex);
    System.arraycopy(srcs, skipIndex+1, dsts, skipIndex, dsts.length-skipIndex);
  }

  /**
   * check if to persist blocks on namenode
   *
   * @return if to persist blocks on namenode
   */
  AtomicBoolean getPersistBlocks(){
    return persistBlocks;
  }

  /**
   * check if to append a chunk
   *
   * @param appendChunk if to append a chunk
   */
  void setAppendChunk(boolean appendChunk){
    this.appendChunk = appendChunk;
  }

  /**
   * get if to append a chunk
   *
   * @return if to append a chunk
   */
  boolean getAppendChunk(){
    return appendChunk;
  }

  /**
   * @return the last exception
   */
  LastExceptionInStreamer getLastException(){
    return lastException;
  }

  /**
   * set socket to null
   */
  void setSocketToNull() {
    this.s = null;
  }

  /**
   * return current sequence number and then increase it by 1
   *
   * @return current sequence number before increasing
   */
  long getAndIncCurrentSeqno() {
    long old = this.currentSeqno;
    this.currentSeqno++;
    return old;
  }

  /**
   * get last queued sequence number
   *
   * @return last queued sequence number
   */
  long getLastQueuedSeqno() {
    return lastQueuedSeqno;
  }

  /**
   * get the number of bytes of current block
   *
   * @return the number of bytes of current block
   */
  long getBytesCurBlock() {
    return bytesCurBlock;
  }

  /**
   * set the bytes of current block that have been written
   *
   * @param bytesCurBlock bytes of current block that have been written
   */
  void setBytesCurBlock(long bytesCurBlock) {
    this.bytesCurBlock = bytesCurBlock;
  }

  /**
   * increase bytes of current block by len.
   *
   * @param len how many bytes to increase to current block
   */
  void incBytesCurBlock(long len) {
    this.bytesCurBlock += len;
  }

  /**
   * set artificial slow down for unit test
   *
   * @param period artificial slow down
   */
  void setArtificialSlowdown(long period) {
    this.artificialSlowdown = period;
  }

  /**
   * if this streamer is to terminate
   *
   * @return if this streamer is to terminate
   */
  boolean streamerClosed(){
    return streamerClosed;
  }

  /**
   * @return The times have retried to recover pipeline, for the same packet.
   */
  @VisibleForTesting
  int getPipelineRecoveryCount() {
    return pipelineRecoveryCount;
  }

  void closeSocket() throws IOException {
    if (s != null) {
      s.close();
    }
  }

  @Override
  public String toString() {
    final ExtendedBlock extendedBlock = block.getCurrentBlock();
    return extendedBlock == null ?
        "block==null" : "" + extendedBlock.getLocalBlock();
  }
}
