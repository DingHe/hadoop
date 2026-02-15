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
package org.apache.hadoop.hdfs.qjournal.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.util.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournaledEditsResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournalStateResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PrepareRecoveryResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.SegmentStateProto;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileInputStream;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.EditLogOutputStream;
import org.apache.hadoop.hdfs.server.namenode.JournalManager;
import org.apache.hadoop.hdfs.server.namenode.JournalSet;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.log.LogThrottlingHelper;
import org.apache.hadoop.log.LogThrottlingHelper.LogAction;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * A JournalManager that writes to a set of remote JournalNodes,
 * requiring a quorum of nodes to ack each write.
 */
//主要用于管理 JournalNode 集群中的操作日志（EditLog）的写入和读取。
//在 HDFS 的高可用(HA)架构中，QuorumJournalManager 负责协调多个 JournalNode 之间的操作，
// 以实现事务日志的分布式存储和故障恢复。它通过 "Quorum"（法定人数）机制，
// 确保只要大多数（通常为 2/3）节点达成一致，即可保证数据的一致性和持久性
@InterfaceAudience.Private
public class QuorumJournalManager implements JournalManager {
  static final Logger LOG = LoggerFactory.getLogger(QuorumJournalManager.class);

  // This config is not publicly exposed
  public static final String QJM_RPC_MAX_TXNS_KEY =
      "dfs.ha.tail-edits.qjm.rpc.max-txns";//配置项键名，表示每次通过 RPC 获取的最大事务数
  public static final int QJM_RPC_MAX_TXNS_DEFAULT = 5000;//默认每次RPC获取的最大事务数，默认为 5000。

  // Maximum number of transactions to fetch at a time when using the
  // RPC edit fetch mechanism
  private final int maxTxnsPerRpc;//实际从配置中读取的每次 RPC 获取的最大事务数，影响事务获取的批量大小
  // Whether or not in-progress tailing is enabled in the configuration
  private final boolean inProgressTailingEnabled;//是否启用未完成日志的跟踪（tailing）。
  // Timeouts for which the QJM will wait for each of the following actions.
  private final int startSegmentTimeoutMs;//启动日志段（segment）的超时时间（单位：毫秒）。
  private final int prepareRecoveryTimeoutMs;//准备恢复操作的超时时间（单位：毫秒）。
  private final int acceptRecoveryTimeoutMs;//接受恢复操作的超时时间（单位：毫秒）。
  private final int finalizeSegmentTimeoutMs;//完成日志段（segment）的超时时间（单位：毫秒）。
  private final int selectInputStreamsTimeoutMs;//选择输入流的超时时间（单位：毫秒）。
  private final int getJournalStateTimeoutMs;//获取日志状态的超时时间（单位：毫秒）。
  private final int newEpochTimeoutMs;//创建新的 epoch（纪元）的超时时间（单位：毫秒）。
  private final int writeTxnsTimeoutMs;//写入事务的超时时间（单位：毫秒）。

  // This timeout is used for calls that don't occur during normal operation
  // e.g. format, upgrade operations and a few others. So we can use rather
  // lengthy timeouts by default.
  private final int timeoutMs;//默认操作的超时时间，主要用于非正常操作（如格式化、升级等）。

  private final Configuration conf;
  private final URI uri;//日志服务的 URI 地址，标识要连接的 JournalNode 集群
  private final NamespaceInfo nsInfo;//当前 HDFS 命名空间的信息，包含命名空间 ID、集群 ID、版本等信息。
  private final String nameServiceId;//NameService 的唯一标识，用于支持 HDFS 高可用（HA）。
  private boolean isActiveWriter;//标识当前 JournalManager 是否是活动写入者，控制写操作权限。


  private final AsyncLoggerSet loggers;//管理多个异步日志记录器的集合，支持对多个 JournalNode 进行操作。

  private static final int OUTPUT_BUFFER_CAPACITY_DEFAULT = 512 * 1024;//输出缓冲区的容量，默认值为 512 KB，控制写入日志的缓存大小。
  private int outputBufferCapacity;
  private final URLConnectionFactory connectionFactory;//HTTP 连接工厂，管理与 JournalNode 的 HTTP 连接。

  /** Limit logging about input stream selection to every 5 seconds max. */
  private static final long SELECT_INPUT_STREAM_LOG_INTERVAL_MS = 5000;
  private final LogThrottlingHelper selectInputStreamLogHelper =
      new LogThrottlingHelper(SELECT_INPUT_STREAM_LOG_INTERVAL_MS);

  @VisibleForTesting
  public QuorumJournalManager(Configuration conf,
                              URI uri,
                              NamespaceInfo nsInfo) throws IOException {
    this(conf, uri, nsInfo, null, IPCLoggerChannel.FACTORY);
  }
  
  public QuorumJournalManager(Configuration conf,
      URI uri, NamespaceInfo nsInfo, String nameServiceId) throws IOException {
    this(conf, uri, nsInfo, nameServiceId, IPCLoggerChannel.FACTORY);
  }

  @VisibleForTesting
  QuorumJournalManager(Configuration conf,
                       URI uri, NamespaceInfo nsInfo,
                       AsyncLogger.Factory loggerFactory) throws IOException {
    this(conf, uri, nsInfo, null, loggerFactory);

  }

  
  QuorumJournalManager(Configuration conf,
      URI uri, NamespaceInfo nsInfo, String nameServiceId,
      AsyncLogger.Factory loggerFactory) throws IOException {
    Preconditions.checkArgument(conf != null, "must be configured");

    this.conf = conf;
    this.uri = uri;
    this.nsInfo = nsInfo;
    this.nameServiceId = nameServiceId;
    //负责与多个 JournalNode 进行通信，管理多个异步日志记录器。
    this.loggers = new AsyncLoggerSet(createLoggers(loggerFactory));

    this.maxTxnsPerRpc =
        conf.getInt(QJM_RPC_MAX_TXNS_KEY, QJM_RPC_MAX_TXNS_DEFAULT);
    Preconditions.checkArgument(maxTxnsPerRpc > 0,
        "Must specify %s greater than 0!", QJM_RPC_MAX_TXNS_KEY);
    this.inProgressTailingEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_HA_TAILEDITS_INPROGRESS_KEY,
        DFSConfigKeys.DFS_HA_TAILEDITS_INPROGRESS_DEFAULT);
    // Configure timeouts.
    this.startSegmentTimeoutMs = conf.getInt(
        DFSConfigKeys.DFS_QJOURNAL_START_SEGMENT_TIMEOUT_KEY,
        DFSConfigKeys.DFS_QJOURNAL_START_SEGMENT_TIMEOUT_DEFAULT);
    this.prepareRecoveryTimeoutMs = conf.getInt(
        DFSConfigKeys.DFS_QJOURNAL_PREPARE_RECOVERY_TIMEOUT_KEY,
        DFSConfigKeys.DFS_QJOURNAL_PREPARE_RECOVERY_TIMEOUT_DEFAULT);
    this.acceptRecoveryTimeoutMs = conf.getInt(
        DFSConfigKeys.DFS_QJOURNAL_ACCEPT_RECOVERY_TIMEOUT_KEY,
        DFSConfigKeys.DFS_QJOURNAL_ACCEPT_RECOVERY_TIMEOUT_DEFAULT);
    this.finalizeSegmentTimeoutMs = conf.getInt(
        DFSConfigKeys.DFS_QJOURNAL_FINALIZE_SEGMENT_TIMEOUT_KEY,
        DFSConfigKeys.DFS_QJOURNAL_FINALIZE_SEGMENT_TIMEOUT_DEFAULT);
    this.selectInputStreamsTimeoutMs = conf.getInt(
        DFSConfigKeys.DFS_QJOURNAL_SELECT_INPUT_STREAMS_TIMEOUT_KEY,
        DFSConfigKeys.DFS_QJOURNAL_SELECT_INPUT_STREAMS_TIMEOUT_DEFAULT);
    this.getJournalStateTimeoutMs = conf.getInt(
        DFSConfigKeys.DFS_QJOURNAL_GET_JOURNAL_STATE_TIMEOUT_KEY,
        DFSConfigKeys.DFS_QJOURNAL_GET_JOURNAL_STATE_TIMEOUT_DEFAULT);
    this.newEpochTimeoutMs = conf.getInt(
        DFSConfigKeys.DFS_QJOURNAL_NEW_EPOCH_TIMEOUT_KEY,
        DFSConfigKeys.DFS_QJOURNAL_NEW_EPOCH_TIMEOUT_DEFAULT);
    this.writeTxnsTimeoutMs = conf.getInt(
        DFSConfigKeys.DFS_QJOURNAL_WRITE_TXNS_TIMEOUT_KEY,
        DFSConfigKeys.DFS_QJOURNAL_WRITE_TXNS_TIMEOUT_DEFAULT);
    this.timeoutMs = (int) conf.getTimeDuration(DFSConfigKeys
            .DFS_QJM_OPERATIONS_TIMEOUT,
        DFSConfigKeys.DFS_QJM_OPERATIONS_TIMEOUT_DEFAULT, TimeUnit
            .MILLISECONDS);

    int connectTimeoutMs = conf.getInt(
        DFSConfigKeys.DFS_QJOURNAL_HTTP_OPEN_TIMEOUT_KEY,
        DFSConfigKeys.DFS_QJOURNAL_HTTP_OPEN_TIMEOUT_DEFAULT);
    int readTimeoutMs = conf.getInt(
        DFSConfigKeys.DFS_QJOURNAL_HTTP_READ_TIMEOUT_KEY,
        DFSConfigKeys.DFS_QJOURNAL_HTTP_READ_TIMEOUT_DEFAULT);
    this.connectionFactory = URLConnectionFactory
        .newDefaultURLConnectionFactory(connectTimeoutMs, readTimeoutMs, conf);
    setOutputBufferCapacity(OUTPUT_BUFFER_CAPACITY_DEFAULT);
  }
  
  protected List<AsyncLogger> createLoggers(
      AsyncLogger.Factory factory) throws IOException {
    return createLoggers(conf, uri, nsInfo, factory, nameServiceId);
  }

  static String parseJournalId(URI uri) {
    String path = uri.getPath();
    Preconditions.checkArgument(path != null && !path.isEmpty(),
        "Bad URI '%s': must identify journal in path component",
        uri);
    String journalId = path.substring(1);
    checkJournalId(journalId);
    return journalId;
  }
  
  public static void checkJournalId(String jid) {
    Preconditions.checkArgument(jid != null &&
        !jid.isEmpty() &&
        !jid.contains("/") &&
        !jid.startsWith("."),
        "bad journal id: " + jid);
  }

  
  /**
   * Fence any previous writers, and obtain a unique epoch number
   * for write-access to the journal nodes.
   *
   * @return the new, unique epoch number
   */
  //获取一个新的、唯一的 epoch 编号，确保当前 QuorumJournalManager 实例拥有对 JournalNode 集群的写入权限
  //阻止（Fence）其他可能存在的写入者，防止数据竞争，保证事务日志的写入一致性
  //在 HDFS 高可用(HA)架构中，多个 NameNode 可能会与相同的 JournalNode 集群通信，
  // epoch 作为一个递增的唯一编号，用于确保同一时间只有一个 NameNode 能够执行写操作
  Map<AsyncLogger, NewEpochResponseProto> createNewUniqueEpoch()
      throws IOException {
    Preconditions.checkState(!loggers.isEpochEstablished(),
        "epoch already created");
    //获取当前 JournalNode 集群的状态
    Map<AsyncLogger, GetJournalStateResponseProto> lastPromises =
      loggers.waitForWriteQuorum(loggers.getJournalState(),
          getJournalStateTimeoutMs, "getJournalState()");
    //计算下一个可用的 epoch 编号
    long maxPromised = Long.MIN_VALUE;
    for (GetJournalStateResponseProto resp : lastPromises.values()) {
      maxPromised = Math.max(maxPromised, resp.getLastPromisedEpoch());
    }
    assert maxPromised >= 0;
    //向 JournalNode 请求设置新 epoch
    long myEpoch = maxPromised + 1;
    Map<AsyncLogger, NewEpochResponseProto> resps =
        loggers.waitForWriteQuorum(loggers.newEpoch(nsInfo, myEpoch),
            newEpochTimeoutMs, "newEpoch(" + myEpoch + ")");
        
    loggers.setEpoch(myEpoch);
    return resps;
  }
  // 主要功能是对 JournalNode 集群执行 格式化操作，即将 JournalNode 的状态初始化或重置。
  // 这在 HDFS 系统中用于初始化日志存储，使其准备好接收新的事务日志。格式化过程中可以选择是否强制执行（force 参数），
  // 即使存在一些错误或潜在问题也进行格式化
  @Override
  public void format(NamespaceInfo nsInfo, boolean force) throws IOException {
    QuorumCall<AsyncLogger, Void> call = loggers.format(nsInfo, force);
    try {
      call.waitFor(loggers.size(), loggers.size(), 0, timeoutMs,
          "format");
    } catch (InterruptedException e) {
      throw new IOException("Interrupted waiting for format() response");
    } catch (TimeoutException e) {
      throw new IOException("Timed out waiting for format() response");
    }
    
    if (call.countExceptions() > 0) {
      call.rethrowException("Could not format one or more JournalNodes");
    }
  }
 // 检查当前的 JournalNode（JN）是否包含数据，即判断是否已经有格式化的数据存储在日志中。
 // 如果日志中有数据，则返回 true，表示日志节点中存在数据；如果没有数据，则返回 false，表明日志可以进行格式化操作
  @Override
  public boolean hasSomeData() throws IOException {
    QuorumCall<AsyncLogger, Boolean> call =
        loggers.isFormatted();

    try {
      call.waitFor(loggers.size(), 0, 0, timeoutMs, "hasSomeData");
    } catch (InterruptedException e) {
      throw new IOException("Interrupted while determining if JNs have data");
    } catch (TimeoutException e) {
      throw new IOException("Timed out waiting for response from loggers");
    }
    
    if (call.countExceptions() > 0) {
      call.rethrowException(
          "Unable to check if JNs are ready for formatting");
    }
    
    // If any of the loggers returned with a non-empty manifest, then
    // we should prompt for format.
    for (Boolean hasData : call.getResults().values()) {
      if (hasData) {
        return true;
      }
    }

    // Otherwise, none were formatted, we can safely format.
    return false;
  }

  /**
   * Run recovery/synchronization for a specific segment.
   * Postconditions:
   * <ul>
   * <li>This segment will be finalized on a majority
   * of nodes.</li>
   * <li>All nodes which contain the finalized segment will
   * agree on the length.</li>
   * </ul>
   * 
   * @param segmentTxId the starting txid of the segment
   * @throws IOException
   */
  //用于恢复一个未关闭的日志段（segment）。
  // 它的主要目的是确保在多数 JournalNode 上完成该日志段的最终确认，并同步所有 JournalNode 对该段的长度达成一致，确保数据一致性
  private void recoverUnclosedSegment(long segmentTxId) throws IOException {
    Preconditions.checkArgument(segmentTxId > 0);
    LOG.info("Beginning recovery of unclosed segment starting at txid " +
        segmentTxId);
    
    // Step 1. Prepare recovery
    //准备恢复
    //为恢复操作准备请求，询问每个 JournalNode 是否已记录该事务 ID 的日志段
    QuorumCall<AsyncLogger,PrepareRecoveryResponseProto> prepare =
        loggers.prepareRecovery(segmentTxId);
    Map<AsyncLogger, PrepareRecoveryResponseProto> prepareResponses=
        loggers.waitForWriteQuorum(prepare, prepareRecoveryTimeoutMs,
            "prepareRecovery(" + segmentTxId + ")");
    LOG.info("Recovery prepare phase complete. Responses:\n" +
        QuorumCall.mapToString(prepareResponses));

    // Determine the logger who either:
    // a) Has already accepted a previous proposal that's higher than any
    //    other
    //
    //  OR, if no such logger exists:
    //
    // b) Has the longest log starting at this transaction ID
    
    // TODO: we should collect any "ties" and pass the URL for all of them
    // when syncing, so we can tolerate failure during recovery better.
    //选择最佳日志
    //使用 SegmentRecoveryComparator 对返回的日志段进行比较，选择出日志条目中状态最合适的节点
    //如果有多个节点已经接受了某个恢复方案或日志段，则选择日志最长的节点
    Entry<AsyncLogger, PrepareRecoveryResponseProto> bestEntry = Collections.max(
        prepareResponses.entrySet(), SegmentRecoveryComparator.INSTANCE); 
    AsyncLogger bestLogger = bestEntry.getKey();
    PrepareRecoveryResponseProto bestResponse = bestEntry.getValue();
    //日志恢复决策与输出
    // Log the above decision, check invariants.
    //如果某个日志节点已经接受了一个更高版本的日志段，直接采用该日志段
    if (bestResponse.hasAcceptedInEpoch()) {
      LOG.info("Using already-accepted recovery for segment " +
          "starting at txid " + segmentTxId + ": " +
          bestEntry);
      //如果没有节点已经接受高版本的日志段，选择最长的日志段
    } else if (bestResponse.hasSegmentState()) {
      LOG.info("Using longest log: " + bestEntry);
    } else {
      //如果所有响应的节点都没有日志段，表示可能发生了节点崩溃的情况，可以跳过当前恢复操作
      // None of the responses to prepareRecovery() had a segment at the given
      // txid. This can happen for example in the following situation:
      // - 3 JNs: JN1, JN2, JN3
      // - writer starts segment 101 on JN1, then crashes before
      //   writing to JN2 and JN3
      // - during newEpoch(), we saw the segment on JN1 and decide to
      //   recover segment 101
      // - before prepare(), JN1 crashes, and we only talk to JN2 and JN3,
      //   neither of which has any entry for this log.
      // In this case, it is allowed to do nothing for recovery, since the
      // segment wasn't started on a quorum of nodes.

      // Sanity check: we should only get here if none of the responses had
      // a log. This should be a postcondition of the recovery comparator,
      // but a bug in the comparator might cause us to get here.
      for (PrepareRecoveryResponseProto resp : prepareResponses.values()) {
        assert !resp.hasSegmentState() :
          "One of the loggers had a response, but no best logger " +
          "was found.";
      }

      LOG.info("None of the responders had a log to recover: " +
          QuorumCall.mapToString(prepareResponses));
      return;
    }
    //检查并验证日志一致性
    SegmentStateProto logToSync = bestResponse.getSegmentState();
    assert segmentTxId == logToSync.getStartTxId();
    
    // Sanity check: none of the loggers should be aware of a higher
    // txid than the txid we intend to truncate to
    for (Map.Entry<AsyncLogger, PrepareRecoveryResponseProto> e :
         prepareResponses.entrySet()) {
      AsyncLogger logger = e.getKey();
      PrepareRecoveryResponseProto resp = e.getValue();

      if (resp.hasLastCommittedTxId() &&
          resp.getLastCommittedTxId() > logToSync.getEndTxId()) {
        throw new AssertionError("Decided to synchronize log to " + logToSync +
            " but logger " + logger + " had seen txid " +
            resp.getLastCommittedTxId() + " committed");
      }
    }
    //获取日志同步 URL 并执行恢复
    URL syncFromUrl = bestLogger.buildURLToFetchLogs(segmentTxId);
    //向所有 JournalNode 发起恢复接受请求，将同步的日志段传递给它们
    QuorumCall<AsyncLogger,Void> accept = loggers.acceptRecovery(logToSync, syncFromUrl);
    loggers.waitForWriteQuorum(accept, acceptRecoveryTimeoutMs,
        "acceptRecovery(" + TextFormat.shortDebugString(logToSync) + ")");

    // If one of the loggers above missed the synchronization step above, but
    // we send a finalize() here, that's OK. It validates the log before
    // finalizing. Hence, even if it is not "in sync", it won't incorrectly
    // finalize.
    QuorumCall<AsyncLogger, Void> finalize =
        loggers.finalizeLogSegment(logToSync.getStartTxId(), logToSync.getEndTxId()); 
    loggers.waitForWriteQuorum(finalize, finalizeSegmentTimeoutMs,
        String.format("finalizeLogSegment(%s-%s)",
            logToSync.getStartTxId(),
            logToSync.getEndTxId()));
  }


  // 根据提供的URI和配置信息，创建与JournalNode通信的AsyncLogger对象列表
  // 每个AsyncLogger对象代表一个与JournalNode通信的客户端，
  // QuorumJournalManager通过这些日志记录器与多个JournalNode进行交互，确保事务日志 (EditLog) 的可靠写入和读取
  static List<AsyncLogger> createLoggers(Configuration conf,
                                         URI uri,//JournalNode 集群的 URI，指示事务日志的存储位置
                                         NamespaceInfo nsInfo, //HDFS 命名空间信息，包含集群的 namespaceID、clusterID
                                         AsyncLogger.Factory factory,
                                         String nameServiceId) //服务的唯一标识，区分不同的 HDFS 集群
      throws IOException {
    List<AsyncLogger> ret = Lists.newArrayList();
    //每个 InetSocketAddress 对象代表一个 JournalNode 的 IP 地址和端口
    //例如qjournal://host1:8485;host2:8485;host3:8485/myjournal
    List<InetSocketAddress> addrs = Util.getAddressesList(uri, conf);
    if (addrs.size() % 2 == 0) {
      LOG.warn("Quorum journal URI '" + uri + "' has an even number " +
          "of Journal Nodes specified. This is not recommended!");
    }
    String jid = parseJournalId(uri);
    for (InetSocketAddress addr : addrs) {
      ret.add(factory.createLogger(conf, nsInfo, jid, nameServiceId, addr));
    }
    return ret;
  }
  // 启动一个新的日志段，用于记录事务（transaction）日志。启动日志段前，会确保系统处于活跃状态，并向 JournalNode 发起请求确认启动该日志段。
  // 返回一个 EditLogOutputStream 实例，表示可以写入新日志段的输出流
  @Override
  public EditLogOutputStream startLogSegment(long txId, int layoutVersion)
      throws IOException {
    Preconditions.checkState(isActiveWriter,
        "must recover segments before starting a new one");
    QuorumCall<AsyncLogger, Void> q = loggers.startLogSegment(txId,
        layoutVersion);
    loggers.waitForWriteQuorum(q, startSegmentTimeoutMs,
        "startLogSegment(" + txId + ")");
    return new QuorumOutputStream(loggers, txId, outputBufferCapacity,
        writeTxnsTimeoutMs, layoutVersion);
  }

  @Override
  public void finalizeLogSegment(long firstTxId, long lastTxId)
      throws IOException {
    QuorumCall<AsyncLogger,Void> q = loggers.finalizeLogSegment(
        firstTxId, lastTxId);
    loggers.waitForWriteQuorum(q, finalizeSegmentTimeoutMs,
        String.format("finalizeLogSegment(%s-%s)", firstTxId, lastTxId));
  }

  @Override
  public void setOutputBufferCapacity(int size) {
    int ipcMaxDataLength = conf.getInt(
        CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH,
        CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH_DEFAULT);
    if (size >= ipcMaxDataLength) {
      throw new IllegalArgumentException("Attempted to use QJM output buffer "
          + "capacity (" + size + ") greater than the IPC max data length ("
          + CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH + " = "
          + ipcMaxDataLength + "). This will cause journals to reject edits.");
    }
    outputBufferCapacity = size;
  }

  @Override
  public void purgeLogsOlderThan(long minTxIdToKeep) throws IOException {
    // This purges asynchronously -- there's no need to wait for a quorum
    // here, because it's always OK to fail.
    LOG.info("Purging remote journals older than txid " + minTxIdToKeep);
    loggers.purgeLogsOlderThan(minTxIdToKeep);
  }

  @Override
  public void recoverUnfinalizedSegments() throws IOException {
    Preconditions.checkState(!isActiveWriter, "already active writer");
    
    LOG.info("Starting recovery process for unclosed journal segments...");
    Map<AsyncLogger, NewEpochResponseProto> resps = createNewUniqueEpoch();
    LOG.info("Successfully started new epoch " + loggers.getEpoch());

    if (LOG.isDebugEnabled()) {
      LOG.debug("newEpoch({}) responses:\n{}", loggers.getEpoch(), QuorumCall.mapToString(resps));
    }

    long mostRecentSegmentTxId = Long.MIN_VALUE;
    for (NewEpochResponseProto r : resps.values()) {
      if (r.hasLastSegmentTxId()) {
        mostRecentSegmentTxId = Math.max(mostRecentSegmentTxId,
            r.getLastSegmentTxId());
      }
    }
    
    // On a completely fresh system, none of the journals have any
    // segments, so there's nothing to recover.
    if (mostRecentSegmentTxId != Long.MIN_VALUE) {
      recoverUnclosedSegment(mostRecentSegmentTxId);
    }
    isActiveWriter = true;
  }

  @Override
  public void close() throws IOException {
    loggers.close();
    connectionFactory.destroy();
  }

  public void selectInputStreams(Collection<EditLogInputStream> streams,
      long fromTxnId, boolean inProgressOk) throws IOException {
    selectInputStreams(streams, fromTxnId, inProgressOk, false);
  }

  @Override
  public void selectInputStreams(Collection<EditLogInputStream> streams,
      long fromTxnId, boolean inProgressOk,
      boolean onlyDurableTxns) throws IOException {
    // Some calls will use inProgressOK to get in-progress edits even if
    // the cache used for RPC calls is not enabled; fall back to using the
    // streaming mechanism to serve such requests
    if (inProgressOk && inProgressTailingEnabled) {
      LOG.debug("Tailing edits starting from txn ID {} via RPC mechanism", fromTxnId);
      try {
        Collection<EditLogInputStream> rpcStreams = new ArrayList<>();
        selectRpcInputStreams(rpcStreams, fromTxnId, onlyDurableTxns);
        streams.addAll(rpcStreams);
        return;
      } catch (IOException ioe) {
        LOG.warn("Encountered exception while tailing edits >= " + fromTxnId +
            " via RPC; falling back to streaming.", ioe);
      }
    }
    selectStreamingInputStreams(streams, fromTxnId, inProgressOk,
        onlyDurableTxns);
  }

  /**
   * Select input streams from the journals, specifically using the RPC
   * mechanism optimized for low latency.
   *
   * @param streams The collection to store the return streams into.
   * @param fromTxnId Select edits starting from this transaction ID
   * @param onlyDurableTxns Iff true, only include transactions which have been
   *                        committed to a quorum of the journals.
   * @throws IOException Upon issues, including cache misses on the journals.
   */
  private void selectRpcInputStreams(Collection<EditLogInputStream> streams,
      long fromTxnId, boolean onlyDurableTxns) throws IOException {
    QuorumCall<AsyncLogger, GetJournaledEditsResponseProto> q =
        loggers.getJournaledEdits(fromTxnId, maxTxnsPerRpc);
    Map<AsyncLogger, GetJournaledEditsResponseProto> responseMap =
        loggers.waitForWriteQuorum(q, selectInputStreamsTimeoutMs,
            "selectRpcInputStreams");
    assert responseMap.size() >= loggers.getMajoritySize() :
        "Quorum call returned without a majority";

    List<Integer> responseCounts = new ArrayList<>();
    for (GetJournaledEditsResponseProto resp : responseMap.values()) {
      responseCounts.add(resp.getTxnCount());
    }
    Collections.sort(responseCounts);
    int highestTxnCount = responseCounts.get(responseCounts.size() - 1);
    if (LOG.isDebugEnabled() || highestTxnCount < 0) {
      StringBuilder msg = new StringBuilder("Requested edits starting from ");
      msg.append(fromTxnId).append("; got ").append(responseMap.size())
          .append(" responses: <");
      for (Map.Entry<AsyncLogger, GetJournaledEditsResponseProto> ent :
          responseMap.entrySet()) {
        msg.append("[").append(ent.getKey()).append(", ")
            .append(ent.getValue().getTxnCount()).append("],");
      }
      msg.append(">");
      if (highestTxnCount < 0) {
        throw new IOException("Did not get any valid JournaledEdits " +
            "responses: " + msg);
      } else {
        LOG.debug(msg.toString());
      }
    }
    // Cancel any outstanding calls to JN's.
    q.cancelCalls();

    int maxAllowedTxns = !onlyDurableTxns ? highestTxnCount :
        responseCounts.get(responseCounts.size() - loggers.getMajoritySize());
    if (maxAllowedTxns == 0) {
      LOG.debug("No new edits available in logs; requested starting from ID {}",
          fromTxnId);
      return;
    }
    LogAction logAction = selectInputStreamLogHelper.record(fromTxnId);
    if (logAction.shouldLog()) {
      LOG.info("Selected loggers with >= " + maxAllowedTxns + " transactions " +
          "starting from lowest txn ID " + logAction.getStats(0).getMin() +
          LogThrottlingHelper.getLogSupressionMessage(logAction));
    }
    PriorityQueue<EditLogInputStream> allStreams = new PriorityQueue<>(
        JournalSet.EDIT_LOG_INPUT_STREAM_COMPARATOR);
    for (GetJournaledEditsResponseProto resp : responseMap.values()) {
      long endTxnId = fromTxnId - 1 +
          Math.min(maxAllowedTxns, resp.getTxnCount());
      allStreams.add(EditLogFileInputStream.fromByteString(
          resp.getEditLog(), fromTxnId, endTxnId, true));
    }
    JournalSet.chainAndMakeRedundantStreams(streams, allStreams, fromTxnId);
  }

  /**
   * Select input streams from the journals, specifically using the streaming
   * mechanism optimized for resiliency / bulk load.
   */
  private void selectStreamingInputStreams(
      Collection<EditLogInputStream> streams, long fromTxnId,
      boolean inProgressOk, boolean onlyDurableTxns) throws IOException {
    QuorumCall<AsyncLogger, RemoteEditLogManifest> q =
        loggers.getEditLogManifest(fromTxnId, inProgressOk);
    Map<AsyncLogger, RemoteEditLogManifest> resps =
        loggers.waitForWriteQuorum(q, selectInputStreamsTimeoutMs,
            "selectStreamingInputStreams");
    if (LOG.isDebugEnabled()) {
      LOG.debug("selectStreamingInputStream manifests:\n {}",
          Joiner.on("\n").withKeyValueSeparator(": ").join(resps));
    }

    final PriorityQueue<EditLogInputStream> allStreams =
        new PriorityQueue<EditLogInputStream>(64,
            JournalSet.EDIT_LOG_INPUT_STREAM_COMPARATOR);
    for (Map.Entry<AsyncLogger, RemoteEditLogManifest> e : resps.entrySet()) {
      AsyncLogger logger = e.getKey();
      RemoteEditLogManifest manifest = e.getValue();
      long committedTxnId = manifest.getCommittedTxnId();

      for (RemoteEditLog remoteLog : manifest.getLogs()) {
        URL url = logger.buildURLToFetchLogs(remoteLog.getStartTxId());

        long endTxId = remoteLog.getEndTxId();

        // If it's bounded by durable Txns, endTxId could not be larger
        // than committedTxnId. This ensures the consistency.
        // We don't do the following for finalized log segments, since all
        // edits in those are guaranteed to be committed.
        if (onlyDurableTxns && inProgressOk && remoteLog.isInProgress()) {
          endTxId = Math.min(endTxId, committedTxnId);
          if (endTxId < remoteLog.getStartTxId()) {
            LOG.warn("Found endTxId (" + endTxId + ") that is less than " +
                "the startTxId (" + remoteLog.getStartTxId() +
                ") - setting it to startTxId.");
            endTxId = remoteLog.getStartTxId();
          }
        }

        EditLogInputStream elis = EditLogFileInputStream.fromUrl(
            connectionFactory, url, remoteLog.getStartTxId(),
            endTxId, remoteLog.isInProgress());
        allStreams.add(elis);
      }
    }
    JournalSet.chainAndMakeRedundantStreams(streams, allStreams, fromTxnId);
  }
  
  @Override
  public String toString() {
    return "QJM to " + loggers;
  }

  @VisibleForTesting
  AsyncLoggerSet getLoggerSetForTests() {
    return loggers;
  }

  @Override
  public void doPreUpgrade() throws IOException {
    QuorumCall<AsyncLogger, Void> call = loggers.doPreUpgrade();
    try {
      call.waitFor(loggers.size(), loggers.size(), 0, timeoutMs,
          "doPreUpgrade");
      
      if (call.countExceptions() > 0) {
        call.rethrowException("Could not do pre-upgrade of one or more JournalNodes");
      }
    } catch (InterruptedException e) {
      throw new IOException("Interrupted waiting for doPreUpgrade() response");
    } catch (TimeoutException e) {
      throw new IOException("Timed out waiting for doPreUpgrade() response");
    }
  }

  @Override
  public void doUpgrade(Storage storage) throws IOException {
    QuorumCall<AsyncLogger, Void> call = loggers.doUpgrade(storage);
    try {
      call.waitFor(loggers.size(), loggers.size(), 0, timeoutMs,
          "doUpgrade");
      
      if (call.countExceptions() > 0) {
        call.rethrowException("Could not perform upgrade of one or more JournalNodes");
      }
    } catch (InterruptedException e) {
      throw new IOException("Interrupted waiting for doUpgrade() response");
    } catch (TimeoutException e) {
      throw new IOException("Timed out waiting for doUpgrade() response");
    }
  }
  
  @Override
  public void doFinalize() throws IOException {
    QuorumCall<AsyncLogger, Void> call = loggers.doFinalize();
    try {
      call.waitFor(loggers.size(), loggers.size(), 0, timeoutMs,
          "doFinalize");
      
      if (call.countExceptions() > 0) {
        call.rethrowException("Could not finalize one or more JournalNodes");
      }
    } catch (InterruptedException e) {
      throw new IOException("Interrupted waiting for doFinalize() response");
    } catch (TimeoutException e) {
      throw new IOException("Timed out waiting for doFinalize() response");
    }
  }
  
  @Override
  public boolean canRollBack(StorageInfo storage, StorageInfo prevStorage,
      int targetLayoutVersion) throws IOException {
    QuorumCall<AsyncLogger, Boolean> call = loggers.canRollBack(storage,
        prevStorage, targetLayoutVersion);
    try {
      call.waitFor(loggers.size(), loggers.size(), 0, timeoutMs,
          "lockSharedStorage");
      
      if (call.countExceptions() > 0) {
        call.rethrowException("Could not check if roll back possible for"
            + " one or more JournalNodes");
      }
      
      // Either they all return the same thing or this call fails, so we can
      // just return the first result.
      try {
        DFSUtil.assertAllResultsEqual(call.getResults().values());
      } catch (AssertionError ae) {
        throw new IOException("Results differed for canRollBack", ae);
      }
      for (Boolean result : call.getResults().values()) {
        return result;
      }
    } catch (InterruptedException e) {
      throw new IOException("Interrupted waiting for lockSharedStorage() " +
          "response");
    } catch (TimeoutException e) {
      throw new IOException("Timed out waiting for lockSharedStorage() " +
          "response");
    }
    
    throw new AssertionError("Unreachable code.");
  }

  @Override
  public void doRollback() throws IOException {
    QuorumCall<AsyncLogger, Void> call = loggers.doRollback();
    try {
      call.waitFor(loggers.size(), loggers.size(), 0, timeoutMs,
          "doRollback");
      
      if (call.countExceptions() > 0) {
        call.rethrowException("Could not perform rollback of one or more JournalNodes");
      }
    } catch (InterruptedException e) {
      throw new IOException("Interrupted waiting for doFinalize() response");
    } catch (TimeoutException e) {
      throw new IOException("Timed out waiting for doFinalize() response");
    }
  }
  
  @Override
  public void discardSegments(long startTxId) throws IOException {
    QuorumCall<AsyncLogger, Void> call = loggers.discardSegments(startTxId);
    try {
      call.waitFor(loggers.size(), loggers.size(), 0,
          timeoutMs, "discardSegments");
      if (call.countExceptions() > 0) {
        call.rethrowException(
            "Could not perform discardSegments of one or more JournalNodes");
      }
    } catch (InterruptedException e) {
      throw new IOException(
          "Interrupted waiting for discardSegments() response");
    } catch (TimeoutException e) {
      throw new IOException(
          "Timed out waiting for discardSegments() response");
    }
  }
  
  @Override
  public long getJournalCTime() throws IOException {
    QuorumCall<AsyncLogger, Long> call = loggers.getJournalCTime();
    try {
      call.waitFor(loggers.size(), loggers.size(), 0,
          timeoutMs, "getJournalCTime");
      
      if (call.countExceptions() > 0) {
        call.rethrowException("Could not journal CTime for one "
            + "more JournalNodes");
      }
      
      // Either they all return the same thing or this call fails, so we can
      // just return the first result.
      try {
        DFSUtil.assertAllResultsEqual(call.getResults().values());
      } catch (AssertionError ae) {
        throw new IOException("Results differed for getJournalCTime", ae);
      }
      for (Long result : call.getResults().values()) {
        return result;
      }
    } catch (InterruptedException e) {
      throw new IOException("Interrupted waiting for getJournalCTime() " +
          "response");
    } catch (TimeoutException e) {
      throw new IOException("Timed out waiting for getJournalCTime() " +
          "response");
    }
    
    throw new AssertionError("Unreachable code.");
  }
}
