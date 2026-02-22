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

import java.io.IOException;
import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSStripedOutputStream.Coordinator;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.util.ByteArrayManager;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Progressable;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * This class extends {@link DataStreamer} to support writing striped blocks
 * to datanodes.
 * A {@link DFSStripedOutputStream} has multiple {@link StripedDataStreamer}s.
 * Whenever the streamers need to talk the namenode, only the fastest streamer
 * sends an rpc call to the namenode and then populates the result for the
 * other streamers.
 */
// StripedDataStreamer 继承自 DataStreamer。
// 在传统的 HDFS 副本模式中，一个文件通常对应一个 DataStreamer；
// 但在 纠删码（EC）模式 下，文件被切分为多个数据单元（Data Units）和校验单元（Parity Units）。
// 多线程协同：一个 DFSStripedOutputStream 会同时拥有多个 StripedDataStreamer。例如，在 RS-6-3 策略下，会有 9 个 Streamer 同时工作。
// 并行写入：每个 Streamer 负责将自己对应的那一列条带数据写入指定的 DataNode。
// 协同机制（Coordinator）：由于多个 Streamer 共享一个“块组（Block Group）”，它们在申请新块或处理错误时需要步调一致。
// 该类通过 Coordinator 减少了对 NameNode 的冗余 RPC 调用（仅由一个 Streamer 代表大家去申请，其余人共享结果）。
// 与普通模式最大的不同在于，StripedDataStreamer 丧失了独立性。它的每一次“跨步”（申请块、重建管道、结束块）都必须通过 Coordinator 与兄弟 Streamer 进行“对表”。这种设计确保了分布式环境下块组（Block Group）数据的一致性。
@InterfaceAudience.Private
public class StripedDataStreamer extends DataStreamer {
  // 核心协调器。
  // 用于在多个 Streamer 之间同步状态。
  // 例如：当所有 Streamer 都写完一个块时，协调结束块的操作；或者代表所有 Streamer 向 NameNode 申请新块。
  private final Coordinator coordinator;
  // 索引编号。标识当前 Streamer 负责的是块组中的第几个条带（例如第 0 个是数据块，第 6 个可能是校验块）。
  private final int index;

  StripedDataStreamer(HdfsFileStatus stat,
                      DFSClient dfsClient, String src,
                      Progressable progress, DataChecksum checksum,
                      AtomicReference<CachingStrategy> cachingStrategy,
                      ByteArrayManager byteArrayManage, String[] favoredNodes,
                      short index, Coordinator coordinator,
                      final EnumSet<AddBlockFlag> flags) {
    super(stat, null, dfsClient, src, progress, checksum, cachingStrategy,
        byteArrayManage, favoredNodes, flags);
    this.index = index;
    this.coordinator = coordinator;
  }
  // 获取当前 Streamer 的索引值。
  int getIndex() {
    return index;
  }
  // 判断当前 Streamer 是否健康。如果不健康（线程已关闭或内部发生错误），则返回 false。
  boolean isHealthy() {
    return !streamerClosed() && !getErrorState().hasInternalError();
  }

  @Override
  protected void endBlock() {
    coordinator.offerEndBlock(index, block.getCurrentBlock());
    super.endBlock();
  }

  /**
   * The upper level DFSStripedOutputStream will allocate the new block group.
   * All the striped data streamer only needs to fetch from the queue, which
   * should be already be ready.
   */
  // 这是 EC 模式的优化：它不再直接去调 NameNode 申请块，而是从协调器的队列（followingBlocks）中弹出（poll）已经由其他“先行”Streamer 申请好的块信息。
  private LocatedBlock getFollowingBlock() throws IOException {
    if (!this.isHealthy()) {
      // No internal block for this streamer, maybe no enough healthy DN.
      // Throw the exception which has been set by the StripedOutputStream.
      this.getLastException().check(false);
    }
    return coordinator.getFollowingBlocks().poll(index);
  }
  // 重写父类方法。用于开启下一个块的输出流。
  @Override
  protected LocatedBlock nextBlockOutputStream() throws IOException {
    boolean success;
    LocatedBlock lb = getFollowingBlock();
    block.setCurrentBlock(lb.getBlock());
    block.setNumBytes(0);
    bytesSent = 0;
    accessToken = lb.getBlockToken();

    DatanodeInfo[] nodes = lb.getLocations();
    StorageType[] storageTypes = lb.getStorageTypes();
    String[] storageIDs = lb.getStorageIDs();

    // Connect to the DataNode. If fail the internal error state will be set.
    success = createBlockOutputStream(nodes, storageTypes, storageIDs, 0L,
        false);

    if (!success) {
      block.setCurrentBlock(null);
      final DatanodeInfo badNode = nodes[getErrorState().getBadNodeIndex()];
      LOG.warn("Excluding datanode " + badNode);
      excludedNodes.put(badNode, badNode);
      throw new IOException("Unable to create new block." + this);
    }
    return lb;
  }

  @VisibleForTesting
  LocatedBlock peekFollowingBlock() {
    return coordinator.getFollowingBlocks().peek(index);
  }
  // 当写入发生错误需要重建管道时：
  @Override
  protected void setupPipelineInternal(DatanodeInfo[] nodes,
      StorageType[] nodeStorageTypes, String[] nodeStorageIDs)
      throws IOException {
    boolean success = false;
    while (!success && !streamerClosed() && dfsClient.clientRunning) {
      if (!handleRestartingDatanode()) {
        return;
      }
      if (!handleBadDatanode()) {
        // for striped streamer if it is datanode error then close the stream
        // and return. no need to replace datanode
        return;
      }

      // get a new generation stamp and an access token
      final LocatedBlock lb = coordinator.getNewBlocks().take(index);
      long newGS = lb.getBlock().getGenerationStamp();
      setAccessToken(lb.getBlockToken());

      // set up the pipeline again with the remaining nodes. when a striped
      // data streamer comes here, it must be in external error state.
      assert getErrorState().hasExternalError()
          || getErrorState().doWaitForRestart();
      success = createBlockOutputStream(nodes, nodeStorageTypes,
          nodeStorageIDs, newGS, true);

      failPacket4Testing();
      getErrorState().checkRestartingNodeDeadline(nodes);

      // notify coordinator the result of createBlockOutputStream
      synchronized (coordinator) {
        if (!streamerClosed()) {
          coordinator.updateStreamer(this, success);
          coordinator.notify();
        } else {
          success = false;
        }
      }

      if (success) {
        // wait for results of other streamers
        success = coordinator.takeStreamerUpdateResult(index);
        if (success) {
          // if all succeeded, update its block using the new GS
          updateBlockGS(newGS);
        } else {
          // otherwise close the block stream and restart the recovery process
          closeStream();
        }
      } else {
        // if fail, close the stream. The internal error state and last
        // exception have already been set in createBlockOutputStream
        // TODO: wait for restarting DataNodes during RollingUpgrade
        closeStream();
        setStreamerAsClosed();
      }
    } // while
  }

  void setExternalError() {
    getErrorState().setExternalError();
    synchronized (dataQueue) {
      dataQueue.notifyAll();
    }
  }

  @Override
  public String toString() {
    return "#" + index + ": " + (!isHealthy() ? "failed, ": "") + super.toString();
  }
}
