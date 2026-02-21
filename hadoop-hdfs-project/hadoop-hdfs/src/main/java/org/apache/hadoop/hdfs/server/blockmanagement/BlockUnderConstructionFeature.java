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
package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState.COMPLETE;

/**
 * Represents the under construction feature of a Block.
 * This is usually the last block of a file opened for write or append.
 */
// 采用了组合模式（Feature 模式），专门为那些处于“构建中”状态（正在写入、追加或恢复）的数据块提供扩展功能。
// 在 HDFS 中，大部分数据块是只读且完整的（COMPLETE）。但对于文件最后一个正在写入的数据块，它具有复杂的中间状态。BlockUnderConstructionFeature 封装了这些复杂性：
// 状态维护：记录块是正在被写入（UNDER_CONSTRUCTION）、已提交（COMMITTED）还是正在恢复（UNDER_RECOVERY）。
// 位置追踪：记录数据块被分配到了哪些 DataNode（即写入流水线的成员）。
// 故障恢复协调：当客户端宕机时，它负责协调“租约恢复”（Lease Recovery），选出主导节点来同步数据。
// EC 模式支持：支持纠删码（Striped Block）的特殊索引处理。
public class BlockUnderConstructionFeature {
  // 数据块的构建状态
  private BlockUCState blockUCState;
  private static final ReplicaUnderConstruction[] NO_REPLICAS =
      new ReplicaUnderConstruction[0];

  /**
   * Block replicas as assigned when the block was allocated.
   */
  // 存储所有分配好的副本信息。每个 ReplicaUnderConstruction 对应一个 DataNode 存储位置。
  private ReplicaUnderConstruction[] replicas = NO_REPLICAS;

  /**
   * Index of the primary data node doing the recovery. Useful for log
   * messages.
   */
  // 记录当前负责执行“恢复任务”的主 DataNode 在 replicas 数组中的索引。
  private int primaryNodeIndex = -1;

  /**
   * The new generation stamp, which this block will have
   * after the recovery succeeds. Also used as a recovery id to identify
   * the right recovery if any of the abandoned recoveries re-appear.
   */
  // 恢复 ID，本质上是预备给该块恢复成功后的新版本号（Generation Stamp）。
  // 它也作为事务 ID，防止旧的、过时的恢复请求干扰当前流程。
  private long blockRecoveryId = 0;

  /**
   * The block source to use in the event of copy-on-write truncate.
   */
  // 用于 truncate（截断）操作。当发生写时拷贝（Copy-on-write）截断时，指向原始的块元数据。
  private BlockInfo truncateBlock;

  public BlockUnderConstructionFeature(Block blk,
      BlockUCState state, DatanodeStorageInfo[] targets, BlockType blockType) {
    assert getBlockUCState() != COMPLETE :
        "BlockUnderConstructionFeature cannot be in COMPLETE state";
    this.blockUCState = state;
    setExpectedLocations(blk, targets, blockType);
  }

  /** Set expected locations */
  // 根据 NameNode 选择的目标节点（Targets），创建 ReplicaUnderConstruction 数组。
  public void setExpectedLocations(Block block, DatanodeStorageInfo[] targets,
      BlockType blockType) {
    if (targets == null) {
      return;
    }
    int numLocations = 0;
    for (DatanodeStorageInfo target : targets) {
      if (target != null) {
        numLocations++;
      }
    }

    this.replicas = new ReplicaUnderConstruction[numLocations];
    int offset = 0;
    for(int i = 0; i < targets.length; i++) {
      if (targets[i] != null) {
        // when creating a new striped block we simply sequentially assign block
        // index to each storage
        // 特殊处理：如果是纠删码块（STRIPED），会根据偏移量为每个内部子块分配连续的 Block ID。
        Block replicaBlock = blockType == BlockType.STRIPED ?
            new Block(block.getBlockId() + i, 0, block.getGenerationStamp()) :
            block;
        replicas[offset++] = new ReplicaUnderConstruction(replicaBlock,
            targets[i], ReplicaState.RBW);
      }
    }
  }

  /**
   * Create array of expected replica locations
   * (as has been assigned by chooseTargets()).
   */
  // 返回所有预期的 DataNode 存储位置。
  public DatanodeStorageInfo[] getExpectedStorageLocations() {
    int numLocations = getNumExpectedLocations();
    DatanodeStorageInfo[] storages = new DatanodeStorageInfo[numLocations];
    for (int i = 0; i < numLocations; i++) {
      storages[i] = replicas[i].getExpectedStorageLocation();
    }
    return storages;
  }

  /**
   * Note that this iterator doesn't guarantee thread-safe. It depends on
   * external mechanisms such as the FSNamesystem lock for protection.
   */
  // 返回迭代器，方便遍历存储位置。
  public Iterator<DatanodeStorageInfo> getExpectedStorageLocationsIterator() {
    return new Iterator<DatanodeStorageInfo>() {
      private int index = 0;

      @Override
      public boolean hasNext() {
        return index <  replicas.length;
      }

      @Override
      public DatanodeStorageInfo next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return replicas[index++].getExpectedStorageLocation();
      }
    };
  }

  /**
   * @return the index array indicating the block index in each storage. Used
   * only by striped blocks.
   */
  // 专用于纠删码模式，返回每个副本在 EC 组中的索引（Index）
  public byte[] getBlockIndices() {
    int numLocations = getNumExpectedLocations();
    byte[] indices = new byte[numLocations];
    for (int i = 0; i < numLocations; i++) {
      indices[i] = BlockIdManager.getBlockIndex(replicas[i]);
    }
    return indices;
  }

  public byte[] getBlockIndicesForSpecifiedStorages(List<Integer> storageIdx) {
    byte[] indices = new byte[storageIdx.size()];
    for (int i = 0; i < indices.length; i++) {
      indices[i] = BlockIdManager.getBlockIndex(replicas[storageIdx.get(i)]);
    }
    return indices;
  }

  public int getNumExpectedLocations() {
    return replicas.length;
  }

  /**
   * when committing a striped block whose size is less than a stripe, we need
   * to decrease the scheduled block size of the DataNodes that do not store
   * any internal block.
   */
  // 纠删码专用。
  // 如果实际写入的条带小于预期，则减少相关 DataNode 的预订空间计数。
  void updateStorageScheduledSize(BlockInfoStriped storedBlock) {
    assert storedBlock.getUnderConstructionFeature() == this;
    if (replicas.length == 0) {
      return;
    }
    final int dataBlockNum = storedBlock.getDataBlockNum();
    final int realDataBlockNum = storedBlock.getRealDataBlockNum();
    if (realDataBlockNum < dataBlockNum) {
      for (ReplicaUnderConstruction replica : replicas) {
        int index = BlockIdManager.getBlockIndex(replica);
        if (index >= realDataBlockNum && index < dataBlockNum) {
          final DatanodeStorageInfo storage =
              replica.getExpectedStorageLocation();
          storage.getDatanodeDescriptor()
              .decrementBlocksScheduled(storage.getStorageType());
        }
      }
    }
  }

  /**
   * Return the state of the block under construction.
   * @see BlockUCState
   */
  public BlockUCState getBlockUCState() {
    return blockUCState;
  }
  // 手动设置构建状态。
  void setBlockUCState(BlockUCState s) {
    blockUCState = s;
  }

  public long getBlockRecoveryId() {
    return blockRecoveryId;
  }

  /** Get recover block */
  public BlockInfo getTruncateBlock() {
    return truncateBlock;
  }

  public void setTruncateBlock(BlockInfo recoveryBlock) {
    this.truncateBlock = recoveryBlock;
  }

  /**
   * Set {@link #blockUCState} to {@link BlockUCState#COMMITTED}.
   */
  // 将状态置为 COMMITTED。这意味着数据已写完，等待 DataNode 最后的确认汇报。
  void commit() {
    blockUCState = BlockUCState.COMMITTED;
  }

  List<ReplicaUnderConstruction> getStaleReplicas(long genStamp) {
    List<ReplicaUnderConstruction> staleReplicas = new ArrayList<>();
    // Remove replicas with wrong gen stamp. The replica list is unchanged.
    for (ReplicaUnderConstruction r : replicas) {
      if (genStamp != r.getGenerationStamp()) {
        staleReplicas.add(r);
      }
    }
    return staleReplicas;
  }

  /**
   * Initialize lease recovery for this block.
   * Find the first alive data-node starting from the previous primary and
   * make it primary.
   * @param blockInfo Block to be recovered
   * @param recoveryId Recovery ID (new gen stamp)
   * @param startRecovery Issue recovery command to datanode if true.
   */
  // 最重要的恢复方法。
  // HDFS 租约恢复（Lease Recovery）机制的核心入口。
  // 当客户端写入文件意外中断时，NameNode 会调用此方法来选出一个“主 DataNode”负责协调各副本的数据一致性。
  public void initializeBlockRecovery(BlockInfo blockInfo, long recoveryId,
      boolean startRecovery) {
    // 将该数据块的状态标记为“正在恢复”。
    setBlockUCState(BlockUCState.UNDER_RECOVERY);
    // blockRecoveryId 通常是 NameNode 生成的一个新的时间戳（Generation Stamp）。
    // 它有两个作用：一是作为恢复过程的唯一版本标识，二是作为该块恢复成功后的最终版本号。
    blockRecoveryId = recoveryId;
    // 控制开关。如果参数为 false，仅更新状态而不执行实际的选主和指令下发。
    if (!startRecovery) {
      return;
    }
    // 安全检查。如果 NameNode 发现没有任何 DataNode 持有这个块的副本，则无法恢复。
    if (replicas.length == 0) {
      NameNode.blockStateChangeLog.warn("BLOCK*" +
          " BlockUnderConstructionFeature.initializeBlockRecovery:" +
          " No blocks found, lease removed.");
      // sets primary node index and return.
      primaryNodeIndex = -1;
      return;
    }
    boolean allLiveReplicasTriedAsPrimary = true;
    // 检查是否所有存活的节点都已经被尝试作为“主节点”却都失败了。
    for (ReplicaUnderConstruction replica : replicas) {
      // Check if all replicas have been tried or not.
      if (replica.isAlive()) {
        allLiveReplicasTriedAsPrimary = allLiveReplicasTriedAsPrimary
            && replica.getChosenAsPrimary();
      }
    }
    // 如果上一轮全试过了都没成功，就将所有副本的 chosenAsPrimary 重置为 false，开启新一轮的尝试。
    if (allLiveReplicasTriedAsPrimary) {
      // Just set all the replicas to be chosen whether they are alive or not.
      for (ReplicaUnderConstruction replica : replicas) {
        replica.setChosenAsPrimary(false);
      }
    }
    long mostRecentLastUpdate = 0;
    ReplicaUnderConstruction primary = null;
    primaryNodeIndex = -1;
    for (int i = 0; i < replicas.length; i++) {
      // Skip alive replicas which have been chosen for recovery.
      // 过滤掉不存活的，或者在本轮已经尝试过担任 Primary 的节点
      if (!(replicas[i].isAlive() && !replicas[i].getChosenAsPrimary())) {
        continue;
      }
      final ReplicaUnderConstruction ruc = replicas[i];
      // 获取 DataNode 最后一次心跳的单调时间
      final long lastUpdate = ruc.getExpectedStorageLocation()
          .getDatanodeDescriptor().getLastUpdateMonotonic();
      // 寻找最近最活跃的节点
      if (lastUpdate > mostRecentLastUpdate) {
        primaryNodeIndex = i;
        primary = ruc;
        mostRecentLastUpdate = lastUpdate;
      }
    }
    if (primary != null) {
      // 将重构任务加入选定 DataNode 的待处理队列
      primary.getExpectedStorageLocation().getDatanodeDescriptor()
          .addBlockToBeRecovered(blockInfo);
      // 标记该节点在本轮已被选中
      primary.setChosenAsPrimary(true);
      NameNode.blockStateChangeLog.debug(
          "BLOCK* {} recovery started, primary={}", this, primary);
    }
  }

  /** Add the reported replica if it is not already in the replica list. */
  // 主要作用是根据 DataNode 的最新汇报，动态更新正在构建中的数据块（UC Block）的副本列表。
  // 由于 HDFS 在写入过程中可能会发生存储介质切换或节点位置更新，这个方法确保了 NameNode 掌握的副本元数据与物理实际保持一致。
  void addReplicaIfNotPresent(DatanodeStorageInfo storage,
      Block reportedBlock, ReplicaState rState) {
    if (replicas.length == 0) {
      // 如果当前该块没有任何已记录的副本，则直接初始化副本数组。
      replicas = new ReplicaUnderConstruction[1];
      replicas[0] = new ReplicaUnderConstruction(reportedBlock, storage,
          rState);
    } else {
      for (int i = 0; i < replicas.length; i++) {
        DatanodeStorageInfo expected =
            replicas[i].getExpectedStorageLocation();
        if (expected == storage) {
          // 情况 A：存储位置完全匹配
          // 此时仅更新该副本的 GenerationStamp（版本时间戳），以确保元数据中的版本号是最新的，然后直接返回。
          replicas[i].setGenerationStamp(reportedBlock.getGenerationStamp());
          return;
        } else if (expected != null && expected.getDatanodeDescriptor() ==
            storage.getDatanodeDescriptor()) {
          // DataNode 汇报的块在不同的存储上（例如从 DISK 换到了 SSD）
          // 如果虽然存储 ID 不同，但所属的 DataNode 描述符 是一致的，说明 DataNode 自行决定将块放在了该节点内的另一个磁盘上。
          // The Datanode reported that the block is on a different storage
          // than the one chosen by BlockPlacementPolicy. This can occur as
          // we allow Datanodes to choose the target storage. Update our
          // state by removing the stale entry and adding a new one.
          // 废弃旧的存储记录，用新的存储信息创建一个新的 ReplicaUnderConstruction 对象替换原位置。
          replicas[i] = new ReplicaUnderConstruction(reportedBlock, storage,
              rState);
          return;
        }
      }
      // 如果遍历完整个循环都没有找到匹配的节点或存储，说明这是一个 NameNode 此前未记录的新副本：
      ReplicaUnderConstruction[] newReplicas =
          new ReplicaUnderConstruction[replicas.length + 1];
      System.arraycopy(replicas, 0, newReplicas, 0, replicas.length);
      newReplicas[newReplicas.length - 1] = new ReplicaUnderConstruction(
          reportedBlock, storage, rState);
      replicas = newReplicas;
    }
  }

  @Override
  public String toString() {
    final StringBuilder b = new StringBuilder(100);
    appendUCParts(b);
    return b.toString();
  }

  private void appendUCParts(StringBuilder sb) {
    sb.append("{UCState=").append(blockUCState)
      .append(", truncateBlock=").append(truncateBlock)
      .append(", primaryNodeIndex=").append(primaryNodeIndex)
      .append(", replicas=[");
    int i = 0;
    for (ReplicaUnderConstruction r : replicas) {
      r.appendStringTo(sb);
      if (++i < replicas.length) {
        sb.append(", ");
      }
    }
    sb.append("]}");
  }
  
  public void appendUCPartsConcise(StringBuilder sb) {
    sb.append("replicas=");
    int i = 0;
    for (ReplicaUnderConstruction r : replicas) {
      sb.append(r.getExpectedStorageLocation().getDatanodeDescriptor());
      if (++i < replicas.length) {
        sb.append(", ");
      }
    }
  }
}
