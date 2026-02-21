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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage.State;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * A Datanode has one or more storages. A storage in the Datanode is represented
 * by this class.
 */
// DatanodeStorageInfo 是 NameNode 内存中描述 DataNode 存储单元 的核心类。
// 在 HDFS 中，一个 DataNode 可以挂载多块硬盘或多种存储介质。DatanodeStorageInfo 的作用就是在 NameNode 端代表 DataNode 上的一个具体存储目录（Storage）。
// 其核心职责包括：
// 管理异构存储：记录存储介质类型（如 SSD, DISK, ARCHIVE），支持 HDFS 的分层存储策略。
// 维护块链表：通过一个 blockList 指针，将存放在该存储上的所有 BlockInfo 串联起来，方便遍历和管理。
// 统计存储状态：实时记录该存储的容量、已用空间、剩余空间等动态指标。
// 生命周期追踪：追踪块报告（Block Report）和心跳（Heartbeat）的状态，判断该存储上的数据是否“陈旧”。

public class DatanodeStorageInfo {
  //一个空的 DatanodeStorageInfo 数组常量，避免创建多次空数组
  public static final DatanodeStorageInfo[] EMPTY_ARRAY = {};
  // 核心作用是将 NameNode 内部管理的详细存储对象（DatanodeStorageInfo）简化并提取为面向客户端或网络传输的节点信息对象（DatanodeInfo）。
  public static DatanodeInfo[] toDatanodeInfos(
      DatanodeStorageInfo[] storages) {
    return storages == null ? null: toDatanodeInfos(Arrays.asList(storages));
  }
  static DatanodeInfo[] toDatanodeInfos(List<DatanodeStorageInfo> storages) {
    // 根据输入列表的大小，初始化一个等长度的 DatanodeInfo 数组。
    final DatanodeInfo[] datanodes = new DatanodeInfo[storages.size()];
    for(int i = 0; i < storages.size(); i++) {
      // DatanodeDescriptor 是 DatanodeInfo 的子类，因此可以直接赋值给数组。
      datanodes[i] = storages.get(i).getDatanodeDescriptor();
    }
    return datanodes;
  }

  static DatanodeDescriptor[] toDatanodeDescriptors(
      DatanodeStorageInfo[] storages) {
    DatanodeDescriptor[] datanodes = new DatanodeDescriptor[storages.length];
    for (int i = 0; i < storages.length; ++i) {
      datanodes[i] = storages[i].getDatanodeDescriptor();
    }
    return datanodes;
  }

  // 提取每个 DatanodeStorageInfo 对象的 storageID 并返回
  public static String[] toStorageIDs(DatanodeStorageInfo[] storages) {
    if (storages == null) {
      return null;
    }
    String[] storageIDs = new String[storages.length];
    for(int i = 0; i < storageIDs.length; i++) {
      storageIDs[i] = storages[i].getStorageID();
    }
    return storageIDs;
  }
  // 提取DatanodeStorageInfo对象中的StorageType信息
  public static StorageType[] toStorageTypes(DatanodeStorageInfo[] storages) {
    if (storages == null) {
      return null;
    }
    StorageType[] storageTypes = new StorageType[storages.length];
    for(int i = 0; i < storageTypes.length; i++) {
      storageTypes[i] = storages[i].getStorageType();
    }
    return storageTypes;
  }
  // 从传入的 DatanodeStorage 对象中更新当前存储的状态和存储类型
  public void updateFromStorage(DatanodeStorage storage) {
    state = storage.getState();
    storageType = storage.getStorageType();
  }

  /**
   * Iterates over the list of blocks belonging to the data-node.
   */
  // 块信息迭代器
  class BlockIterator implements Iterator<BlockInfo> {
    private BlockInfo current;

    BlockIterator(BlockInfo head) {
      this.current = head;
    }

    public boolean hasNext() {
      return current != null;
    }

    public BlockInfo next() {
      BlockInfo res = current;
      current =
          current.getNext(current.findStorageInfo(DatanodeStorageInfo.this));
      return res;
    }

    public void remove() {
      throw new UnsupportedOperationException("Sorry. can't remove.");
    }
  }
  // 指向该存储所属的 DataNode 节点对象。
  private final DatanodeDescriptor dn;
  // 该存储的全局唯一 ID。
  private final String storageID;
  // 存储介质类型（DISK/SSD/ARCHIVE/RAM_DISK/PROVIDED）。
  private StorageType storageType;
  // 存储状态（NORMAL 或 FAILED）。
  private State state;
  // 总容量（字节）。
  private long capacity;
  // HDFS 已使用的空间。
  private long dfsUsed;
  // 非 HDFS 占用的空间（如系统文件）。
  private long nonDfsUsed;
  // 剩余可用空间。
  private volatile long remaining;
  // 当前块池（Block Pool）使用的空间。
  private long blockPoolUsed;
  // 指向该存储上块链表的头部。
  private volatile BlockInfo blockList = null;
  // 该存储上当前存在的块总数。
  private int numBlocks = 0;

  /** The number of block reports received */
  //数据节点向 NameNode 发送的块报告数量
  private int blockReportCount = 0;

  /** Whether the NameNode has received block reports for this storage since it
   * was started.*/
  //标识是否自 NameNode 启动以来接收了至少一次块报告
  private boolean hasReceivedBlockReport = false;

  /**
   * Set to false on any NN failover, and reset to true
   * whenever a block report is received.
   */
  //标识自 NameNode 故障切换（Failover）以来是否收到心跳，防止使用过时数据
  private boolean heartbeatedSinceFailover = false;

  /**
   * At startup or at failover, the storages in the cluster may have pending
   * block deletions from a previous incarnation of the NameNode. The block
   * contents are considered as stale until a block report is received. When a
   * storage is considered as stale, the replicas on it are also considered as
   * stale. If any block has at least one stale replica, then no invalidations
   * will be processed for this block. See HDFS-1972.
   */
  // 块内容是否陈旧。
  // 在 NameNode 重启或切主时为 true，收到完整块报告后转为 false。
  private boolean blockContentsStale = true;
  //构造方法
  DatanodeStorageInfo(DatanodeDescriptor dn, DatanodeStorage s) {
    this(dn, s.getStorageID(), s.getStorageType(), s.getState());
  }

  DatanodeStorageInfo(DatanodeDescriptor dn, String storageID,
      StorageType storageType, State state) {
    this.dn = dn;
    this.storageID = storageID;
    this.storageType = storageType;
    this.state = state;
  }

  public int getBlockReportCount() {
    return blockReportCount;
  }

  boolean hasReceivedBlockReport() {
    return hasReceivedBlockReport;
  }

  void setBlockReportCount(int blockReportCount) {
    this.blockReportCount = blockReportCount;
  }

  public boolean areBlockContentsStale() {
    return blockContentsStale;
  }

  @VisibleForTesting
  public void setBlockContentsStale(boolean value) {
    blockContentsStale = value;
  }

  void markStaleAfterFailover() {
    heartbeatedSinceFailover = false;
    blockContentsStale = true;
  }

  void receivedHeartbeat(StorageReport report) {
    updateState(report);
    heartbeatedSinceFailover = true;
  }

  void receivedBlockReport() {
    if (heartbeatedSinceFailover) {
      blockContentsStale = false;
    }
    blockReportCount++;
    hasReceivedBlockReport = true;
  }

  @VisibleForTesting
  public void setUtilizationForTesting(long capacity, long dfsUsed,
                      long remaining, long blockPoolUsed) {
    this.capacity = capacity;
    this.dfsUsed = dfsUsed;
    this.remaining = remaining;
    this.blockPoolUsed = blockPoolUsed;
  }

  State getState() {
    return this.state;
  }

  void setState(State state) {
    this.state = state;
  }

  void setHeartbeatedSinceFailover(boolean value) {
    heartbeatedSinceFailover = value;
  }

  boolean areBlocksOnFailedStorage() {
    return getState() == State.FAILED && numBlocks != 0;
  }

  @VisibleForTesting
  public String getStorageID() {
    return storageID;
  }

  public StorageType getStorageType() {
    return storageType;
  }

  long getCapacity() {
    return capacity;
  }

  long getDfsUsed() {
    return dfsUsed;
  }

  long getNonDfsUsed() {
    return nonDfsUsed;
  }

  long getRemaining() {
    return remaining;
  }

  long getBlockPoolUsed() {
    return blockPoolUsed;
  }

  // addBlock 方法是 NameNode 更新块元数据的高频操作。
  // 它的核心任务是：建立或更新“数据块（Block）”与“物理存储单元（Storage）”之间的映射关系。
  public AddBlockResult addBlock(BlockInfo b, Block reportedBlock) {
    // First check whether the block belongs to a different storage
    // on the same DN.
    // 默认将操作结果设置为 ADDED（新增）。如果后续发现该块已存在于其他存储中，会修改此状态。
    AddBlockResult result = AddBlockResult.ADDED;
    // 在当前数据节点（DataNode）范围内搜索该块是否已经记录在某个存储上。
    // HDFS 规定同一个数据块在同一个 DataNode 上只能存在一个副本（即使该节点有多个硬盘）。这行代码是为了检查是否存在“跨硬盘”重复。
    DatanodeStorageInfo otherStorage =
        b.findStorageInfo(getDatanodeDescriptor());
    // 处理已存在的存储关系
    if (otherStorage != null) {
      if (otherStorage != this) {
        // The block belongs to a different storage. Remove it first.
        // 块属于不同的存储，先将其移除
        otherStorage.removeBlock(b);
        result = AddBlockResult.REPLACED;
      } else {
        // The block is already associated with this storage.
        // 块已经关联到当前存储
        return AddBlockResult.ALREADY_EXIST;
      }
    }

    // add to the head of the data-node list
    b.addStorage(this, reportedBlock);
    insertToList(b);
    return result;
  }

  AddBlockResult addBlock(BlockInfo b) {
    return addBlock(b, b);
  }

  public void insertToList(BlockInfo b) {
    blockList = b.listInsert(blockList, this);
    numBlocks++;
  }
  boolean removeBlock(BlockInfo b) {
    blockList = b.listRemove(blockList, this);
    if (b.removeStorage(this)) {
      numBlocks--;
      return true;
    } else {
      return false;
    }
  }

  int numBlocks() {
    return numBlocks;
  }

  Iterator<BlockInfo> getBlockIterator() {
    return new BlockIterator(blockList);
  }

  /**
   * Move block to the head of the list of blocks belonging to the data-node.
   * @return the index of the head of the blockList
   */
  int moveBlockToHead(BlockInfo b, int curIndex, int headIndex) {
    blockList = b.moveBlockToHead(blockList, this, curIndex, headIndex);
    return curIndex;
  }


  /**
   * Used for testing only.
   * @return the head of the blockList
   */
  @VisibleForTesting
  BlockInfo getBlockListHeadForTesting(){
    return blockList;
  }

  void updateState(StorageReport r) {
    capacity = r.getCapacity();
    dfsUsed = r.getDfsUsed();
    nonDfsUsed = r.getNonDfsUsed();
    remaining = r.getRemaining();
    blockPoolUsed = r.getBlockPoolUsed();
  }

  public DatanodeDescriptor getDatanodeDescriptor() {
    return dn;
  }

  /** Increment the number of blocks scheduled for each given storage */ 
  public static void incrementBlocksScheduled(DatanodeStorageInfo... storages) {
    for (DatanodeStorageInfo s : storages) {
      s.getDatanodeDescriptor().incrementBlocksScheduled(s.getStorageType());
    }
  }

  /**
   * Decrement the number of blocks scheduled for each given storage. This will
   * be called during abandon block or delete of UC block.
   */
  public static void decrementBlocksScheduled(DatanodeStorageInfo... storages) {
    for (DatanodeStorageInfo s : storages) {
      s.getDatanodeDescriptor().decrementBlocksScheduled(s.getStorageType());
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (!(obj instanceof DatanodeStorageInfo)) {
      return false;
    }
    final DatanodeStorageInfo that = (DatanodeStorageInfo)obj;
    return this.storageID.equals(that.storageID);
  }

  @Override
  public int hashCode() {
    return storageID.hashCode();
  }

  @Override
  public String toString() {
    return "[" + storageType + "]" + storageID + ":" + state + ":" + dn;
  }
  
  StorageReport toStorageReport() {
    return new StorageReport(
        new DatanodeStorage(storageID, state, storageType),
        false, capacity, dfsUsed, remaining, blockPoolUsed, nonDfsUsed);
  }

  static Iterable<StorageType> toStorageTypes(
      final Iterable<DatanodeStorageInfo> infos) {
    return new Iterable<StorageType>() {
        @Override
        public Iterator<StorageType> iterator() {
          return new Iterator<StorageType>() {
            final Iterator<DatanodeStorageInfo> i = infos.iterator();
            @Override
            public boolean hasNext() {return i.hasNext();}
            @Override
            public StorageType next() {return i.next().getStorageType();}
            @Override
            public void remove() {
              throw new UnsupportedOperationException();
            }
          };
        }
      };
  }

  /** @return the first {@link DatanodeStorageInfo} corresponding to
   *          the given datanode
   */
  //
  static DatanodeStorageInfo getDatanodeStorageInfo(
      final Iterable<DatanodeStorageInfo> infos,
      final DatanodeDescriptor datanode) {
    if (datanode == null) {
      return null;
    }
    for(DatanodeStorageInfo storage : infos) {
      if (storage.getDatanodeDescriptor() == datanode) {
        return storage;
      }
    }
    return null;
  }

  @VisibleForTesting
  void setRemainingForTests(int remaining) {
    this.remaining = remaining;
  }

  enum AddBlockResult {
    ADDED, REPLACED, ALREADY_EXIST
  }
}
