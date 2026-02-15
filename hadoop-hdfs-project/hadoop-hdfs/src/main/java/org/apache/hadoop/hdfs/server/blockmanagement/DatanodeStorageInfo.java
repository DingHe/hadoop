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
//代表DataNode上一个存储（storage)，一个DataNode可以定义多个存储（dfs.datanode.data.dir）定义多个目录。
//存储是可以异构的
public class DatanodeStorageInfo {
  public static final DatanodeStorageInfo[] EMPTY_ARRAY = {};//一个空的 DatanodeStorageInfo 数组常量，避免创建多次空数组
 //将 DatanodeStorageInfo 数组转换为对应的 DatanodeInfo 数组，主要用于在集群中传递节点信息
  public static DatanodeInfo[] toDatanodeInfos(
      DatanodeStorageInfo[] storages) {
    return storages == null ? null: toDatanodeInfos(Arrays.asList(storages));
  }
  static DatanodeInfo[] toDatanodeInfos(List<DatanodeStorageInfo> storages) {
    final DatanodeInfo[] datanodes = new DatanodeInfo[storages.size()];
    for(int i = 0; i < storages.size(); i++) {
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
  //提取每个 DatanodeStorageInfo 对象的 storageID 并返回
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
  //提取DatanodeStorageInfo对象中的StorageType信息
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
 //从传入的 DatanodeStorage 对象中更新当前存储的状态和存储类型
  public void updateFromStorage(DatanodeStorage storage) {
    state = storage.getState();
    storageType = storage.getStorageType();
  }

  /**块信息迭代器
   * Iterates over the list of blocks belonging to the data-node.
   */
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

  private final DatanodeDescriptor dn;//关联的 DatanodeDescriptor 对象，表示此存储所属的数据节点
  private final String storageID;//存储的唯一标识符，由 DatanodeStorage 提供，区分同一数据节点的不同存储
  private StorageType storageType;//存储类型，使用 StorageType 枚举（如 DISK、SSD、ARCHIVE 等），表示存储介质类型
  private State state;//存储的状态，使用 DatanodeStorage.State 枚举，表示当前存储是否正常（NORMAL）、不可用（FAILED）等

  private long capacity;//存储的总容量（字节数）
  private long dfsUsed;//存储上被 HDFS 使用的空间
  private long nonDfsUsed;//存储上被非 HDFS 使用的空间（如系统文件或其他应用占用）
  private volatile long remaining;//存储的剩余可用空间
  private long blockPoolUsed;//存储上用于保存块池（Block Pool）的空间

  private volatile BlockInfo blockList = null;//该存储上管理的块链表头，类型为 BlockInfo，实现块的管理和遍历
  private int numBlocks = 0;//当前存储上的块数量

  /** The number of block reports received */
  private int blockReportCount = 0;//数据节点向 NameNode 发送的块报告数量

  /** Whether the NameNode has received block reports for this storage since it
   * was started.*/
  private boolean hasReceivedBlockReport = false;//标识是否自 NameNode 启动以来接收了至少一次块报告

  /**
   * Set to false on any NN failover, and reset to true
   * whenever a block report is received.
   */
  private boolean heartbeatedSinceFailover = false;//标识自 NameNode 故障切换（Failover）以来是否收到心跳，防止使用过时数据

  /**
   * At startup or at failover, the storages in the cluster may have pending
   * block deletions from a previous incarnation of the NameNode. The block
   * contents are considered as stale until a block report is received. When a
   * storage is considered as stale, the replicas on it are also considered as
   * stale. If any block has at least one stale replica, then no invalidations
   * will be processed for this block. See HDFS-1972.
   */
  private boolean blockContentsStale = true;//指示该存储上的块内容是否处于“陈旧”状态，通常在 NameNode 重启或切换时设置为 true
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
  //将一个 BlockInfo 对象添加到当前 DatanodeStorageInfo 对象中，表示将一个数据块与特定的 DataNode 存储单元关联起来
  public AddBlockResult addBlock(BlockInfo b, Block reportedBlock) {
    // First check whether the block belongs to a different storage
    // on the same DN.
    AddBlockResult result = AddBlockResult.ADDED;
    DatanodeStorageInfo otherStorage =
        b.findStorageInfo(getDatanodeDescriptor());//查数据块是否已存在于同一个 DataNode 的其他存储单元上

    if (otherStorage != null) {
      if (otherStorage != this) {
        // The block belongs to a different storage. Remove it first.
        otherStorage.removeBlock(b);
        result = AddBlockResult.REPLACED;
      } else {
        // The block is already associated with this storage.
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
