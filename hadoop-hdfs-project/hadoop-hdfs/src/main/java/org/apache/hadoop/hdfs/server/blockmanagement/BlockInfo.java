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

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.util.LightWeightGSet;

import static org.apache.hadoop.hdfs.server.namenode.INodeId.INVALID_INODE_ID;

/**
 * For a given block (or an erasure coding block group), BlockInfo class
 * maintains 1) the {@link BlockCollection} it is part of, and 2) datanodes
 * where the replicas of the block, or blocks belonging to the erasure coding
 * block group, are stored.
 */
@InterfaceAudience.Private
public abstract class BlockInfo extends Block
    implements LightWeightGSet.LinkedElement {
  //定义一个空的 BlockInfo 数组，通常用于返回空结果时使用，避免创建新的对象，提升效率
  public static final BlockInfo[] EMPTY_ARRAY = {};

  /**表示该块的副本数量，即该块会在多少个 DataNode 上存储副本
   * Replication factor.
   */
  private short replication;

  /**指向与该块关联的文件（BlockCollection，通常是 INodeFile）的唯一标识
   * Block collection ID.
   */
  private volatile long bcId;
  //这是为了实现 LightWeightGSet.LinkedElement 接口，支持将 BlockInfo 对象放入 LightWeightGSet 集合，方便在 Namenode 中对 BlockInfo 进行高效管理
  /** For implementing {@link LightWeightGSet.LinkedElement} interface. */
  private LightWeightGSet.LinkedElement nextLinkedElement;

  /**
   * This array contains triplets of references. For each i-th storage, the
   * block belongs to triplets[3*i] is the reference to the
   * {@link DatanodeStorageInfo} and triplets[3*i+1] and triplets[3*i+2] are
   * references to the previous and the next blocks, respectively, in the list
   * of blocks belonging to this storage.
   *
   * Using previous and next in Object triplets is done instead of a
   * {@link LinkedList} list to efficiently use memory. With LinkedList the cost
   * per replica is 42 bytes (LinkedList#Entry object per replica) versus 16
   * bytes using the triplets.
   */
  //存储与该块相关的 DataNode 及其链表信息，结构为三元组
  //triplets[3 * i]：对应 DataNode 存储信息（DatanodeStorageInfo）
  //triplets[3 * i + 1]：该块在该 DataNode 上的前一个块（BlockInfo）
  //triplets[3 * i + 2]：该块在该 DataNode 上的下一个块（BlockInfo）
  protected Object[] triplets;
  //记录该块在构建过程中的信息（如复制目标、状态等）。如果该块已经完成（COMPLETE 状态），此字段为 null
  private BlockUnderConstructionFeature uc;

  /**
   * Construct an entry for blocksmap
   * @param size the block's replication factor, or the total number of blocks
   *             in the block group  short size：副本数量（副本因子
   */
  public BlockInfo(short size) {
    this.triplets = new Object[3 * size];//创建用于存储 DataNode 信息的 triplets 数组，长度为 3 * size，每个 DataNode 需要 3 个位置
    this.bcId = INVALID_INODE_ID; //初始化 bcId 为无效值 INVALID_INODE_ID，表示该块未关联任何文件
    this.replication = isStriped() ? 0 : size;//如果块是条带化存储（Erasure Coding，isStriped() 为 true），replication 设置为 0，否则设置为 size
  }

  public BlockInfo(Block blk, short size) {
    super(blk);
    this.triplets = new Object[3 * size];
    this.bcId = INVALID_INODE_ID;
    this.replication = isStriped() ? 0 : size;
  }

  public short getReplication() {
    return replication;
  }

  public void setReplication(short repl) {
    this.replication = repl;
  }

  public long getBlockCollectionId() {
    return bcId;
  }

  public void setBlockCollectionId(long id) {
    this.bcId = id;
  }
  //删除块与文件的关联
  public void delete() {
    setBlockCollectionId(INVALID_INODE_ID);
  }

  public boolean isDeleted() {
    return bcId == INVALID_INODE_ID;
  }

  public Iterator<DatanodeStorageInfo> getStorageInfos() {
    return new BlocksMap.StorageIterator(this);
  }
  //获取Datanode的描述信息
  public DatanodeDescriptor getDatanode(int index) {
    DatanodeStorageInfo storage = getStorageInfo(index);
    return storage == null ? null : storage.getDatanodeDescriptor();
  }
  //获取指定索引的存储信息
  DatanodeStorageInfo getStorageInfo(int index) {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert index >= 0 && index * 3 < triplets.length : "Index is out of bound";
    return (DatanodeStorageInfo)triplets[index * 3];
  }
  //获取前一个BlockInfo节点
  BlockInfo getPrevious(int index) {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert index >= 0 && index * 3 + 1 < triplets.length : "Index is out of bound";
    BlockInfo info = (BlockInfo)triplets[index * 3 + 1];
    assert info == null ||
        info.getClass().getName().startsWith(BlockInfo.class.getName()) :
        "BlockInfo is expected at " + (index * 3 + 1);
    return info;
  }
  //获取下一个BlockInfo节点
  BlockInfo getNext(int index) {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert index >= 0 && index * 3 + 2 < triplets.length : "Index is out of bound";
    BlockInfo info = (BlockInfo)triplets[index * 3 + 2];
    assert info == null || info.getClass().getName().startsWith(
        BlockInfo.class.getName()) :
        "BlockInfo is expected at " + (index * 3 + 2);
    return info;
  }
  //设置指定索引的存储信息
  void setStorageInfo(int index, DatanodeStorageInfo storage) {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert index >= 0 && index * 3 < triplets.length : "Index is out of bound";
    triplets[index * 3] = storage;
  }

  /**
   * Return the previous block on the block list for the datanode at
   * position index. Set the previous block on the list to "to".
   * 设置前一个BlockInfo节点
   * @param index - the datanode index
   * @param to - block to be set to previous on the list of blocks
   * @return current previous block on the list of blocks
   */
  BlockInfo setPrevious(int index, BlockInfo to) {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert index >= 0 && index * 3 + 1 < triplets.length : "Index is out of bound";
    BlockInfo info = (BlockInfo) triplets[index * 3 + 1];
    triplets[index * 3 + 1] = to;
    return info;
  }

  /**
   * Return the next block on the block list for the datanode at
   * position index. Set the next block on the list to "to".
   * 设置下一个BlockInfo节点
   * @param index - the datanode index
   * @param to - block to be set to next on the list of blocks
   * @return current next block on the list of blocks
   */
  BlockInfo setNext(int index, BlockInfo to) {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert index >= 0 && index * 3 + 2 < triplets.length : "Index is out of bound";
    BlockInfo info = (BlockInfo) triplets[index * 3 + 2];
    triplets[index * 3 + 2] = to;
    return info;
  }
  //返回块当前可以存储的副本数（triplets 数组长度除以 3）
  public int getCapacity() {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert triplets.length % 3 == 0 : "Malformed BlockInfo";
    return triplets.length / 3;
  }

  /** 获取block在多少个datanode上
   * Count the number of data-nodes the block currently belongs to (i.e., NN
   * has received block reports from the DN).
   */
  public abstract int numNodes();

  /**增加block的存储位置
   * Add a {@link DatanodeStorageInfo} location for a block
   * @param storage The storage to add
   * @param reportedBlock The block reported from the datanode. This is only
   *                      used by erasure coded blocks, this block's id contains
   *                      information indicating the index of the block in the
   *                      corresponding block group.
   */
  abstract boolean addStorage(DatanodeStorageInfo storage, Block reportedBlock);

  /**
   * Remove {@link DatanodeStorageInfo} location for a block
   */
  abstract boolean removeStorage(DatanodeStorageInfo storage);

  public abstract boolean isStriped();
  //返回块的类型
  public abstract BlockType getBlockType();

  /** @return true if there is no datanode storage associated with the block */
  abstract boolean hasNoStorage();

  /**
   * Checks whether this block has a Provided replica.
   * @return true if this block has a replica on Provided storage.
   */
  abstract boolean isProvided();

  /** 用于在当前 BlockInfo 对象的副本列表中查找与指定 DatanodeDescriptor 对应的 DatanodeStorageInfo
   * Find specified DatanodeStorageInfo.
   * @return DatanodeStorageInfo or null if not found.
   */
  DatanodeStorageInfo findStorageInfo(DatanodeDescriptor dn) {
    int len = getCapacity(); //获取当前块的副本容量
    DatanodeStorageInfo providedStorageInfo = null;
    for(int idx = 0; idx < len; idx++) {
      DatanodeStorageInfo cur = getStorageInfo(idx);
      if(cur != null) {
        if (cur.getStorageType() == StorageType.PROVIDED) { //如果存在 PROVIDED 类型的存储（外部存储，如云存储或远程存储），临时保存该存储信息
          // if block resides on provided storage, only match the storage ids
          if (dn.getStorageInfo(cur.getStorageID()) != null) { //查目标 DataNode 是否包含相同的存储 ID
            // do not return here as we have to check the other
            // DatanodeStorageInfos for this block which could be local
            providedStorageInfo = cur;
          }
        } else if (cur.getDatanodeDescriptor() == dn) { //如果存储不是 PROVIDED 类型，直接检查 DatanodeDescriptor 是否与传入的 dn 对象相同。
          return cur;
        }
      }
    }
    return providedStorageInfo;
  }

  /**
   * Find specified DatanodeStorageInfo.
   * @return index or -1 if not found.
   */
  int findStorageInfo(DatanodeStorageInfo storageInfo) {
    int len = getCapacity();
    for(int idx = 0; idx < len; idx++) {
      DatanodeStorageInfo cur = getStorageInfo(idx);
      if (cur == storageInfo) {
        return idx;
      }
    }
    return -1;
  }

  /**
   * Insert this block into the head of the list of blocks
   * related to the specified DatanodeStorageInfo.
   * If the head is null then form a new list.
   * @return current block as the new head of the list.
   */
  BlockInfo listInsert(BlockInfo head, DatanodeStorageInfo storage) {
    int dnIndex = this.findStorageInfo(storage);
    assert dnIndex >= 0 : "Data node is not found: current";
    assert getPrevious(dnIndex) == null && getNext(dnIndex) == null :
        "Block is already in the list and cannot be inserted.";
    this.setPrevious(dnIndex, null);
    this.setNext(dnIndex, head);
    if (head != null) {
      head.setPrevious(head.findStorageInfo(storage), this);
    }
    return this;
  }

  /**
   * Remove this block from the list of blocks
   * related to the specified DatanodeStorageInfo.
   * If this block is the head of the list then return the next block as
   * the new head.
   * @return the new head of the list or null if the list becomes
   * empy after deletion.
   */
  BlockInfo listRemove(BlockInfo head, DatanodeStorageInfo storage) {
    if (head == null) {
      return null;
    }
    int dnIndex = this.findStorageInfo(storage);
    if (dnIndex < 0) { // this block is not on the data-node list
      return head;
    }

    BlockInfo next = this.getNext(dnIndex);
    BlockInfo prev = this.getPrevious(dnIndex);
    this.setNext(dnIndex, null);
    this.setPrevious(dnIndex, null);
    if (prev != null) {
      prev.setNext(prev.findStorageInfo(storage), next);
    }
    if (next != null) {
      next.setPrevious(next.findStorageInfo(storage), prev);
    }
    if (this == head) { // removing the head
      head = next;
    }
    return head;
  }

  /**
   * Remove this block from the list of blocks related to the specified
   * DatanodeDescriptor. Insert it into the head of the list of blocks.
   *
   * @return the new head of the list.
   */
  public BlockInfo moveBlockToHead(BlockInfo head, DatanodeStorageInfo storage,
      int curIndex, int headIndex) {
    if (head == this) {
      return this;
    }
    BlockInfo next = this.setNext(curIndex, head);
    BlockInfo prev = this.setPrevious(curIndex, null);

    head.setPrevious(headIndex, this);
    prev.setNext(prev.findStorageInfo(storage), next);
    if (next != null) {
      next.setPrevious(next.findStorageInfo(storage), prev);
    }
    return this;
  }

  @Override
  public int hashCode() {
    // Super implementation is sufficient
    return super.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    // Sufficient to rely on super's implementation
    return (this == obj) || super.equals(obj);
  }

  @Override
  public LightWeightGSet.LinkedElement getNext() {
    return nextLinkedElement;
  }

  @Override
  public void setNext(LightWeightGSet.LinkedElement next) {
    this.nextLinkedElement = next;
  }

  /* UnderConstruction Feature related */

  public BlockUnderConstructionFeature getUnderConstructionFeature() {
    return uc;
  }

  public BlockUCState getBlockUCState() {
    return uc == null ? BlockUCState.COMPLETE : uc.getBlockUCState();
  }

  /**
   * Is this block complete?
   *
   * @return true if the state of the block is {@link BlockUCState#COMPLETE}
   */
  public boolean isComplete() {
    return getBlockUCState().equals(BlockUCState.COMPLETE);
  }

  public boolean isUnderRecovery() {
    return getBlockUCState().equals(BlockUCState.UNDER_RECOVERY);
  }

  public final boolean isCompleteOrCommitted() {
    final BlockUCState state = getBlockUCState();
    return state.equals(BlockUCState.COMPLETE) ||
        state.equals(BlockUCState.COMMITTED);
  }

  /**
   * Add/Update the under construction feature.
   */
  public void convertToBlockUnderConstruction(BlockUCState s,
      DatanodeStorageInfo[] targets) {
    if (isComplete()) {
      uc = new BlockUnderConstructionFeature(this, s, targets,
          this.getBlockType());
    } else {
      // the block is already under construction
      uc.setBlockUCState(s);
      uc.setExpectedLocations(this, targets, this.getBlockType());
    }
  }

  /**
   * Convert an under construction block to complete.
   */
  void convertToCompleteBlock() {
    assert getBlockUCState() != BlockUCState.COMPLETE :
        "Trying to convert a COMPLETE block";
    uc = null;
  }

  /**
   * Process the recorded replicas. When about to commit or finish the
   * pipeline recovery sort out bad replicas.
   * @param genStamp  The final generation stamp for the block.
   * @return staleReplica's List.
   */
  public List<ReplicaUnderConstruction> setGenerationStampAndVerifyReplicas(
      long genStamp) {
    Preconditions.checkState(uc != null && !isComplete());
    // Set the generation stamp for the block.
    setGenerationStamp(genStamp);

    return uc.getStaleReplicas(genStamp);
  }

  /**
   * Commit block's length and generation stamp as reported by the client.
   * Set block state to {@link BlockUCState#COMMITTED}.
   * @param block - contains client reported block length and generation
   * @return staleReplica's List.
   * @throws IOException if block ids are inconsistent.
   */
  List<ReplicaUnderConstruction> commitBlock(Block block) throws IOException {
    if (getBlockId() != block.getBlockId()) {
      throw new IOException("Trying to commit inconsistent block: id = "
          + block.getBlockId() + ", expected id = " + getBlockId());
    }
    Preconditions.checkState(!isComplete());
    uc.commit();
    this.setNumBytes(block.getNumBytes());
    // Sort out invalid replicas.
    return setGenerationStampAndVerifyReplicas(block.getGenerationStamp());
  }
}
