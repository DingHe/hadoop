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
// BlockInfo 的核心作用是在 NameNode 内存中维护块的元数据及其物理位置映射。
// 关联文件：它知道自己属于哪个文件（通过 bcId 关联到 INodeFile）。
// 管理位置：它记录了该块的所有副本分别存储在哪些 DataNode 的哪个存储空间（Storage）上。
// 高效链表结构：它不使用标准的 List 集合，而是通过一个特殊的 triplets 数组，将属于同一个 DataNode 上的所有块串联成双向链表。这种设计极大地节省了 NameNode 在管理数亿个块时的内存开销。
// 状态管理：它维护块的状态（如：正在写入、已完成、处于恢复状态等）。
@InterfaceAudience.Private
public abstract class BlockInfo extends Block
    implements LightWeightGSet.LinkedElement {
  // 定义一个空的 BlockInfo 数组，
  // 通常用于返回空结果时使用，避免创建新的对象，提升效率
  public static final BlockInfo[] EMPTY_ARRAY = {};

  /**
   * Replication factor.
   */
  // 副本因子。
  // 表示该块预期的副本数。如果是纠删码（EC）块，该值为 0。
  private short replication;

  /**
   * Block collection ID.
   */
  // BlockCollection ID。
  // 对应文件的 ID，用于快速定位该块属于哪个文件。
  private volatile long bcId;
  /** For implementing {@link LightWeightGSet.LinkedElement} interface. */
  // 用于 LightWeightGSet 的链表指针，使 BlockInfo 能直接存入 HDFS 特有的高效哈希表。
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
  //核心存储数组。长度为 $3 \times \text{副本数}$。每 3 个元素为一个单位：
  // DatanodeStorageInfo: 指向存储该副本的 DN 存储。
  // Previous BlockInfo: 该 DN 上链表的前一个块。
  // Next BlockInfo: 该 DN 上链表的下一个块。
  protected Object[] triplets;
  // 正在构建特征。
  // 记录处于写入中或恢复中状态的块的临时信息（如租约持有者、目标节点等）。
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
  // 获取或设置预期的副本数。
  public short getReplication() {
    return replication;
  }

  public void setReplication(short repl) {
    this.replication = repl;
  }
  // 关联或获取文件 ID。
  public long getBlockCollectionId() {
    return bcId;
  }

  public void setBlockCollectionId(long id) {
    this.bcId = id;
  }
  // 通过将 bcId 设置为无效来标记该块已被删除。
  public void delete() {
    setBlockCollectionId(INVALID_INODE_ID);
  }

  public boolean isDeleted() {
    return bcId == INVALID_INODE_ID;
  }

  public Iterator<DatanodeStorageInfo> getStorageInfos() {
    return new BlocksMap.StorageIterator(this);
  }
  // 获取第 index 个副本所在的 DatanodeDescriptor（数据节点描述符）。
  public DatanodeDescriptor getDatanode(int index) {
    DatanodeStorageInfo storage = getStorageInfo(index);
    return storage == null ? null : storage.getDatanodeDescriptor();
  }
  // 获取第 index 个副本所在的 DatanodeStorageInfo。
  DatanodeStorageInfo getStorageInfo(int index) {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert index >= 0 && index * 3 < triplets.length : "Index is out of bound";
    return (DatanodeStorageInfo)triplets[index * 3];
  }
  // 在第 index 个副本所在的 DataNode 上，获取该块的前一个或下一个块。
  BlockInfo getPrevious(int index) {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert index >= 0 && index * 3 + 1 < triplets.length : "Index is out of bound";
    BlockInfo info = (BlockInfo)triplets[index * 3 + 1];
    assert info == null ||
        info.getClass().getName().startsWith(BlockInfo.class.getName()) :
        "BlockInfo is expected at " + (index * 3 + 1);
    return info;
  }
  // 在第 index 个副本所在的 DataNode 上，获取该块的前一个或下一个块。
  BlockInfo getNext(int index) {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert index >= 0 && index * 3 + 2 < triplets.length : "Index is out of bound";
    BlockInfo info = (BlockInfo)triplets[index * 3 + 2];
    assert info == null || info.getClass().getName().startsWith(
        BlockInfo.class.getName()) :
        "BlockInfo is expected at " + (index * 3 + 2);
    return info;
  }
  // 设置存储的位置信息
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
  // 返回块当前可以存储的副本数（triplets 数组长度除以 3）
  public int getCapacity() {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert triplets.length % 3 == 0 : "Malformed BlockInfo";
    return triplets.length / 3;
  }

  /**
   * Count the number of data-nodes the block currently belongs to (i.e., NN
   * has received block reports from the DN).
   */
  // 获取block在多少个datanode上
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

  /**
   * Find specified DatanodeStorageInfo.
   * @return DatanodeStorageInfo or null if not found.
   */
  // 查找该块是否在指定的 DataNode 上有副本，并返回对应的存储信息。
  // 用于在当前 BlockInfo 对象的副本列表中查找与指定 DatanodeDescriptor 对应的 DatanodeStorageInfo
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
  // 在 HDFS 中，NameNode 需要管理每个 DataNode 上存储的所有数据块。
  // 为了节省内存，它没有使用 ArrayList 或 HashSet，而是将属于同一个 DataNode 存储（DatanodeStorageInfo）的所有 BlockInfo 对象串联成一个自定义的双向链表。
  // listInsert 方法的作用就是将当前的 BlockInfo 对象插入到该 DataNode 维护的块链表的头部。
  BlockInfo listInsert(BlockInfo head, DatanodeStorageInfo storage) {
    // 找到该块在 triplets 数组中的起始位置索引（dnIndex）。接下来的前驱（Previous）和后继（Next）引用都将基于这个索引进行操作。
    int dnIndex = this.findStorageInfo(storage);
    assert dnIndex >= 0 : "Data node is not found: current";
    // 在插入之前，确保当前块在该存储对应的链表指针（前驱和后继）都是空的。
    // 防止将一个已经在链表中的块重复插入，避免造成链表死循环或逻辑混乱。
    assert getPrevious(dnIndex) == null && getNext(dnIndex) == null :
        "Block is already in the list and cannot be inserted.";
    // 因为是要插入到链表头部，所以当前节点的前驱必须设置为 null。
    this.setPrevious(dnIndex, null);
    // 将当前块的下一个节点指向原有的链表头（head）
    this.setNext(dnIndex, head);
    // 如果原链表不为空（即 head != null）
    if (head != null) {
      // 找到原头节点在同一个存储下的索引位。
      // 将原头节点的前驱指针指向当前块。
      head.setPrevious(head.findStorageInfo(storage), this);
    }
    // 返回新的头节点
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
  // 将当前的 BlockInfo 对象从特定的 DataNode 存储（DatanodeStorageInfo）所维护的双向块链表中移除。
  // 由于这是在一个手动实现的双向链表上进行操作，它必须小心地重新连接被移除节点的前后节点。
  BlockInfo listRemove(BlockInfo head, DatanodeStorageInfo storage) {
    // 如果传入的链表头指针为空，说明该存储下没有任何块，直接返回 null。
    if (head == null) {
      return null;
    }
    // 首先在当前块中找到对应 storage 的副本索引。
    int dnIndex = this.findStorageInfo(storage);
    if (dnIndex < 0) { // this block is not on the data-node list
      return head;
    }
    // 从 triplets 数组中取出当前块在该链表中的后继节点（next）和前驱节点（prev）。
    BlockInfo next = this.getNext(dnIndex);
    BlockInfo prev = this.getPrevious(dnIndex);
    // 切断当前节点的连接（清理指针）
    this.setNext(dnIndex, null);
    this.setPrevious(dnIndex, null);
    // 重新连接前驱节点的后继
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
  // 将一个已经在链表中的块（this）从当前位置移除，并重新插入到该链表的头部。
  // 这通常用于 MRU（Most Recently Used） 逻辑，或者在某些操作后为了快速访问而优化链表顺序。
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
  // 在 HDFS 中，一个数据块并不总是静态的。当一个文件正在被写入、追加（Append）或者因为某种异常需要恢复时，数据块会从“已完成”（Complete）状态转变为“构建中”（Under Construction）状态。
  // 为当前块添加或更新“构建中”特征（Feature），以便记录哪些 DataNode 应该是该块的存储目标。
  public void convertToBlockUnderConstruction(BlockUCState s,
      DatanodeStorageInfo[] targets) {
    // 检查当前块是否处于 COMPLETE 状态（即 uc 属性为 null）。
    if (isComplete()) {
      // 情况一：新创建一个构建特征（Complete -> UC）
      uc = new BlockUnderConstructionFeature(this, s, targets,
          this.getBlockType());
    } else {
      // the block is already under construction
      // 情况二：更新已有的构建特征（UC -> 新的 UC 状态）
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
