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

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockType;

/**
 * Subclass of {@link BlockInfo}, used for a block with replication scheme.
 */
// 主要用于管理 HDFS 副本机制下的数据块。
// 所谓的“连续（Contiguous）”，是指数据在逻辑上是连续存储的副本。该类的核心职责是：
// 副本位置跟踪：在 NameNode 内存中记录一个数据块的所有副本分别存储在哪些 DataNode 的哪些存储单元上。
// 内存优化存储：继承自 BlockInfo，使用一种被称为 “三元组（Triplets）” 的数组结构来保存存储信息，这种结构极大地节省了 NameNode 在管理数以亿计数据块时的内存开销。
// 状态维护：提供添加、删除、查询副本位置的方法，并判断数据块的类型（非条带化）。
@InterfaceAudience.Private
public class BlockInfoContiguous extends BlockInfo {

  public BlockInfoContiguous(short size) {
    super(size);
  }

  public BlockInfoContiguous(Block blk, short size) {
    super(blk, size);
  }

  /**
   * Ensure that there is enough  space to include num more triplets.
   * @return first free triplet index.
   */
  // 确保 triplets 数组有足够的空间再容纳 num 个新的副本记录。
  private int ensureCapacity(int num) {
    assert this.triplets != null : "BlockInfo is not initialized";
    int last = numNodes();
    if (triplets.length >= (last+num)*3) {
      return last;
    }
    /* Not enough space left. Create a new array. Should normally
     * happen only when replication is manually increased by the user. */
    Object[] old = triplets;
    triplets = new Object[(last+num)*3];
    System.arraycopy(old, 0, triplets, 0, last * 3);
    return last;
  }
  // 当 DataNode 上报自己拥有该块的副本时，将该位置信息存入内存。
  @Override
  boolean addStorage(DatanodeStorageInfo storage, Block reportedBlock) {
    // 确保上报的块 ID 与当前对象一致。
    Preconditions.checkArgument(this.getBlockId() == reportedBlock.getBlockId(),
        "reported blk_%s is different from stored blk_%s",
        reportedBlock.getBlockId(), this.getBlockId());
    // find the last null node
    int lastNode = ensureCapacity(1);
    setStorageInfo(lastNode, storage);
    setNext(lastNode, null);
    setPrevious(lastNode, null);
    return true;
  }

  @Override
  boolean removeStorage(DatanodeStorageInfo storage) {
    int dnIndex = findStorageInfo(storage);
    if (dnIndex < 0) { // the node is not found
      return false;
    }
    assert getPrevious(dnIndex) == null && getNext(dnIndex) == null :
        "Block is still in the list and must be removed first.";
    // find the last not null node
    int lastNode = numNodes()-1;
    // replace current node triplet by the lastNode one
    setStorageInfo(dnIndex, getStorageInfo(lastNode));
    setNext(dnIndex, getNext(lastNode));
    setPrevious(dnIndex, getPrevious(lastNode));
    // set the last triplet to null
    setStorageInfo(lastNode, null);
    setNext(lastNode, null);
    setPrevious(lastNode, null);
    return true;
  }

  @Override
  boolean isProvided() {
    int len = getCapacity();
    for (int idx = 0; idx < len; idx++) {
      DatanodeStorageInfo storage = getStorageInfo(idx);
      if (storage != null
          && storage.getStorageType().equals(StorageType.PROVIDED)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public int numNodes() {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert triplets.length % 3 == 0 : "Malformed BlockInfo";

    for (int idx = getCapacity()-1; idx >= 0; idx--) {
      if (getDatanode(idx) != null) {
        return idx + 1;
      }
    }
    return 0;
  }

  @Override
  public final boolean isStriped() {
    return false;
  }

  @Override
  public BlockType getBlockType() {
    return BlockType.CONTIGUOUS;
  }

  @Override
  final boolean hasNoStorage() {
    return getStorageInfo(0) == null;
  }
}
