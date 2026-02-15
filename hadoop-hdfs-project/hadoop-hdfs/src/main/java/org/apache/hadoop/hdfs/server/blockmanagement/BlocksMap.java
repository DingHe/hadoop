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

import java.util.Iterator;
import java.util.concurrent.atomic.LongAdder;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.INodeId;
import org.apache.hadoop.util.GSet;
import org.apache.hadoop.util.LightWeightGSet;

/**
 * This class maintains the map from a block to its metadata.
 * block's metadata currently includes blockCollection it belongs to and
 * the datanodes that store the block.
 */
//主要用于管理 HDFS 中块的映射关系，它维护了从 Block 到 BlockInfo 的映射，存储了块的元数据（如存储的节点、所属的文件等）。
// 它提供了对块的添加、删除、查询以及配额管理等操作。
// 同时，它还维护了块的统计信息，如复制块和 EC 块组的数量，确保了 HDFS 的数据块管理和配额统计功能
class BlocksMap {
  public static class StorageIterator implements Iterator<DatanodeStorageInfo> {
    private final BlockInfo blockInfo;
    private int nextIdx = 0;

    StorageIterator(BlockInfo blkInfo) {
      this.blockInfo = blkInfo;
    }

    @Override
    public boolean hasNext() {
      if (blockInfo == null) {
        return false;
      }
      while (nextIdx < blockInfo.getCapacity() &&
          blockInfo.getDatanode(nextIdx) == null) {
        // note that for striped blocks there may be null in the triplets
        nextIdx++;
      }
      return nextIdx < blockInfo.getCapacity();
    }

    @Override
    public DatanodeStorageInfo next() {
      return blockInfo.getStorageInfo(nextIdx++);
    }

    @Override
    public void remove()  {
      throw new UnsupportedOperationException("Sorry. can't remove.");
    }
  }

  /** Constant {@link LightWeightGSet} capacity. */
  //表示 BlocksMap 的容量，也就是存储 Block 的集合的大小
  private final int capacity;
  // GSet<Block, BlockInfo> 类型的属性，表示存储 Block 与其对应的 BlockInfo 对象之间的映射关系
  private GSet<Block, BlockInfo> blocks;
  //LongAdder 内部维护了一组变量（Cell 数组），而不是单一的计数器
  //多个线程可以同时对不同的变量进行累加，从而避免了对同一个变量的频繁竞争
  //当需要获取最终的累加值时，LongAdder 会将所有内部变量的值相加
  private final LongAdder totalReplicatedBlocks = new LongAdder();//记录已复制的块的数量
  private final LongAdder totalECBlockGroups = new LongAdder();//记录存储的 EC（Erasure Code）块组的数量

  BlocksMap(int capacity) {
    // Use 2% of total memory to size the GSet capacity
    this.capacity = capacity;
    this.blocks = new LightWeightGSet<Block, BlockInfo>(capacity) {
      @Override
      public Iterator<BlockInfo> iterator() {
        SetIterator iterator = new SetIterator();
        /*
         * Not tracking any modifications to set. As this set will be used
         * always under FSNameSystem lock, modifications will not cause any
         * ConcurrentModificationExceptions. But there is a chance of missing
         * newly added elements during iteration.
         */
        iterator.setTrackModification(false);
        return iterator;
      }
    };
  }


  void close() {
    clear();
    blocks = null;
  }
  
  void clear() {
    if (blocks != null) {
      blocks.clear();
      totalReplicatedBlocks.reset();
      totalECBlockGroups.reset();
    }
  }

  /**
   * Add block b belonging to the specified block collection to the map.
   */
  //b：要添加的 BlockInfo 对象，表示一个块
  //bc：BlockCollection 对象，表示块所属的块集合（通常是一个文件或目录）
  //首先从 blocks 集合中获取 b 对应的 BlockInfo 对象，如果不存在就将 b 加入到集合中
  BlockInfo addBlockCollection(BlockInfo b, BlockCollection bc) {
    BlockInfo info = blocks.get(b);
    if (info != b) {
      info = b;
      blocks.put(info);
      incrementBlockStat(info);
    }
    info.setBlockCollectionId(bc.getId());
    return info;
  }

  /**
   * Remove the block from the block map;
   * remove it from all data-node lists it belongs to;
   * and remove all data-node locations associated with the block.
   */
  //block：要移除的 BlockInfo 对象
  void removeBlock(BlockInfo block) {
    BlockInfo blockInfo = blocks.remove(block);
    if (blockInfo == null) {
      return;
    }
    //如果 blockInfo 不为 null，则首先递减统计数
    decrementBlockStat(block);

    assert blockInfo.getBlockCollectionId() == INodeId.INVALID_INODE_ID;
    final int size = blockInfo.isStriped() ?
        blockInfo.getCapacity() : blockInfo.numNodes();  //block在多少个节点上
    for(int idx = size - 1; idx >= 0; idx--) {//把所有相关的block删除
      DatanodeDescriptor dn = blockInfo.getDatanode(idx);
      if (dn != null) {
        removeBlock(dn, blockInfo); // remove from the list and wipe the location
      }
    }
  }

  /** Returns the block object if it exists in the map. */
  BlockInfo getStoredBlock(Block b) {
    return blocks.get(b);
  }

  /**
   * Searches for the block in the BlocksMap and 
   * returns {@link Iterable} of the storages the block belongs to.
   */
  Iterable<DatanodeStorageInfo> getStorages(Block b) {
    return getStorages(blocks.get(b));
  }

  /**
   * For a block that has already been retrieved from the BlocksMap
   * returns {@link Iterable} of the storages the block belongs to.
   */
  Iterable<DatanodeStorageInfo> getStorages(final BlockInfo storedBlock) {
    return new Iterable<DatanodeStorageInfo>() {
      @Override
      public Iterator<DatanodeStorageInfo> iterator() {
        return new StorageIterator(storedBlock);
      }
    };
  }

  /** counts number of containing nodes. Better than using iterator. */
  int numNodes(Block b) {
    BlockInfo info = blocks.get(b);
    return info == null ? 0 : info.numNodes();
  }

  /**
   * Remove data-node reference from the block.
   * Remove the block from the block map
   * only if it does not belong to any file and data-nodes.
   */
  boolean removeNode(Block b, DatanodeDescriptor node) {
    BlockInfo info = blocks.get(b);
    if (info == null)
      return false;

    // remove block from the data-node list and the node from the block info
    boolean removed = removeBlock(node, info);

    if (info.hasNoStorage()    // no datanodes left
        && info.isDeleted()) { // does not belong to a file
      blocks.remove(b);  // remove block from the map
      decrementBlockStat(info);
    }
    return removed;
  }

  /**
   * Remove block from the list of blocks belonging to the data-node. Remove
   * data-node from the block.
   */
  static boolean removeBlock(DatanodeDescriptor dn, BlockInfo b) {
    final DatanodeStorageInfo s = b.findStorageInfo(dn);
    // if block exists on this datanode
    return s != null && s.removeBlock(b);
  }

  int size() {
    if (blocks != null) {
      return blocks.size();
    } else {
      return 0;
    }
  }

  Iterable<BlockInfo> getBlocks() {
    return blocks;
  }
  
  /** Get the capacity of the HashMap that stores blocks */
  int getCapacity() {
    return capacity;
  }

  private void incrementBlockStat(BlockInfo block) {
    if (block.isStriped()) {
      totalECBlockGroups.increment();
    } else {
      totalReplicatedBlocks.increment();
    }
  }

  private void decrementBlockStat(BlockInfo block) {
    if (block.isStriped()) {
      totalECBlockGroups.decrement();
      assert totalECBlockGroups.longValue() >= 0 :
          "Total number of ec block groups should be non-negative";
    } else {
      totalReplicatedBlocks.decrement();
      assert totalReplicatedBlocks.longValue() >= 0 :
          "Total number of replicated blocks should be non-negative";
    }
  }

  long getReplicatedBlocks() {
    return totalReplicatedBlocks.longValue();
  }

  long getECBlockGroups() {
    return totalECBlockGroups.longValue();
  }
}
