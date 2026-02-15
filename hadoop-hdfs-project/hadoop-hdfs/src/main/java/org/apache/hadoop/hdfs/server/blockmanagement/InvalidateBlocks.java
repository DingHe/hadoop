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

import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.LongAdder;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.util.LightWeightHashSet;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.hdfs.DFSUtil;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * Keeps a Collection for every named machine containing blocks
 * that have recently been invalidated and are thought to live
 * on the machine in question.
 */
//管理了所有关于待删除块的集合，并且确保在集群中对无效块的管理得当，避免性能问题
@InterfaceAudience.Private
class InvalidateBlocks {
  //键是 DatanodeInfo 对象（代表数据节点的信息），值是 LightWeightHashSet<Block>（每个数据节点对应的无效块的集合）。
  // 该映射表用于存储每个数据节点的无效块
  private final Map<DatanodeInfo, LightWeightHashSet<Block>>
      nodeToBlocks = new HashMap<>();
  //与 nodeToBlocks 类似，但它仅存储具有块类型 STRIPED（即分布式块组，通常用于 Erasure Coding）的无效块
  private final Map<DatanodeInfo, LightWeightHashSet<Block>>
      nodeToECBlocks = new HashMap<>();
  //用于记录待删除的块的总数，类型是 LongAdder，可以在多个线程之间高效地增减
  private final LongAdder numBlocks = new LongAdder();
  //类似于 numBlocks，但用于记录待删除的 EC（Erasure Coding）块的总数
  private final LongAdder numECBlocks = new LongAdder();
  //每次操作时最多可以删除的块数量
  private final int blockInvalidateLimit;
  //用于管理块的 ID，并确定某个块是否是分布式块
  private final BlockIdManager blockIdManager;

  /**
   * The period of pending time for block invalidation since the NameNode
   * startup
   */
  //自 NameNode 启动以来，块无效化的等待时间（以毫秒为单位）。在这个时间范围内，块的删除被推迟
  private final long pendingPeriodInMs;
  /** the startup time */
  //NameNode 启动的时间（以单调时钟时间为单位）。它用于计算距离启动已过的时间
  private final long startupTime = Time.monotonicNow();

  InvalidateBlocks(final int blockInvalidateLimit, long pendingPeriodInMs,
                   final BlockIdManager blockIdManager) {
    this.blockInvalidateLimit = blockInvalidateLimit;
    this.pendingPeriodInMs = pendingPeriodInMs;
    this.blockIdManager = blockIdManager;
    printBlockDeletionTime();
  }

  private void printBlockDeletionTime() {
    BlockManager.LOG.info("{} is set to {}",
        DFSConfigKeys.DFS_NAMENODE_STARTUP_DELAY_BLOCK_DELETION_SEC_KEY,
        DFSUtil.durationToString(pendingPeriodInMs));
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy MMM dd HH:mm:ss");
    Calendar calendar = new GregorianCalendar();
    calendar.add(Calendar.SECOND, (int) (this.pendingPeriodInMs / 1000));
    BlockManager.LOG.info("The block deletion will start around {}",
        sdf.format(calendar.getTime()));
  }

  /**
   * @return The total number of blocks to be invalidated.
   */
  long numBlocks() {
    return getECBlocks() + getBlocks();
  }

  /**
   * @return The total number of blocks of type
   * {@link org.apache.hadoop.hdfs.protocol.BlockType#CONTIGUOUS}
   * to be invalidated.
   */
  long getBlocks() {
    return numBlocks.longValue();
  }

  /**
   * @return The total number of blocks of type
   * {@link org.apache.hadoop.hdfs.protocol.BlockType#STRIPED}
   * to be invalidated.
   */
  long getECBlocks() {
    return numECBlocks.longValue();
  }
  //返回给定数据节点的普通块集合
  private LightWeightHashSet<Block> getBlocksSet(final DatanodeInfo dn) {
    return nodeToBlocks.get(dn);
  }
  //返回给定数据节点的 EC 块集合
  private LightWeightHashSet<Block> getECBlocksSet(final DatanodeInfo dn) {
    return nodeToECBlocks.get(dn);
  }

  private LightWeightHashSet<Block> getBlocksSet(final DatanodeInfo dn,
      final Block block) {
    if (blockIdManager.isStripedBlock(block)) {
      return getECBlocksSet(dn);
    } else {
      return getBlocksSet(dn);
    }
  }

  private void putBlocksSet(final DatanodeInfo dn, final Block block,
      final LightWeightHashSet set) {
    if (blockIdManager.isStripedBlock(block)) {
      assert getECBlocksSet(dn) == null;
      nodeToECBlocks.put(dn, set);
    } else {
      assert getBlocksSet(dn) == null;
      nodeToBlocks.put(dn, set);
    }
  }

  private long getBlockSetsSize(final DatanodeInfo dn) {
    LightWeightHashSet<Block> replicaBlocks = getBlocksSet(dn);
    LightWeightHashSet<Block> stripedBlocks = getECBlocksSet(dn);
    return ((replicaBlocks == null ? 0 : replicaBlocks.size()) +
        (stripedBlocks == null ? 0 : stripedBlocks.size()));
  }


  /**
   * @return true if the given storage has the given block listed for
   * invalidation. Blocks are compared including their generation stamps:
   * if a block is pending invalidation but with a different generation stamp,
   * returns false.
   */
  synchronized boolean contains(final DatanodeInfo dn, final Block block) {
    final LightWeightHashSet<Block> s = getBlocksSet(dn, block);
    if (s == null) {
      return false; // no invalidate blocks for this storage ID
    }
    Block blockInSet = s.getElement(block);
    return blockInSet != null &&
        block.getGenerationStamp() == blockInSet.getGenerationStamp();
  }

  /**
   * Add a block to the block collection which will be
   * invalidated on the specified datanode.
   */
  synchronized void add(final Block block, final DatanodeInfo datanode,
      final boolean log) {
    LightWeightHashSet<Block> set = getBlocksSet(datanode, block);
    if (set == null) {
      set = new LightWeightHashSet<>();
      putBlocksSet(datanode, block, set);
    }
    if (set.add(block)) {
      if (blockIdManager.isStripedBlock(block)) {
        numECBlocks.increment();
      } else {
        numBlocks.increment();
      }
      if (log) {
        NameNode.blockStateChangeLog.debug("BLOCK* {}: add {} to {}",
            getClass().getSimpleName(), block, datanode);
      }
    }
  }

  /** Remove a storage from the invalidatesSet */
  synchronized void remove(final DatanodeInfo dn) {
    LightWeightHashSet<Block> replicaBlockSets = nodeToBlocks.remove(dn);
    if (replicaBlockSets != null) {
      numBlocks.add(replicaBlockSets.size() * -1);
    }
    LightWeightHashSet<Block> ecBlocksSet = nodeToECBlocks.remove(dn);
    if (ecBlocksSet != null) {
      numECBlocks.add(ecBlocksSet.size() * -1);
    }
  }

  /** Remove the block from the specified storage. */
  synchronized void remove(final DatanodeInfo dn, final Block block) {
    final LightWeightHashSet<Block> v = getBlocksSet(dn, block);
    if (v != null && v.remove(block)) {
      if (blockIdManager.isStripedBlock(block)) {
        numECBlocks.decrement();
      } else {
        numBlocks.decrement();
      }
      if (v.isEmpty() && getBlockSetsSize(dn) == 0) {
        remove(dn);
      }
    }
  }

  private void dumpBlockSet(final Map<DatanodeInfo,
      LightWeightHashSet<Block>> nodeToBlocksMap, final PrintWriter out) {
    for(Entry<DatanodeInfo, LightWeightHashSet<Block>> entry :
        nodeToBlocksMap.entrySet()) {
      final LightWeightHashSet<Block> blocks = entry.getValue();
      if (blocks != null && blocks.size() > 0) {
        out.println(entry.getKey());
        out.println(StringUtils.join(',', blocks));
      }
    }
  }
  /** Print the contents to out. */
  synchronized void dump(final PrintWriter out) {
    final int size = nodeToBlocks.values().size() +
        nodeToECBlocks.values().size();
    out.println("Metasave: Blocks " + numBlocks()
        + " waiting deletion from " + size + " datanodes.");
    if (size == 0) {
      return;
    }
    dumpBlockSet(nodeToBlocks, out);
    dumpBlockSet(nodeToECBlocks, out);
  }

  /** @return a list of the storage IDs. */
  synchronized List<DatanodeInfo> getDatanodes() {
    HashSet<DatanodeInfo> set = new HashSet<>();
    set.addAll(nodeToBlocks.keySet());
    set.addAll(nodeToECBlocks.keySet());
    return new ArrayList<>(set);
  }

  /**
   * @return the remianing pending time
   */
  @VisibleForTesting
  long getInvalidationDelay() {
    return pendingPeriodInMs - (Time.monotonicNow() - startupTime);
  }

  /**
   * Get blocks to invalidate by limit as blocks that can be sent in one
   * message is limited.
   * @return the remaining limit
   */
  private int getBlocksToInvalidateByLimit(LightWeightHashSet<Block> blockSet,
      List<Block> toInvalidate, LongAdder statsAdder, int limit) {
    assert blockSet != null;
    int remainingLimit = limit;
    List<Block> polledBlocks = blockSet.pollN(limit);
    remainingLimit -= polledBlocks.size();
    toInvalidate.addAll(polledBlocks);
    statsAdder.add(polledBlocks.size() * -1);
    return remainingLimit;
  }

  synchronized List<Block> invalidateWork(final DatanodeDescriptor dn) {
    final long delay = getInvalidationDelay();
    if (delay > 0) {
      BlockManager.LOG
          .debug("Block deletion is delayed during NameNode startup. "
              + "The deletion will start after {} ms.", delay);
      return null;
    }

    int remainingLimit = blockInvalidateLimit;
    final List<Block> toInvalidate = new ArrayList<>();

    if (nodeToBlocks.get(dn) != null) {
      remainingLimit = getBlocksToInvalidateByLimit(nodeToBlocks.get(dn),
          toInvalidate, numBlocks, remainingLimit);
    }
    if ((remainingLimit > 0) && (nodeToECBlocks.get(dn) != null)) {
      getBlocksToInvalidateByLimit(nodeToECBlocks.get(dn),
          toInvalidate, numECBlocks, remainingLimit);
    }
    if (toInvalidate.size() > 0) {
      if (getBlockSetsSize(dn) == 0) {
        remove(dn);
      }
      dn.addBlocksToBeInvalidated(toInvalidate);
    }
    return toInvalidate;
  }
  
  synchronized void clear() {
    nodeToBlocks.clear();
    nodeToECBlocks.clear();
    numBlocks.reset();
    numECBlocks.reset();
  }
}
