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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.security.AccessControlException;

/** 
 * This interface is used by the block manager to expose a
 * few characteristics of a collection of Block/BlockUnderConstruction.
 */
@InterfaceAudience.Private
public interface BlockCollection {
  /**获取块集合中的最后一个块（BlockInfo）
   * Get the last block of the collection.
   */
  BlockInfo getLastBlock();

  /** 计算块集合的内容摘要（文件大小、块数、目录数量等）
   * Get content summary.
   */
  ContentSummary computeContentSummary(BlockStoragePolicySuite bsps)
      throws AccessControlException;

  /**获取当前块集合中块（或块组）的数量
   * @return the number of blocks or block groups
   */ 
  int numBlocks();

  /**获取块集合中的所有块信息
   * Get the blocks (striped or contiguous).
   */
  BlockInfo[] getBlocks();

  /**获取块集合的 首选块大小（通常由 HDFS dfs.blocksize 配置决定）
   * Get preferred block size for the collection 
   * @return preferred block size in bytes
   */
  long getPreferredBlockSize();

  /**获取块集合的 首选复制因子（即每个块存储的副本数）
   * Get block replication for the collection.
   * @return block replication value. Return 0 if the file is erasure coded.
   */
  short getPreferredBlockReplication();

  /**获取块集合的 存储策略 ID，指示该集合如何存储数据（热数据、冷数据等），使用场景，管理不同存储介质（SSD、HDD、ARCHIVE）的文件存储方式
   * @return the storage policy ID.
   */
  byte getStoragePolicyID();

  /** 获取块集合的名称（通常是文件路径）
   * Get the name of the collection.
   */
  String getName();

  /**
   * Set the block (contiguous or striped) at the given index.
   */
  void setBlock(int index, BlockInfo blk);

  /** 最后一个块 转换为 正在构造的块（Under Construction）
   * Convert the last block of the collection to an under-construction block
   * and set the locations.
   */
  void convertLastBlockToUC(BlockInfo lastBlock,
      DatanodeStorageInfo[] targets) throws IOException;

  /**判断当前块集合是否 处于构造状态（即文件是否仍在写入）
   * @return whether the block collection is under construction.
   */
  boolean isUnderConstruction();

  /**判断当前块集合是否 采用条带化存储（即是否使用 Erasure Coding）
   * @return whether the block collection is in striping format
   */
  boolean isStriped();

  /**获取当前块集合的唯一 ID（通常是 INodeFile 的 ID）
   * @return the id for the block collection
   */
  long getId();
}
