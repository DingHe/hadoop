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
package org.apache.hadoop.hdfs.server.namenode.metrics;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * This interface defines the methods to get status pertaining to blocks of type
 * {@link org.apache.hadoop.hdfs.protocol.BlockType#CONTIGUOUS} in FSNamesystem
 * of a NameNode. It is also used for publishing via JMX.
 * <p>
 * Aggregated status of all blocks is reported in
 * @see FSNamesystemMBean
 * Name Node runtime activity statistic info is reported in
 * @see org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics
 *///用于获取与 Hadoop HDFS 中 复制块（Replicated Blocks） 状态相关的信息。该接口定义了多个方法，提供有关复制块的状态数据，供管理和监控工具（例如 JMX）使用
@InterfaceAudience.Private
public interface ReplicatedBlocksMBean {
  /**返回复制冗余度较低的块的数量。冗余度较低的块是指它们的副本数低于所期望的副本数
   * Return low redundancy blocks count.
   */
  long getLowRedundancyReplicatedBlocks();

  /**返回损坏的复制块的数量。损坏的块是指它们的副本中的数据损坏，导致无法读取或使用
   * Return corrupt blocks count.
   */
  long getCorruptReplicatedBlocks();

  /**返回丢失的复制块的数量。丢失的块是指其所有副本都不可用的块
   * Return missing blocks count.
   */
  long getMissingReplicatedBlocks();

  /**返回复制因子为 1 的丢失块的数量。复制因子为 1 的块只有一个副本，如果这个副本丢失，块就会丢失
   * Return count of missing blocks with replication factor one.
   */
  long getMissingReplicationOneBlocks();

  /**返回未来复制块的总字节数。未来的块是指已创建但尚未完全分配或写入的块
   * Return total bytes of future blocks.
   */
  long getBytesInFutureReplicatedBlocks();

  /**返回等待删除的复制块的数量。这些块正在排队等待删除，可能是因为不再需要或被标记为删除
   * Return count of blocks that are pending deletion.
   */
  long getPendingDeletionReplicatedBlocks();

  /**返回复制块的总数量。复制块是指所有正常存在且没有标记为丢失、损坏或待删除的块
   * Return total number of replicated blocks.
   */
  long getTotalReplicatedBlocks();
}
