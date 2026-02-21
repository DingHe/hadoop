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
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;

/**
 * ReplicaUnderConstruction contains information about replicas (or blocks
 * belonging to a block group) while they are under construction.
 *
 * The GS, the length and the state of the replica is as reported by the
 * datanode.
 *
 * It is not guaranteed, but expected, that datanodes actually have
 * corresponding replicas.
 */
// 主要用于追踪那些尚未完成写入的数据块副本状态。
// 当一个客户端开始向 HDFS 写入文件时，该文件对应的数据块处于 UNDER_CONSTRUCTION 状态。
// 此时，NameNode 需要记录这个块被分配到了哪些 DataNode 上，以及这些 DataNode 上副本的实时进度。
// 流水线管理：记录副本被分配的预期位置（Expected Location），这决定了写入流水线（Pipeline）的节点顺序。
// 状态追踪：监控副本在 DataNode 上的生命周期状态（如正在写入、等待恢复等）。
// 故障恢复辅助：在租约恢复（Lease Recovery）期间，标记哪个副本被选为主节点（Primary）来同步其他副本的数据。
class ReplicaUnderConstruction extends Block {
  // 副本的预期存储位置。
  // 这是在 NameNode 分配块时指定的 DataNode 存储单元。
  // 虽然它是“预期的”，但在正常写入过程中，数据就应该流向这里。它不仅代表了物理位置，也隐含了它在写入流水线中的拓扑位置。
  private final DatanodeStorageInfo expectedLocation;
  // 副本的当前状态。
  private HdfsServerConstants.ReplicaState state;
  // 是否被选为主恢复节点。
  private boolean chosenAsPrimary;

  ReplicaUnderConstruction(Block block,
      DatanodeStorageInfo target,
      HdfsServerConstants.ReplicaState state) {
    super(block);
    this.expectedLocation = target;
    this.state = state;
    this.chosenAsPrimary = false;
  }

  /**
   * Expected block replica location as assigned when the block was allocated.
   * This defines the pipeline order.
   * It is not guaranteed, but expected, that the data-node actually has
   * the replica.
   */
  DatanodeStorageInfo getExpectedStorageLocation() {
    return expectedLocation;
  }

  /**
   * Get replica state as reported by the data-node.
   */
  HdfsServerConstants.ReplicaState getState() {
    return state;
  }

  /**
   * Whether the replica was chosen for recovery.
   */
  boolean getChosenAsPrimary() {
    return chosenAsPrimary;
  }

  /**
   * Set replica state.
   */
  void setState(HdfsServerConstants.ReplicaState s) {
    state = s;
  }

  /**
   * Set whether this replica was chosen for recovery.
   */
  void setChosenAsPrimary(boolean chosenAsPrimary) {
    this.chosenAsPrimary = chosenAsPrimary;
  }

  /**
   * Is data-node the replica belongs to alive.
   */
  boolean isAlive() {
    return expectedLocation.getDatanodeDescriptor().isAlive();
  }

  @Override // Block
  public int hashCode() {
    return super.hashCode();
  }

  @Override // Block
  public boolean equals(Object obj) {
    // Sufficient to rely on super's implementation
    return (this == obj) || super.equals(obj);
  }

  @Override
  public String toString() {
    final StringBuilder b = new StringBuilder(50);
    appendStringTo(b);
    return b.toString();
  }

  @Override
  public void appendStringTo(StringBuilder sb) {
    sb.append("ReplicaUC[")
        .append(expectedLocation)
        .append("|")
        .append(state)
        .append("]");
  }
}

