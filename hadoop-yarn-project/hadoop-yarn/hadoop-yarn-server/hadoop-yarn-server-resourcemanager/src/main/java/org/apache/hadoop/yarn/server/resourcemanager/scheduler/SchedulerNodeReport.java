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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;

/**
 * Node usage report.
 */
//表示 YARN 调度器对某个节点（Node）的资源使用情况的报告。它提供该节点的已用资源、可用资源、资源利用率及容器数量，
// 帮助资源管理器（ResourceManager）进行调度决策
@Private
@Stable
public class SchedulerNodeReport {
  private final Resource used;//已分配的资源，表示当前节点上已被应用使用的 CPU、内存等资源
  private final Resource avail;//未分配的资源，表示当前节点上仍可用于调度的资源量。
  private final ResourceUtilization utilization;//资源利用率，用于存储 CPU、内存等资源的使用情况（可能包括自定义资源，如 GPU）
  private final int num;//当前运行的容器数量，表示该节点上正在运行的任务数
  
  public SchedulerNodeReport(SchedulerNode node) {
    this.used = node.getAllocatedResource();
    this.avail = node.getUnallocatedResource();
    this.num = node.getNumContainers();
    this.utilization = node.getNodeUtilization();
  }
  
  /**
   * @return the amount of resources currently used by the node.
   */
  public Resource getUsedResource() {
    return used;
  }

  /**
   * @return the amount of resources currently available on the node
   */
  public Resource getAvailableResource() {
    return avail;
  }

  /**
   * @return the number of containers currently running on this node.
   */
  public int getNumContainers() {
    return num;
  }

  /**
   *
   * @return utilization of this node
   */
  public ResourceUtilization getUtilization() {
    return utilization;
  }
}
