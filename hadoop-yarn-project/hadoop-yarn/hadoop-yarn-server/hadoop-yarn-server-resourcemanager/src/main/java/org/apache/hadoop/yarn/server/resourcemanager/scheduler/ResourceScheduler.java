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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Recoverable;

/**
 * This interface is the one implemented by the schedulers. It mainly extends 
 * {@link YarnScheduler}. 
 *
 */
//主要功能是与调度器的初始化、资源分配、节点管理等相关的操作
@LimitedPrivate("yarn")
@Evolving
public interface ResourceScheduler extends YarnScheduler, Recoverable {

  /**
   * Set RMContext for <code>ResourceScheduler</code>.
   * This method should be called immediately after instantiating
   * a scheduler once.
   * @param rmContext created by ResourceManager
   */
  void setRMContext(RMContext rmContext);

  /** 重新初始化 ResourceScheduler。该方法通常用于重新配置调度器，或者在 ResourceManager 重启时使用
   * Re-initialize the <code>ResourceScheduler</code>.
   * @param conf configuration
   * @param rmContext RMContext.
   * @throws IOException an I/O exception has occurred.
   */
  void reinitialize(Configuration conf, RMContext rmContext) throws IOException;

  /** 获取集群中可用的节点 ID，基于指定的资源名称
   * Get the {@link NodeId} available in the cluster by resource name.
   * @param resourceName resource name
   * @return the number of available {@link NodeId} by resource name.
   */
  List<NodeId> getNodeIds(String resourceName);

  /** 尝试在指定节点上为应用程序分配资源。此方法忽略 numAllocations，只尝试分配一个容器
   * Attempts to allocate a SchedulerRequest on a Node.
   * NOTE: This ignores the numAllocations in the resource sizing and tries
   *       to allocate a SINGLE container only.
   * @param appAttempt ApplicationAttempt.
   * @param schedulingRequest SchedulingRequest.
   * @param schedulerNode SchedulerNode.
   * @return true if proposal was accepted.
   */
  boolean attemptAllocationOnNode(SchedulerApplicationAttempt appAttempt,
      SchedulingRequest schedulingRequest, SchedulerNode schedulerNode);

  /** 重置调度器的指标。通常在特定的条件下，如重启、重新初始化等，需要重置调度器的相关统计数据和度量指标
   * Reset scheduler metrics.
   */
  void resetSchedulerMetrics();
}
