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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerHealth;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.preemption.PreemptionManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

/**
 * Read-only interface to {@link CapacityScheduler} context.
 */
// CapacityScheduler（容量调度器）的只读上下文接口，用于提供调度器运行时的各种配置信息和资源状态
public interface CapacitySchedulerContext {
  //获取 CapacityScheduler 的配置对象，包含队列资源配额、调度策略等
  CapacitySchedulerConfiguration getConfiguration();
  //获取 CapacityScheduler 的队列上下文信息（如队列层级结构、资源管理等
  CapacitySchedulerQueueContext getQueueContext();
  //返回 YARN 集群允许的最小资源单位（如最小 CPU 核数、最小内存量）
  Resource getMinimumResourceCapability();
  //返回 YARN 集群允许的最大资源单位（如最大 CPU 核数、最大内存量）
  Resource getMaximumResourceCapability();
  //根据指定的 queueName 返回该队列允许的最大资源能力
  Resource getMaximumResourceCapability(String queueName);
  //获取 ContainerTokenSecretManager，用于管理容器的安全令牌
  RMContainerTokenSecretManager getContainerTokenSecretManager();
  //返回当前 YARN 集群的节点数量
  int getNumClusterNodes();

  RMContext getRMContext();
  //返回整个YARN集群的总资源信息
  Resource getClusterResource();

  /**
   * Get the yarn configuration.
   * @return yarn configuration.
   */
  Configuration getConf();
  //获取资源计算器 ResourceCalculator，用于比较不同资源类型（如 CPU 和内存）
  ResourceCalculator getResourceCalculator();
  //获取指定 NodeId 的 FiCaSchedulerNode，即调度器中的节点信息
  FiCaSchedulerNode getNode(NodeId nodeId);
  //获取 attemptId 对应的 FiCaSchedulerApp（表示 YARN 中的应用尝试）
  FiCaSchedulerApp getApplicationAttempt(ApplicationAttemptId attemptId);
  //获取 PreemptionManager，用于管理资源的抢占策略（即高优先级任务是否可以抢占低优先级任务的资源）
  PreemptionManager getPreemptionManager();
  //获取调度器的健康状况信息（如是否存在资源分配不均衡等问题）
  SchedulerHealth getSchedulerHealth();
  //获取最近一次 YARN 节点状态更新的时间戳
  long getLastNodeUpdateTime();

  /**
   * @return QueueCapacities root queue of the Capacity Scheduler Queue, root
   *         queue used capacities for different labels are same as that of the
   *         cluster.
   */
  //返回集群的资源使用情况（已使用、可用资源等）
  ResourceUsage getClusterResourceUsage();
  //获取 ActivitiesManager，用于记录和跟踪调度决策过程中的活动日志
  ActivitiesManager getActivitiesManager();
  //获取 CapacityScheduler 的队列管理器，负责管理调度器中的资源队列
  CapacitySchedulerQueueManager getCapacitySchedulerQueueManager();

  /**
   *获取整个集群中应用程序的最大优先级
   * @return Max Cluster level App priority.
   */
  Priority getMaxClusterLevelAppPriority();

  /**
   * Returns if configuration is mutable.
   * @return if configuration is mutable
   */
  boolean isConfigurationMutable();

  /**
   * Get clock from scheduler
   * @return Clock
   */
  Clock getClock();
  //获取应用程序的排序规则，用于确定哪些应用程序的资源请求优先级更高。
  CapacityScheduler.PendingApplicationComparator getPendingApplicationComparator();
}
