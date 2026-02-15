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

package org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity;

import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy.IntraQueuePreemptionOrderPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * This interface provides context for the calculation of ideal allocation
 * and preemption for the {@code CapacityScheduler}.
 */
//预抢占机制 的核心接口之一，专门为 容量调度器 (CapacityScheduler) 提供预抢占上下文。其主要作用包括：
//计算理想的资源分配：帮助确定队列应该获得的公平资源量。
//执行资源预抢占：识别哪些容器可以被优先终止，以释放资源给更高优先级的应用。
//管理调度器状态：维护集群的资源信息、已分配资源情况、受影响队列等数据。
//提供预抢占策略参数：定义各种策略参数，如最大允许超配比 (MaxIgnoreOverCapacity)、
// 自然终止因子 (NaturalTerminationFactor)、跨队列预抢占 (CrossQueuePreemption) 等
public interface CapacitySchedulerPreemptionContext {
  //获取 CapacityScheduler 实例，以便访问当前调度器的状态和方法
  CapacityScheduler getScheduler();
  //获取特定队列 (queueName) 在特定分区 (partition) 上的临时资源状态
  TempQueuePerPartition getQueueByPartition(String queueName,
      String partition);
  //获取 queueName 队列下所有的分区资源信息
  Collection<TempQueuePerPartition> getQueuePartitions(String queueName);
  //获取 ResourceCalculator 实例，该实例用于计算不同资源类型（如 CPU、内存）之间的公平性。
  ResourceCalculator getResourceCalculator();

  RMContext getRMContext();
  //检查是否仅观察模式，即不会真正触发资源回收，而是用于测试和监控
  boolean isObserveOnly();
  //返回所有可以被杀死（用于预抢占）的容器 ID
  Set<ContainerId> getKillableContainers();
  //获取最大超分配比率，当超分配超过此值时，系统将尝试回收资源
  double getMaxIgnoreOverCapacity();
  //获取自然终止因子，用于估算某些任务可能在短时间内自己结束的概率，以减少不必要的预抢占
  double getNaturalTerminationFactor();
  //返回所有叶子队列的名称，这些队列是真正运行应用的地方
  Set<String> getLeafQueueNames();
  //返回集群中的所有资源分区名称
  Set<String> getAllPartitions();
  //返回集群允许的最高应用优先级值。
  int getClusterMaxApplicationPriority();
  //获取指定 partition 的可用资源信息
  Resource getPartitionResource(String partition);
  //获取在 partition 上资源不足的队列集合
  LinkedHashSet<String> getUnderServedQueuesPerPartition(String partition);
  //将 queueName 标记为 partition 上的资源不足队列，供后续资源调整使用
  void addPartitionToUnderServedQueues(String queueName, String partition);
  //获取队列内预抢占的最小触发阈值，低于该阈值时不会触发资源回收
  float getMinimumThresholdForIntraQueuePreemption();
  //获取队列内最大可用的预抢占比例，确保不会过度回收资源
  float getMaxAllowableLimitForIntraQueuePreemption();
  //获取默认的容器杀死等待超时，避免任务过早终止影响作业执行
  long getDefaultMaximumKillWaitTimeout();
  //获取队列内部预抢占顺序策略，如按照优先级或资源占用情况决定先杀哪些任务
  @Unstable
  IntraQueuePreemptionOrderPolicy getIntraQueuePreemptionOrderPolicy();
  //检查是否启用跨队列的保守 DRF 预抢占策略
  boolean getCrossQueuePreemptionConservativeDRF();
   //检查是否启用队列内部的保守 DRF 预抢占策略
  boolean getInQueuePreemptionConservativeDRF();
}
