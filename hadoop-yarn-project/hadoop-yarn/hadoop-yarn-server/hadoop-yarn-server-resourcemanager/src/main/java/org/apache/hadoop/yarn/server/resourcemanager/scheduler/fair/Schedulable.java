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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;

/**
 * A Schedulable represents an entity that can be scheduled such as an
 * application or a queue. It provides a common interface so that algorithms
 * such as fair sharing can be applied both within a queue and across queues.
 *
 * A Schedulable is responsible for three roles:
 * 1) Assign resources through {@link #assignContainer}.
 * 2) It provides information about the app/queue to the scheduler, including:
 *    - Demand (maximum number of tasks required)
 *    - Minimum share (for queues)
 *    - Job/queue weight (for fair sharing)
 *    - Start time and priority (for FIFO)
 * 3) It can be assigned a fair share, for use with fair scheduling.
 *
 * Schedulable also contains two methods for performing scheduling computations:
 * - updateDemand() is called periodically to compute the demand of the various
 *   jobs and queues, which may be expensive (e.g. jobs must iterate through all
 *   their tasks to count failed tasks, tasks that can be speculated, etc).
 */
//表示可以被调度的实体，例如应用程序或队列。它提供了一个公共接口，使得调度算法（例如公平共享）可以在队列内部以及队列之间应用。
// Schedulable 负责管理资源的分配、提供有关应用程序/队列的信息，并支持公平调度中分配公平份额的计算
@Private
@Unstable
public interface Schedulable {
  /**
   * Name of job/queue, used for debugging as well as for breaking ties in
   * scheduling order deterministically.
   * @return Name of job/queue.
   */
  //返回调度实体（作业或队列）的名称。这个名称用于调试和调度顺序中的决策，确保在调度中能唯一标识该实体
  String getName();

  /**
   * Maximum number of resources required by this Schedulable. This is defined as
   * number of currently utilized resources + number of unlaunched resources (that
   * are either not yet launched or need to be speculated).
   * @return resources required by this Schedulable.
   */
  //返回此 Schedulable 实体所需的最大资源数量。
  // 包括当前已使用的资源和未启动的资源（即尚未启动或需要进行推测的资源）。这个方法用于计算调度器是否可以满足该实体的资源需求
  Resource getDemand();

  /**
   * Get the aggregate amount of resources consumed by the schedulable.
   * @return aggregate amount of resources.
   */
  //返回调度实体当前消耗的资源总量。这个信息可以帮助调度器了解当前的资源占用情况
  Resource getResourceUsage();

  /**
   * Minimum Resource share assigned to the schedulable.
   * @return Minimum Resource share.
   */
  //返回分配给此 Schedulable 实体的最小资源份额。该方法用于确保该实体至少能够获得一定的资源，以免被饿死
  Resource getMinShare();

  /**
   * Maximum Resource share assigned to the schedulable.
   * @return Maximum Resource share.
   */
  //返回分配给此 Schedulable 实体的最大资源份额。这个方法用于限制某个实体最大能够占用的资源量，防止某个实体占用过多资源影响其他实体
  Resource getMaxShare();

  /**
   * Job/queue weight in fair sharing. Weights are only meaningful when
   * compared. A weight of 2.0f has twice the weight of a weight of 1.0f,
   * which has twice the weight of a weight of 0.5f. A weight of 1.0f is
   * considered unweighted or a neutral weight. A weight of 0 is no weight.
   *
   * @return the weight
   */
  //返回作业/队列在公平共享中的权重。权重用于在调度中进行相对比较，例如，权重大于 1.0 的实体会比权重为 1.0 的实体获得更多的资源
  float getWeight();

  /**
   * Start time for jobs in FIFO queues; meaningless for QueueSchedulables.
   * @return Start time for jobs.
   */
  //返回作业在 FIFO 队列中的启动时间。对于队列调度实体（如队列本身）来说，这个值是没有意义的
  long getStartTime();

 /**
  * Job priority for jobs in FIFO queues; meaningless for QueueSchedulables.
  * @return Job priority.
  */
 //返回作业的优先级。对于 FIFO 队列中的作业，这个值是重要的，可以决定作业调度的先后顺序。对于队列调度实体，这个值没有意义
  Priority getPriority();

  /** Refresh the Schedulable's demand and those of its children if any. */
  //用于刷新 Schedulable 实体及其子实体（如果有的话）的需求。通常是定期调用，用来计算不同作业和队列的需求。
  // 这个方法可能会很昂贵，因为作业必须遍历它们的所有任务来统计失败的任务、可以进行推测的任务等
  void updateDemand();

  /**
   * Assign a container on this node if possible, and return the amount of
   * resources assigned.
   *
   * @param node FSSchedulerNode.
   * @return the amount of resources assigned.
   */
  //尝试在指定的节点上分配一个容器，如果可能的话，返回分配的资源量
  Resource assignContainer(FSSchedulerNode node);

  /**
   * Get the fair share assigned to this Schedulable.
   * @return the fair share assigned to this Schedulable.
   */
  //返回分配给此 Schedulable 实体的公平份额。
  // 公平份额是调度器根据公平调度算法计算出来的资源分配量，用于保证各个实体在公平调度中得到相应的资源
  Resource getFairShare();

  /**
   * Assign a fair share to this Schedulable.
   * @param fairShare a fair share to this Schedulable.
   */
  void setFairShare(Resource fairShare);

  /**
   * Check whether the schedulable is preemptable.
   * @return <code>true</code> if the schedulable is preemptable;
   *         <code>false</code> otherwise
   */
  //判断该 Schedulable 实体是否可以被抢占。若返回 true，表示该实体的资源可以被其他实体抢占，通常用于实现资源抢占机制
  boolean isPreemptable();
}
