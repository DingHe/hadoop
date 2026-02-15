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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy;

import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;


/**
 * A SchedulableEntity is a process to be scheduled.
 * for example, an application / application attempt
 */
// 代表一个需要调度的实体。在 YARN 调度器中，它通常用于表示一个应用程序或应用程序的尝试。
// 该接口定义了与调度相关的基本方法，这些方法允许调度器了解应用的资源需求、优先级、分区信息等，并且能够根据调度策略（例如 FIFO）来处理这些实体
public interface SchedulableEntity {
  
  /** 返回该调度实体的唯一标识符
   * Id - each entity must have a unique id.
   * @return id.
   */
  public String getId();
  
  /**
   * Compare the passed SchedulableEntity to this one for input order.
   * Input order is implementation defined and should reflect the 
   * correct ordering for first-in first-out processing.
   *
   * @param other SchedulableEntity.
   * @return correct ordering.
   */
  //返回一个整数，表示当前实体与 other 实体的顺序。具体的比较逻辑取决于实现，这通常会与调度策略（如 FIFO）相关
  public int compareInputOrderTo(SchedulableEntity other);
  
  /**
   * View of Resources wanted and consumed by the entity.
   * @return ResourceUsage.
   */
  //提供调度实体的资源使用情况（例如内存、CPU 核心等）。实现类需要提供关于资源需求和使用的详细信息，调度器根据这些信息来安排资源分配
  public ResourceUsage getSchedulingResourceUsage();
  
  /**
   * Get the priority of the application.
   * @return priority of the application.
   */
  //返回该调度实体的优先级
  public Priority getPriority();

  /**
   * Whether application was running before RM restart.
   * @return true, application was running before RM restart;
   * otherwise false.
   */
  //指示该应用程序是否在资源管理器重启后恢复运行
  public boolean isRecovering();

  /**
   * Get partition corresponding to this entity.
   * @return partition node label.
   */
  //与此调度实体相关联的分区节点标签（即该实体所需的资源所在的节点分区）
  String getPartition();

  /**
   * Start time of the job.
   * @return start time
   */
  //该调度实体的启动时间（即作业开始时间）
  long getStartTime();
}
