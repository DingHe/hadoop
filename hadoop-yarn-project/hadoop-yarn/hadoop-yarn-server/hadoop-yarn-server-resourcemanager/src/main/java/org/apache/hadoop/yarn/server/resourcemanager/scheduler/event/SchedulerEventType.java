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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.event;
//定义 YARN 资源管理器调度器 (Scheduler) 内部的各种事件类型。这些事件用于描述节点、应用、容器等在 YARN 调度过程中的各种状态变化，
// 并在 YARN 事件系统 (EventHandler) 中传递给 SchedulerEvent，从而触发相应的调度行为
//它主要用于 SchedulerEvent 类中，调度器根据事件类型作出相应的决策，例如：
//处理新的计算节点 (NODE_ADDED)
//移除节点 (NODE_REMOVED)
//处理应用提交 (APP_ADDED) 和移除 (APP_REMOVED)
//处理容器过期 (CONTAINER_EXPIRED)
//释放容器 (RELEASE_CONTAINER)
//进行资源抢占 (MARK_CONTAINER_FOR_PREEMPTION)
public enum SchedulerEventType {

  // Source: Node 与节点相关的事件 (Node 相关)
  NODE_ADDED, //当新的计算节点（NodeManager）加入 YARN 集群时触发
  NODE_REMOVED, //当节点从集群中移除时触发
  NODE_UPDATE, //当节点的状态（如心跳、资源使用情况）发生变化时触发
  NODE_RESOURCE_UPDATE, //当节点的资源（CPU、内存等）被更新时触发
  NODE_LABELS_UPDATE, //当节点的标签（如 GPU、HighMem）发生更新时触发
  NODE_ATTRIBUTES_UPDATE, //当节点的属性信息发生变化时触发

  // Source: RMApp 与应用程序 (Application) 相关的事件
  APP_ADDED, //当新的 YARN 应用被提交时触发
  APP_REMOVED, //当应用被完成（成功或失败）或被杀死时触发

  // Source: RMAppAttempt
  APP_ATTEMPT_ADDED,//当新的应用尝试（ApplicationAttempt）被创建时触发
  APP_ATTEMPT_REMOVED, //当应用尝试结束（成功或失败）时触发

  // Source: ContainerAllocationExpirer
  CONTAINER_EXPIRED, //当容器分配后长时间未被使用而超时时触发

  // Source: SchedulerAppAttempt::pullNewlyUpdatedContainer.
  RELEASE_CONTAINER, //当调度器需要释放某个已分配的容器时触发

  /* Source: SchedulingEditPolicy */
  KILL_RESERVED_CONTAINER, //当调度器决定终止一个预留容器时触发

  // Mark a container for preemption 与资源抢占 (Preemption) 相关的事件
  MARK_CONTAINER_FOR_PREEMPTION, //标记某个容器为“即将被抢占”状态，以便调度器可以在必要时回收资源

  // Mark a for-preemption container killable
  MARK_CONTAINER_FOR_KILLABLE,//标记某个容器为“可以被杀死”的状态

  // Cancel a killable container
  MARK_CONTAINER_FOR_NONKILLABLE, //取消某个容器的“可杀死”状态

  //Queue Management Change 与队列管理 (Queue Management) 相关的事件
  MANAGE_QUEUE, //触发队列管理操作（如动态调整队列大小）

  // Auto created queue, auto deletion check
  AUTO_QUEUE_DELETION //当队列满足自动删除条件时触发（例如应用退出后自动删除动态创建的队列）
}
