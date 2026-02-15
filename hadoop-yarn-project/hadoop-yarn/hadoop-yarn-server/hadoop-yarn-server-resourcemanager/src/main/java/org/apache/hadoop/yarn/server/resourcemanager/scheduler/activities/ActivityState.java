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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities;

/*
 * Collection of activity operation states.
 */
// 用于定义 YARN 资源管理器在资源调度过程中，某个活动（Activity）可能处于的不同状态。
// 它主要用于 资源分配的决策记录，帮助追踪资源调度的不同阶段，包括资源的成功分配、被跳过、被拒绝、被保留等情况
public enum ActivityState {
  // default state when adding a new activity in node allocation
  DEFAULT, //表示新创建的调度活动，在节点分配时尚未进入具体的分配逻辑
  // container is allocated to sub-queues/applications or this queue/application
  ACCEPTED, //资源已被接受，并可能会分配到子队列或应用程序，或者直接由当前队列/应用程序使用。
  // queue or application voluntarily give up to use the resource OR
  // nothing allocated
  SKIPPED, //资源被主动放弃使用，或者当前尝试没有分配到任何资源。
  // container could not be allocated to sub-queues or this application
  REJECTED,  //资源分配尝试失败，无法将容器分配给子队列或应用程序。
  ALLOCATED, // successfully allocate a new non-reserved container 资源成功分配给了应用，并创建了一个新的 非保留 容器（non-reserved container）
  RESERVED,  // successfully reserve a new container  资源被成功 预留，表示该容器将在后续尝试中使用，而不是立即分配。
  RE_RESERVED  // successfully reserve a new container  资源被 重新预留，通常用于已经预留过的容器，再次被确认保留。
}
