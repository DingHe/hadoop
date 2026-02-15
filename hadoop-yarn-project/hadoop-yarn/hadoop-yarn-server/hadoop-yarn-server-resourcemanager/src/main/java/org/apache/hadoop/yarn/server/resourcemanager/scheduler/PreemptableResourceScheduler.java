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

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;

/**
 * Interface for a scheduler that supports preemption/killing
 *
 */
//资源调度器接口，用于 支持资源抢占（Preemption）和任务终止（Killing）。
// 它继承了 ResourceScheduler，扩展了 资源管理器（ResourceManager） 的功能，使其能够：
// 取消某个容器的预留（killReservedContainer）。
// 向特定应用程序请求抢占（markContainerForPreemption）。
// 直接标记某个容器为可被终止（markContainerForKillable）
public interface PreemptableResourceScheduler extends ResourceScheduler {

  /**
   * If the scheduler support container reservations, this method is used to
   * ask the scheduler to drop the reservation for the given container.
   * @param container Reference to reserved container allocation.
   */
  //container：RMContainer 类型，表示一个已经预留（Reserved）的容器
  //如果调度器支持 容器预留（Reservation），此方法用于 取消该容器的预留，释放资源供其他应用使用
  void killReservedContainer(RMContainer container);

  /**
   * Ask the scheduler to obtain back the container from a specific application
   * by issuing a preemption request
   * @param aid the application from which we want to get a container back
   * @param container the container we want back
   */
  //aid：ApplicationAttemptId，表示要抢占资源的 应用尝试 ID
  //container：RMContainer，表示需要被抢占的 容器。
  //这个方法用于 请求从特定应用程序抢占资源
  void markContainerForPreemption(ApplicationAttemptId aid, RMContainer container);

  /**
   * Ask the scheduler to forcibly interrupt the container given as input.
   * @param container RMContainer.
   */
  //container：RMContainer，表示 需要被标记为可终止 的容器
  //该方法 通知调度器，某个容器可以被 强制终止（kill）
  void markContainerForKillable(RMContainer container);

}
