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

package org.apache.hadoop.yarn.server.resourcemanager.rmcontainer;
//表示 RMContainer (资源管理容器) 在其生命周期中可能发生的事件类型
public enum RMContainerEventType {

  // Source: SchedulerApp
  //容器启动事件，表示 RMContainer 开始其生命周期
  START,
  //容器被 ApplicationMaster (AM) 获取，资源管理器已经成功分配该容器
  ACQUIRED,
  //触发容器的终止，可能是 ApplicationMaster 主动释放资源，或者 NodeManager 因节点失效触发容器终止
  KILL, // Also from Node on NodeRemoval
  //容器被预留给一个应用，但尚未正式分配，可用于抢占机制
  RESERVED,
  
  // when a container acquired by AM after
  // it increased/decreased
  //容器的资源在 AM 请求后被增加或减少，AM 重新获取该容器
  ACQUIRE_UPDATED_CONTAINER, 
  //容器已经在 NodeManager 上成功启动
  LAUNCHED,
  //容器正常完成执行，并返回了退出状态。
  FINISHED,

  // Source: ApplicationMasterService->Scheduler
  //ApplicationMaster 释放了该容器，意味着 AM 不再使用此容器，容器可被回收。
  RELEASED,

  // Source: ContainerAllocationExpirer
  //容器的分配超时，没有被 AM 及时使用，导致回收。
  EXPIRE,
  //资源管理器在重启后，尝试恢复之前的容器信息。
  RECOVER,
  
  // Source: Scheduler
  // Resource change approved by scheduler
  //调度器批准了容器资源变更请求，例如 CPU 或内存的扩展或缩减。
  CHANGE_RESOURCE,
  
  // NM reported resource change is done
  //NodeManager 确认已经完成资源变更请求，表明该容器的资源调整已生效。
  NM_DONE_CHANGE_RESOURCE 
}
