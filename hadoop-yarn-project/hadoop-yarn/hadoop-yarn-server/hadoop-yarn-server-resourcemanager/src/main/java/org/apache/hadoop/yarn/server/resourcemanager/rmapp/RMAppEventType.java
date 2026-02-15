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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp;
//表示资源管理器（ResourceManager，简称 RM）中应用程序的不同事件类型
public enum RMAppEventType {
  // Source: ClientRMService
  START, //代表应用程序启动事件，通常由客户端发起，表示应用程序的启动请求
  RECOVER,//代表应用程序恢复事件，通常用于应用程序从持久化状态中恢复时触发
  KILL,//代表应用程序被终止事件，表示应用程序请求终止或被强制终止

  // Source: Scheduler and RMAppManager
  APP_REJECTED,//代表应用程序被调度器拒绝事件，通常当资源不可用或不满足调度器的策略时触发

  // Source: Scheduler
  APP_ACCEPTED,//代表应用程序被调度器接受事件，表示应用程序的提交已被调度器接受并准备开始执行

  // Source: RMAppAttempt
  ATTEMPT_REGISTERED,//代表应用程序的一个尝试（RMAppAttempt）被注册事件，表示新的尝试已经开始
  ATTEMPT_UNREGISTERED,//代表应用程序的一个尝试（RMAppAttempt）被注销事件，表示一个尝试结束并从 ResourceManager 注销
  ATTEMPT_FINISHED, // 代表应用程序的尝试（RMAppAttempt）完成事件，通常在应用程序尝试的执行完成时触发，可能是成功或失败
  ATTEMPT_FAILED,//代表应用程序尝试（RMAppAttempt）失败事件，表示该尝试未能成功完成
  ATTEMPT_KILLED,//代表应用程序尝试（RMAppAttempt）被杀死事件，通常是由于用户请求或调度器策略
  NODE_UPDATE,//代表节点的更新事件，通常由节点状态更新引起，例如节点宕机或恢复
  ATTEMPT_LAUNCHED,//代表应用程序的尝试（RMAppAttempt）已启动事件，表示应用程序的尝试容器已经成功启动
  
  // Source: Container and ResourceTracker
  APP_RUNNING_ON_NODE,//代表应用程序正在特定节点上运行事件，表示容器在某个节点上启动并运行

  // Source: RMStateStore
  APP_NEW_SAVED,//代表应用程序的新状态已保存事件，通常由资源管理器状态存储（RMStateStore）触发，表示应用程序的初始状态已被持久化
  APP_UPDATE_SAVED,//代表应用程序的状态更新已保存事件，表示应用程序状态的更改已被成功保存
  APP_SAVE_FAILED,//代表应用程序状态保存失败事件，表示资源管理器在尝试保存应用程序的状态时出现错误
}
