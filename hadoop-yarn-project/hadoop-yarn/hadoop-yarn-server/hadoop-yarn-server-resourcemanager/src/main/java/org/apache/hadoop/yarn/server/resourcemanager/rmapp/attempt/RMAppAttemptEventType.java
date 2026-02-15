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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt;
//定义 应用尝试（Application Attempt） 在其生命周期中可能发生的各种事件类型
public enum RMAppAttemptEventType {
  // Source: RMApp
  START, //应用尝试启动的事件
  KILL, //请求终止该应用尝试的事件
  FAIL, //应用尝试失败的事件

  // Source: AMLauncher
  LAUNCHED, //AM（Application Master）已成功启动的事件，通常由 AMLauncher 触发
  LAUNCH_FAILED, //AM 启动失败的事件，意味着 AMLauncher 可能遇到了异常，例如资源不足或节点故障

  // Source: AMLivelinessMonitor
  EXPIRE, //AM 超时未发送心跳而被认定为失活的事件，通常意味着 AM 崩溃或长时间未响应
  
  // Source: ApplicationMasterService
  REGISTERED, //AM 成功向 ApplicationMasterService 注册的事件，意味着 AM 可以开始向 YARN 申请资源
  STATUS_UPDATE, //AM 向 ApplicationMasterService 发送状态更新信息的事件，例如应用的进度或任务状态变化
  UNREGISTERED,//AM 向 ApplicationMasterService 取消注册的事件，通常发生在应用尝试结束时（成功或失败）

  // Source: Containers
  CONTAINER_ALLOCATED, //AM 成功获得 YARN 分配的容器，该容器可以用于执行任务
  CONTAINER_FINISHED, //ARN 发现某个容器已完成执行，该事件可能由 NodeManager 或 ResourceManager 触发
  
  // Source: RMStateStore
  ATTEMPT_NEW_SAVED, //应用尝试的初始状态已成功持久化到 RMStateStore，表示该尝试的信息已存入持久化存储（如 HDFS）
  ATTEMPT_UPDATE_SAVED, //应用尝试的状态已成功更新到 RMStateStore，例如 AM 重新启动后恢复状态时会触发

  // Source: Scheduler
  ATTEMPT_ADDED, //应用尝试被调度器识别并添加到调度队列，意味着该尝试可以开始请求资源
  
  // Source: RMAttemptImpl.recover
  RECOVER //应用尝试从持久化存储中恢复的事件，例如 ResourceManager 发生故障重启后，尝试重新加载应用状态

}
