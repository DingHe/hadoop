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
// 应用尝试（Application Attempt） 的不同状态

public enum RMAppAttemptState {
  NEW,//新建状态，应用尝试刚刚被创建，尚未提交给调度器。
  SUBMITTED, //已提交状态，应用尝试已提交给资源管理器，但尚未被调度。
  SCHEDULED, //已调度状态，应用尝试被调度器接收，正在等待资源分配。
  ALLOCATED,//已分配状态，调度器已经为应用尝试分配了容器（Container），但尚未启动。
  LAUNCHED, //已启动状态，应用尝试的主容器（AM 容器）已经启动。
  FAILED, //失败状态，应用尝试由于某些原因失败，可能会有新的尝试被创建。
  RUNNING,//运行中状态，应用尝试已经成功启动，并且正在执行任务
  FINISHING,//正在完成状态，应用尝试即将完成，可能在做一些清理工作
  FINISHED,//已完成状态，应用尝试成功完成并终止
  KILLED,//被杀死状态，应用尝试被外部或 RM 终止，可能是由于用户取消、资源不足等原因
  ALLOCATED_SAVING,//分配后保存状态，应用尝试在资源分配后，正在进行一些持久化存储的操作
  LAUNCHED_UNMANAGED_SAVING,//启动非托管 AM 后的保存状态，对于非托管的应用程序（Unmanaged AM），在 AM 启动后进行持久化存储操作
  FINAL_SAVING //最终保存状态，应用尝试进入终止前的最终状态，正在执行最后的存储操作
}
