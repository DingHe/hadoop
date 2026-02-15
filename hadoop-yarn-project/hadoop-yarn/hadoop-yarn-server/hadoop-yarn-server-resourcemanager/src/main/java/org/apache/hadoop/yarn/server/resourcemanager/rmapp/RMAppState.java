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
//表示应用程序的不同状态。它用于跟踪一个应用程序在资源管理器（ResourceManager）生命周期中的状态变化
public enum RMAppState {
  NEW, //应用程序刚被提交到资源管理器，尚未开始任何处理
  NEW_SAVING, //应用程序在提交时，正在保存相关的初始化信息，以便后续的调度和处理
  SUBMITTED, //应用程序已被提交，等待资源调度
  ACCEPTED, //资源管理器已经接受应用程序的提交，并且正在为其分配资源，等待启动
  RUNNING, //应用程序已成功启动，正在运行中
  FINAL_SAVING, //应用程序运行完毕，但正在保存其最终状态和信息，准备结束
  FINISHING, //应用程序正在结束，所有资源正在释放并进行最终清理
  FINISHED, //应用程序已经成功完成，并且所有相关的资源和状态已经清理完毕
  FAILED, //应用程序执行失败，可能因为资源不足、错误配置或其他原因
  KILLING, //应用程序正在被杀死，通常是在应用程序请求停止或由于某种故障需要终止
  KILLED //应用程序已经被成功杀死，所有相关资源和状态已被清理
}
