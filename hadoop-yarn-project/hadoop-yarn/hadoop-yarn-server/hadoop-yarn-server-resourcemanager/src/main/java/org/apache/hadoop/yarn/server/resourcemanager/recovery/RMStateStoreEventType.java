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

package org.apache.hadoop.yarn.server.resourcemanager.recovery;
//与 ResourceManager 状态存储系统（RMStateStore）的操作相关。每个事件代表一个特定的操作，用于操作和管理资源管理器的状态存储
public enum RMStateStoreEventType {
  STORE_APP_ATTEMPT,//表示存储应用尝试（应用程序尝试）。这个事件通常在应用尝试的状态存储时触发
  STORE_APP,//表示存储应用（应用程序）。该事件触发时表示需要将应用程序的状态存储在系统中
  UPDATE_APP,//表示更新应用。此事件触发时需要更新某个应用程序的状态信息
  UPDATE_APP_ATTEMPT,//表示更新应用尝试。这个事件通常用于更新应用程序的某次尝试状态
  REMOVE_APP,//表示移除应用。该事件触发时表示从状态存储中删除某个应用程序的状态
  REMOVE_APP_ATTEMPT,//表示移除应用尝试。此事件会删除某个应用程序尝试的状态信息
  FENCED,//表示 RMStateStore 处于被围栏（禁用）状态。这个事件表示状态存储不可用，无法进行进一步的操作

  // Below events should be called synchronously
  //同步事件：以下事件应当同步执行，因为它们涉及到关键的资源管理和安全信息的存储和移除
  STORE_MASTERKEY,//表示存储主密钥。此事件涉及将主密钥信息存储到状态存储中
  REMOVE_MASTERKEY,//表示移除主密钥。此事件触发时需要从状态存储中删除主密钥
  STORE_DELEGATION_TOKEN,//表示存储委托令牌。此事件会将委托令牌存储到状态存储中
  REMOVE_DELEGATION_TOKEN,//表示移除委托令牌。此事件触发时需要从状态存储中删除委托令牌
  UPDATE_DELEGATION_TOKEN,//表示更新委托令牌。此事件用于更新现有的委托令牌信息
  UPDATE_AMRM_TOKEN,//表示更新 AMRM 令牌。该事件会更新与 AMRM（ApplicationMaster ResourceManager）相关的令牌信息
  STORE_RESERVATION,//表示存储保留资源的状态。此事件将保留资源信息存储到状态存储中
  REMOVE_RESERVATION,//表示移除保留资源的状态。该事件会删除资源保留的相关信息
  STORE_PROXY_CA_CERT,//表示存储代理 CA 证书。此事件将代理 CA 证书存储到状态存储中
}
