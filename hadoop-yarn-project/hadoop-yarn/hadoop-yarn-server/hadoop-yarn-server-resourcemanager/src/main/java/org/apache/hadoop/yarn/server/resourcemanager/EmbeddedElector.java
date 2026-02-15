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
package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.service.Service;

/** 主要用于 领导者选举（Leader Election），通常结合 Zookeeper 进行管理
 * Interface that all embedded leader electors must implement.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface EmbeddedElector extends Service{
  /** 该方法用于 退出并重新加入领导者选举
   * Leave and rejoin leader election.
   */
  void rejoinElection();

  /**
   * Get information about the elector's connection to Zookeeper.
   * 该方法返回 Zookeeper 连接状态 可能的状态包括：
   * CONNECTED
   * DISCONNECTED
   * RECONNECTING
   * SUSPENDED
   * @return zookeeper connection state
   */
  String getZookeeperConnectionState();
}
