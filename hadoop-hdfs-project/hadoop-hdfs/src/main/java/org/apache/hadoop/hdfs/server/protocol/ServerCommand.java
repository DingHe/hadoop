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
package org.apache.hadoop.hdfs.server.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Base class for a server command.
 * Issued by the name-node to notify other servers what should be done.
 * Commands are defined by actions defined in respective protocols.
 * 
 * @see DatanodeProtocol
 * @see NamenodeProtocol
 */
// 定义了 NameNode 向集群中其他服务器（主要是 DataNode）下达指令的通用模型。
// 在 HDFS 的主从架构中，NameNode 通常不会主动通过网络“推送”指令给 DataNode，而是通过“响应”机制：
// 心跳响应 (Heartbeat Response)：DataNode 定期向 NameNode 发送心跳，NameNode 在心跳的返回值中携带一组指令。
// 协议支撑：这些指令涵盖了数据块复制、块删除、数据恢复、重新注册等所有管理动作。
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class ServerCommand {
  // 存储该指令的具体动作编号（ID）。
  private final int action;

  /**
   * Create a command for the specified action.
   * Actions are protocol specific.
   * 
   * @see DatanodeProtocol
   * @see NamenodeProtocol
   * @param action protocol specific action
   */
  public ServerCommand(int action) {
    this.action = action;
  }

  /**
   * Get server command action.
   * @return action code.
   */
  public int getAction() {
    return this.action;
  }

  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName())
        .append("/")
        .append(action);
    return sb.toString();
  }
}
