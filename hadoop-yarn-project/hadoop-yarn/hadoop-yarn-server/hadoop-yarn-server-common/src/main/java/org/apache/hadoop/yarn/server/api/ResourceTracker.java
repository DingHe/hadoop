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
package org.apache.hadoop.yarn.server.api;

import java.io.IOException;

import org.apache.hadoop.io.retry.AtMostOnce;
import org.apache.hadoop.io.retry.Idempotent;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerResponse;

/**
 * This is used by the Node Manager to register/nodeHeartbeat/unregister with
 * the ResourceManager.
 */
//NodeManager（NM）与 ResourceManager（RM）通信的接口，用于：
//注册 (registerNodeManager)：NM 启动后向 RM 注册，以加入集群。
//心跳 (nodeHeartbeat)：NM 定期向 RM 发送心跳，报告资源使用情况、运行状态等。
//注销 (unRegisterNodeManager)：NM 关闭或被移除时，通知 RM 进行资源清理
public interface ResourceTracker {
  //request：RegisterNodeManagerRequest，包含 NM 的注册信息，例如：
  //NodeId（节点 ID）
  //Resource（节点资源情况）
  //NMContainerStatus（节点上运行的容器状态）
  @Idempotent
  RegisterNodeManagerResponse registerNodeManager(
      RegisterNodeManagerRequest request) throws YarnException, IOException;
  //request：NodeHeartbeatRequest，包含 NM 的状态信息，如：
  //NodeId（节点 ID）
  //ContainerStatus（容器运行状态）
  //NodeHealthStatus（节点健康情况）
  //LastKnownRMContainer（上次报告的容器情况）
  @AtMostOnce
  NodeHeartbeatResponse nodeHeartbeat(NodeHeartbeatRequest request)
      throws YarnException, IOException;
  //request：UnRegisterNodeManagerRequest，包含需要注销的 NM 信息
  @Idempotent
  UnRegisterNodeManagerResponse unRegisterNodeManager(
      UnRegisterNodeManagerRequest request) throws YarnException, IOException;
}
