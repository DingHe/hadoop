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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ContainerRequest;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;


/**
 * Represents the ResourceManager's view of an application container. See 
 * {@link RMContainerImpl} for an implementation. Containers may be in one
 * of several states, given in {@link RMContainerState}. An RMContainer
 * instance may exist even if there is no actual running container, such as 
 * when resources are being reserved to fill space for a future container 
 * allocation.
 */
//RMContainer 类的作用是为 YARN 集群中的每个容器提供一个管理接口，记录容器的分配、状态、资源等信息。
// 该类可以存在即使容器没有实际运行，例如当资源预留用于未来的容器分配时
public interface RMContainer extends EventHandler<RMContainerEvent>,
    Comparable<RMContainer> {
  //容器 ID，唯一标识容器
  ContainerId getContainerId();

  void setContainerId(ContainerId containerId);
  //应用尝试 ID，标识属于哪个应用尝试
  ApplicationAttemptId getApplicationAttemptId();
  //容器状态，表示当前容器的生命周期状态（例如，运行、预留、失败等）
  RMContainerState getState();
  //返回容器对象，代表容器的详细信息
  Container getContainer();
  //预留的资源，表示容器所需的预留资源
  Resource getReservedResource();
  //预留的节点 ID，表示容器被分配的节点
  NodeId getReservedNode();
  //预留调度器键，唯一标识容器在调度器中的请求
  SchedulerRequestKey getReservedSchedulerKey();
  //分配的资源，表示容器当前的已分配资源
  Resource getAllocatedResource();
  //最后确认的资源，表示最后一次确认的容器资源
  Resource getLastConfirmedResource();
  //分配的节点 ID，容器被调度到的节点
  NodeId getAllocatedNode();
  //分配的调度器键，标识容器的调度请求。
  SchedulerRequestKey getAllocatedSchedulerKey();
  //分配的优先级，容器在资源调度中的优先级
  Priority getAllocatedPriority();
  //创建时间，容器的创建时间戳。
  long getCreationTime();
  //结束时间，容器完成或失败的时间戳。
  long getFinishTime();
  //诊断信息，容器的错误或调试信息。
  String getDiagnosticsInfo();
  //日志 URL，容器日志的访问链接。
  String getLogURL();
  //容器退出状态，容器运行结束时的退出状态码。
  int getContainerExitStatus();
  //容器状态，描述容器当前的运行状态。
  ContainerState getContainerState();
   //创建并返回一个 ContainerReport 对象，包含容器的详细信息。
  ContainerReport createContainerReport();
  //判断容器是否为 Application Master (AM) 容器。
  boolean isAMContainer();
  //返回与容器相关的调度请求。
  ContainerRequest getContainerRequest();
 //节点 HTTP 地址，容器所在节点的 HTTP 地址。
  String getNodeHttpAddress();
  //暴露的端口信息，表示容器的端口暴露情况。
  Map<String, List<Map<String, String>>> getExposedPorts();
  //设置容器的暴露端口信息。
  void setExposedPorts(Map<String, List<Map<String, String>>> exposed);
   //节点标签表达式，描述容器的节点约束条件。
  String getNodeLabelExpression();
  //队列名称，容器所在的 YARN 队列名称。
  String getQueueName();
  //执行类型，指明容器的执行模式（如保证执行模式或者机会执行）。
  ExecutionType getExecutionType();

  /**
   * If the container was allocated by a container other than the Resource
   * Manager (e.g., the distributed scheduler in the NM
   * <code>LocalScheduler</code>).
   * @return If the container was allocated remotely.
   */
  //判断容器是否由远程分配的资源管理器分配。
  boolean isRemotelyAllocated();

  /*
   * Return reserved resource for reserved containers, return allocated resource
   * for other container
   */
  //获取已分配或已预留的资源。
  Resource getAllocatedOrReservedResource();
  //判断容器是否已完成。
  boolean completed();
  //获取容器所在的节点 ID。
  NodeId getNodeId();

  /**
   * Return {@link SchedulingRequest#getAllocationTags()} specified by AM.
   * @return allocation tags, could be null/empty
   */
  //获取应用程序指定的分配标签。
  Set<String> getAllocationTags();
}
