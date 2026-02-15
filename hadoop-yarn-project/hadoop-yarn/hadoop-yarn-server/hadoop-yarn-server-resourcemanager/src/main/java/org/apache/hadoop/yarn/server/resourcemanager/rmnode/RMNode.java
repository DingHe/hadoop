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

package org.apache.hadoop.yarn.server.resourcemanager.rmnode;


import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.net.Node;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.OpportunisticContainersStatus;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * Node managers information on available resources 
 * and other static information.
 *
 */
// 每个节点的相关信息，主要是NodeManager的资源管理和节点健康状况的相关数据。
// 它为ResourceManager提供节点的资源、状态、健康报告等信息，并处理节点心跳、资源更新等事件
public interface RMNode {

  /**获取节点的唯一标识符（NodeId）
   * the node id of of this node.
   * @return the node id of this node.
   */
  public NodeId getNodeID();
  
  /**获取节点的主机名
   * the hostname of this node
   * @return hostname of this node
   */
  public String getHostName();
  
  /**获取节点的命令端口
   * the command port for this node
   * @return command port for this node
   */
  public int getCommandPort();
  
  /**获取节点的HTTP端口
   * the http port for this node
   * @return http port for this node
   */
  public int getHttpPort();


  /**获取节点的ContainerManager地址
   * the ContainerManager address for this node.
   * @return the ContainerManager address for this node.
   */
  public String getNodeAddress();
  
  /**获取节点的HTTP URL地址
   * the http-Address for this node.
   * @return the http-url address for this node
   */
  public String getHttpAddress();
  
  /**获取节点的最新健康报告
   * the latest health report received from this node.
   * @return the latest health report received from this node.
   */
  public String getHealthReport();
  
  /**获取最新健康报告的时间戳
   * the time of the latest health report received from this node.
   * @return the time of the latest health report received from this node.
   */
  public long getLastHealthReportTime();

  /**获取NodeManager的版本信息
   * the node manager version of the node received as part of the
   * registration with the resource manager.
   * @return node manager version.
   */
  public String getNodeManagerVersion();

  /**获取节点的总可用资源（CPU、内存等）
   * the total available resource.
   * @return the total available resource.
   */
  public Resource getTotalCapability();

  /**
   * The total allocated resources to containers.
   * This will include the sum of Guaranteed and Opportunistic
   * containers queued + running + paused on the node.
   * @return the total allocated resources, including all Guaranteed and
   * Opportunistic containers in queued, running and paused states.
   */
  //获取分配给容器的总资源，包括正在运行、排队和暂停的容器资源
  default Resource getAllocatedContainerResource() {
    return Resources.none();
  }

  /**判断节点的资源是否有更新
   * If the total available resources has been updated.
   * @return If the capability has been updated.
   */
  boolean isUpdatedCapability();

  /**标记资源更新事件已经处理
   * Mark that the updated event has been processed.
   */
  void resetUpdatedCapability();

  /**获取所有容器的资源使用情况的聚合
   * the aggregated resource utilization of the containers.
   * @return the aggregated resource utilization of the containers.
   */
  public ResourceUtilization getAggregatedContainersUtilization();

  /**获取节点的总资源使用情况
   * the total resource utilization of the node.
   * @return the total resource utilization of the node.
   */
  public ResourceUtilization getNodeUtilization();

  /** 获取节点的物理资源（实际的硬件资源）
   * the physical resources in the node.
   * @return the physical resources in the node.
   */
  Resource getPhysicalResource();

  /** 获取节点所在的机架名
   * The rack name for this node manager.
   * @return the rack name.
   */
  public String getRackName();
  
  /** 获取该节点的Node对象，包含了节点的其他信息
   * the {@link Node} information for this node.
   * @return {@link Node} information for this node.
   */
  public Node getNode();
  //获取节点的当前状态（如RUNNING、DECOMMISSIONED等）
  public NodeState getState();
  //获取需要清理的容器列表
  public List<ContainerId> getContainersToCleanUp();
  //获取需要清理的应用程序列表
  public List<ApplicationId> getAppsToCleanup();
  //获取当前运行的应用程序列表
  List<ApplicationId> getRunningApps();

  /**
   * Update a {@link NodeHeartbeatResponse} with the list of containers and
   * applications to clean up for this node, and the containers to be updated.
   * 更新节点心跳响应，包含清理的容器和应用信息
   * @param response the {@link NodeHeartbeatResponse} to update
   */
  void setAndUpdateNodeHeartbeatResponse(NodeHeartbeatResponse response);
  //获取节点的最后一次心跳响应
  public NodeHeartbeatResponse getLastNodeHeartBeatResponse();

  /** 重置最后一次心跳响应的ID为0
   * Reset lastNodeHeartbeatResponse's ID to 0.
   */
  void resetLastNodeHeartBeatResponse();

  /**
   * Get and clear the list of containerUpdates accumulated across NM
   * heartbeats.
   *  获取并清除通过节点心跳积累的容器更新信息
   * @return containerUpdates accumulated across NM heartbeats.
   */
  public List<UpdatedContainerInfo> pullContainerUpdates();
  
  /**
   * Get set of labels in this node
   * 获取节点的标签集合
   * @return labels in this node
   */
  public Set<String> getNodeLabels();
  //获取新增的容器列表
  public List<Container> pullNewlyIncreasedContainers();
  //获取节点上机会性容器的状态
  OpportunisticContainersStatus getOpportunisticContainersStatus();
  //获取节点未跟踪的时间戳
  long getUntrackedTimeStamp();
  //设置节点未跟踪的时间戳
  void setUntrackedTimeStamp(long timeStamp);
  /* 获取可选的去激活超时值（秒），如果没有则返回null
   * Optional decommissioning timeout in second
   * (null indicates absent timeout).
   * @return the decommissioning timeout in second.
   */
  Integer getDecommissioningTimeout();

  /** 获取节点的分配标签及其计数的映射
   * Get the allocation tags and their counts associated with this node.
   * @return a map of each allocation tag and its count.
   */
  Map<String, Long> getAllocationTagsWithCount();

  /** 获取与此节点关联的RMContext
   * @return the RM context associated with this RM node.
   */
  RMContext getRMContext();

  /**获取该节点的所有属性
   * @return all node attributes as a Set.
   */
  Set<NodeAttribute> getAllNodeAttributes();
  //计算节点心跳的间隔时间，根据不同因素调整心跳频率
  long calculateHeartBeatInterval(long defaultInterval,
      long minInterval, long maxInterval, float speedupFactor,
      float slowdownFactor);
}
