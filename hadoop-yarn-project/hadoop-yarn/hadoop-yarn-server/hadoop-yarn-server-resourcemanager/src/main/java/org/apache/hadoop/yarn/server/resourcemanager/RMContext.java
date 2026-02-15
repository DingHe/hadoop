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

import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.yarn.ams.ApplicationMasterServiceContext;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.ConfigurationProvider;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.nodelabels.NodeAttributesManager;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.SystemCredentialsForAppsProto;
import org.apache.hadoop.yarn.server.resourcemanager.ahs.RMApplicationHistoryWriter;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.SystemMetricsPublisher;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMDelegatedNodeLabelsUpdater;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSystem;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceProfilesManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.monitor.RMAppLifetimeMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.PlacementConstraintManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.AllocationTagsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.distributed.QueueLimitCalculator;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.MultiNodeSortingManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.AMRMTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.DelegationTokenRenewer;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.ProxyCAManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMDelegationTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.timelineservice.RMTimelineCollectorManager;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.VolumeManager;

/**
 * Context of the ResourceManager.
 */
//存储和管理资源管理器（ResourceManager）相关上下文信息的接口。
// 它提供了对资源管理器各个组件的访问接口，包括调度器、节点管理器、容器、应用程序以及安全和日志管理等。
// RMContext 为资源管理器提供了一个集中的上下文，允许各个模块和服务通过这个上下文与资源管理器进行交互，确保资源管理器的各项功能协调运作
public interface RMContext extends ApplicationMasterServiceContext {
  //获取分发器（Dispatcher），用于调度处理异步事件
  Dispatcher getDispatcher();
  //判断高可用（HA）是否启用，返回一个布尔值
  boolean isHAEnabled();
  //获取高可用服务的状态
  HAServiceState getHAServiceState();
  //获取资源管理器的状态存储（例如，保存资源管理器的持久化状态）
  RMStateStore getStateStore();
  //获取所有应用程序的映射
  ConcurrentMap<ApplicationId, RMApp> getRMApps();

  ConcurrentMap<ApplicationId, SystemCredentialsForAppsProto>
      getSystemCredentialsForApps();
  //获取所有非活动的资源管理器节点
  ConcurrentMap<NodeId, RMNode> getInactiveRMNodes();
  //获取所有活动的资源管理器节点
  ConcurrentMap<NodeId, RMNode> getRMNodes();
  //获取应用程序管理器（AM）存活监视器
  AMLivelinessMonitor getAMLivelinessMonitor();

  AMLivelinessMonitor getAMFinishingMonitor();
  //获取容器分配过期器
  ContainerAllocationExpirer getContainerAllocationExpirer();
  //获取委托令牌更新器
  DelegationTokenRenewer getDelegationTokenRenewer();
  //获取 AMRM 令牌的密钥管理器
  AMRMTokenSecretManager getAMRMTokenSecretManager();
  //获取容器令牌的密钥管理器
  RMContainerTokenSecretManager getContainerTokenSecretManager();
  //获取 NodeManager 令牌的密钥管理器
  NMTokenSecretManagerInRM getNMTokenSecretManager();
  //获取资源调度器
  ResourceScheduler getScheduler();

  NodesListManager getNodesListManager();

  ClientToAMTokenSecretManagerInRM getClientToAMTokenSecretManager();

  AdminService getRMAdminService();
  //获取客户端资源管理服务
  ClientRMService getClientRMService();

  ApplicationMasterService getApplicationMasterService();

  ResourceTrackerService getResourceTrackerService();

  void setClientRMService(ClientRMService clientRMService);

  RMDelegationTokenSecretManager getRMDelegationTokenSecretManager();

  void setRMDelegationTokenSecretManager(
      RMDelegationTokenSecretManager delegationTokenSecretManager);
  //取资源管理器应用程序历史记录写入器
  RMApplicationHistoryWriter getRMApplicationHistoryWriter();

  void setRMApplicationHistoryWriter(
      RMApplicationHistoryWriter rmApplicationHistoryWriter);

  void setSystemMetricsPublisher(SystemMetricsPublisher systemMetricsPublisher);

  SystemMetricsPublisher getSystemMetricsPublisher();

  void setRMTimelineCollectorManager(
      RMTimelineCollectorManager timelineCollectorManager);

  RMTimelineCollectorManager getRMTimelineCollectorManager();
  //获取配置提供者，用于管理配置
  ConfigurationProvider getConfigurationProvider();

  boolean isWorkPreservingRecoveryEnabled();
  //获取节点标签管理器
  RMNodeLabelsManager getNodeLabelManager();
  
  public void setNodeLabelManager(RMNodeLabelsManager mgr);

  NodeAttributesManager getNodeAttributesManager();

  void setNodeAttributesManager(NodeAttributesManager mgr);

  RMDelegatedNodeLabelsUpdater getRMDelegatedNodeLabelsUpdater();

  void setRMDelegatedNodeLabelsUpdater(
      RMDelegatedNodeLabelsUpdater nodeLabelsUpdater);

  long getEpoch();
  //获取预留系统，用于管理资源预留
  ReservationSystem getReservationSystem();

  boolean isSchedulerReadyForAllocatingContainers();
  
  Configuration getYarnConfiguration();
  
  PlacementManager getQueuePlacementManager();
  
  void setQueuePlacementManager(PlacementManager placementMgr);

  void setLeaderElectorService(EmbeddedElector elector);

  EmbeddedElector getLeaderElectorService();

  QueueLimitCalculator getNodeManagerQueueLimitCalculator();

  void setRMAppLifetimeMonitor(RMAppLifetimeMonitor rmAppLifetimeMonitor);

  RMAppLifetimeMonitor getRMAppLifetimeMonitor();
  //获取高可用 ZooKeeper 连接的状态
  String getHAZookeeperConnectionState();

  ResourceManager getResourceManager();

  ResourceProfilesManager getResourceProfilesManager();

  void setResourceProfilesManager(ResourceProfilesManager mgr);

  String getAppProxyUrl(Configuration conf, ApplicationId applicationId);

  AllocationTagsManager getAllocationTagsManager();

  void setAllocationTagsManager(AllocationTagsManager allocationTagsManager);

  PlacementConstraintManager getPlacementConstraintManager();

  void setPlacementConstraintManager(
      PlacementConstraintManager placementConstraintManager);

  MultiNodeSortingManager<SchedulerNode> getMultiNodeSortingManager();

  void setMultiNodeSortingManager(
      MultiNodeSortingManager<SchedulerNode> multiNodeSortingManager);

  ProxyCAManager getProxyCAManager();

  void setProxyCAManager(ProxyCAManager proxyCAManager);

  VolumeManager getVolumeManager();

  void setVolumeManager(VolumeManager volumeManager);

  long getTokenSequenceNo();

  void incrTokenSequenceNo();
}
