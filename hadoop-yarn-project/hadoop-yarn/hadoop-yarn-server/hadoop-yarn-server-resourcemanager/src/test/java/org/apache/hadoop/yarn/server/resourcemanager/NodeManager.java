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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.protocolrecords.CommitResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ContainerUpdateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ContainerUpdateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetLocalizationStatusesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetLocalizationStatusesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.IncreaseContainersResourceRequest;
import org.apache.hadoop.yarn.api.protocolrecords.IncreaseContainersResourceResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReInitializeContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReInitializeContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceLocalizationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceLocalizationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RestartContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RollbackResponse;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.YarnVersionInfo;
import org.apache.hadoop.yarn.util.resource.Resources;
// 负责管理和监控每个节点（Node）的资源和容器（Container）。
// 它处理来自 ResourceManager 的请求，启动、停止、更新容器，维护资源使用情况，
// 并周期性地向 ResourceManager 发送心跳信号以报告节点的健康状态和资源状态
@Private
public class NodeManager implements ContainerManagementProtocol {
  private static final Logger LOG =
      LoggerFactory.getLogger(NodeManager.class);
  private static final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  //容器管理器的地址，包含主机名和端口号
  final private String containerManagerAddress;
  //节点的 HTTP 地址，通常用于与客户端进行交互
  final private String nodeHttpAddress;
  //节点所在的机架名称
  final private String rackName;
  //节点的唯一标识符，包含主机名和端口号
  final private NodeId nodeId;
  //节点的资源能力（如 CPU、内存）
  final private Resource capability;
  //与该节点关联的 ResourceManager 实例
  final private ResourceManager resourceManager;
  //当前节点上可用的资源量
  Resource available = recordFactory.newRecordInstance(Resource.class);
  //当前节点上已用的资源量
  Resource used = recordFactory.newRecordInstance(Resource.class);
  //与 ResourceManager 通信的服务，用于注册节点和发送心跳等
  final ResourceTrackerService resourceTrackerService;
  //存储每个应用程序的容器列表
  final Map<ApplicationId, List<Container>> containers = 
    new HashMap<ApplicationId, List<Container>>();
  //存储每个容器的状态
  final Map<Container, ContainerStatus> containerStatusMap =
      new HashMap<Container, ContainerStatus>();
  
  public NodeManager(String hostName, int containerManagerPort, int httpPort,
      String rackName, Resource capability,
      ResourceManager resourceManager, NodeStatus nodestatus)
      throws IOException, YarnException {
    this.containerManagerAddress = hostName + ":" + containerManagerPort;
    this.nodeHttpAddress = hostName + ":" + httpPort;
    this.rackName = rackName;
    this.resourceTrackerService = resourceManager.getResourceTrackerService();
    this.capability = capability;
    Resources.addTo(available, capability);
    this.nodeId = NodeId.newInstance(hostName, containerManagerPort);
    RegisterNodeManagerRequest request = recordFactory
        .newRecordInstance(RegisterNodeManagerRequest.class);
    request.setHttpPort(httpPort);
    request.setResource(capability);
    request.setNodeId(this.nodeId);
    request.setNMVersion(YarnVersionInfo.getVersion());
    request.setNodeStatus(nodestatus);
    resourceTrackerService.registerNodeManager(request);
    this.resourceManager = resourceManager;
    resourceManager.getResourceScheduler().getNodeReport(this.nodeId);
  }
  
  public String getHostName() {
    return containerManagerAddress;
  }

  public String getRackName() {
    return rackName;
  }

  public NodeId getNodeId() {
    return nodeId;
  }

  public Resource getCapability() {
    return capability;
  }

  public Resource getAvailable() {
    return available;
  }
  
  public Resource getUsed() {
    return used;
  }
  
  int responseID = 0;
  //返回该节点所有容器的状态
  private List<ContainerStatus> getContainerStatuses(Map<ApplicationId, List<Container>> containers) {
    List<ContainerStatus> containerStatuses = new ArrayList<ContainerStatus>();
    for (List<Container> appContainers : containers.values()) {
      for (Container container : appContainers) {
        containerStatuses.add(containerStatusMap.get(container));
      }
    }
    return containerStatuses;
  }
  //向 ResourceManager 发送一个心跳请求，定期报告当前节点的状态，包括节点上运行的容器的状态
  public void heartbeat() throws IOException, YarnException {
    NodeStatus nodeStatus = 
      org.apache.hadoop.yarn.server.resourcemanager.NodeManager.createNodeStatus(
          nodeId,
          getContainerStatuses(containers));//返回该节点上所有容器的状态
    nodeStatus.setResponseId(responseID);
    //创建并发送心跳请求
    NodeHeartbeatRequest request = recordFactory
        .newRecordInstance(NodeHeartbeatRequest.class);
    request.setNodeStatus(nodeStatus);
    NodeHeartbeatResponse response = resourceTrackerService
        .nodeHeartbeat(request);
    responseID = response.getResponseId();
  }
  //主要目的是启动容器。它接收一个包含多个容器启动请求的对象，并处理这些请求，最终启动相应的容器并更新节点的资源使用情况
  @Override
  synchronized public StartContainersResponse startContainers(
      StartContainersRequest requests) 
  throws YarnException {
    //遍历每个容器启动请求
    for (StartContainerRequest request : requests.getStartContainerRequests()) {
      //从请求中提取 containerToken
      Token containerToken = request.getContainerToken();
      ContainerTokenIdentifier tokenId = null;

      try {
        tokenId = BuilderUtils.newContainerTokenIdentifier(containerToken);
      } catch (IOException e) {
        throw RPCUtil.getRemoteException(e);
      }
      //获取容器的 ID 和对应的应用 ID
      ContainerId containerID = tokenId.getContainerID();
      ApplicationId applicationId =
          containerID.getApplicationAttemptId().getApplicationId();
      //获取该应用的容器列表 applicationContainers，如果没有则创建一个新的列表，并将其添加到 containers map 中
      List<Container> applicationContainers = containers.get(applicationId);
      if (applicationContainers == null) {
        applicationContainers = new ArrayList<Container>();
        containers.put(applicationId, applicationContainers);
      }

      // Sanity check
      //检查容器是否已经存在
      for (Container container : applicationContainers) {
        if (container.getId().compareTo(containerID) == 0) {
          throw new IllegalStateException("Container " + containerID
              + " already setup on node " + containerManagerAddress);
        }
      }
      //创建新的容器对象
      Container container =
          BuilderUtils.newContainer(containerID, this.nodeId, nodeHttpAddress,
            tokenId.getResource(), null, null // DKDC - Doesn't matter
            );
      //创建容器状态对象:
      ContainerStatus containerStatus =
          BuilderUtils.newContainerStatus(container.getId(),
            ContainerState.NEW, "", -1000, container.getResource());
      applicationContainers.add(container);
      containerStatusMap.put(container, containerStatus);
      //更新资源使用情况
      Resources.subtractFrom(available, tokenId.getResource());
      Resources.addTo(used, tokenId.getResource());

      LOG.debug("startContainer: node={} application={} container={}"
          +" available={} used={}", containerManagerAddress, applicationId,
          container, available, used);

    }
    StartContainersResponse response =
        StartContainersResponse.newInstance(null, null, null);
    return response;
  }

  synchronized public void checkResourceUsage() {
    LOG.info("Checking resource usage for " + containerManagerAddress);
    Assert.assertEquals(available.getMemorySize(),
        resourceManager.getResourceScheduler().getNodeReport(
            this.nodeId).getAvailableResource().getMemorySize());
    Assert.assertEquals(used.getMemorySize(),
        resourceManager.getResourceScheduler().getNodeReport(
            this.nodeId).getUsedResource().getMemorySize());
  }
  //主要目的是停止一个或多个容器。它接收一个请求对象，包含要停止的容器 ID 列表。方法会逐一处理这些容器，更新容器状态，移除容器，释放资源，并记录日志
  @Override
  synchronized public StopContainersResponse stopContainers(StopContainersRequest request) 
  throws YarnException {
    //遍历容器 ID 列表
    for (ContainerId containerID : request.getContainerIds()) {
      //获取应用 ID
      String applicationId =
          String.valueOf(containerID.getApplicationAttemptId()
            .getApplicationId().getId());
      // Mark the container as COMPLETE
      List<Container> applicationContainers = containers.get(containerID.getApplicationAttemptId()
              .getApplicationId());
      for (Container c : applicationContainers) {
        if (c.getId().compareTo(containerID) == 0) {
          ContainerStatus containerStatus = containerStatusMap.get(c);
          //将容器状态标记为 COMPLETE
          containerStatus.setState(ContainerState.COMPLETE);
          containerStatusMap.put(c, containerStatus);
        }
      }

      // Send a heartbeat
      //发送心跳
      try {
        heartbeat();
      } catch (IOException ioe) {
        throw RPCUtil.getRemoteException(ioe);
      }

      // Remove container and update status
      //移除容器并更新资源
      int ctr = 0;
      Container container = null;
      for (Iterator<Container> i = applicationContainers.iterator(); i
        .hasNext();) {
        container = i.next();
        if (container.getId().compareTo(containerID) == 0) {
          i.remove();
          ++ctr;
        }
      }

      if (ctr != 1) {
        throw new IllegalStateException("Container " + containerID
            + " stopped " + ctr + " times!");
      }
      //释放资源并更新节点资源
      Resources.addTo(available, container.getResource());
      Resources.subtractFrom(used, container.getResource());

      LOG.debug("stopContainer: node={} application={} container={}"
          + " available={} used={}", containerManagerAddress, applicationId,
          containerID, available, used);
    }
    return StopContainersResponse.newInstance(null,null);
  }

  @Override
  synchronized public GetContainerStatusesResponse getContainerStatuses(
      GetContainerStatusesRequest request) throws YarnException {
    List<ContainerStatus> statuses = new ArrayList<ContainerStatus>();
    for (ContainerId containerId : request.getContainerIds()) {
      List<Container> appContainers =
          containers.get(containerId.getApplicationAttemptId()
            .getApplicationId());
      Container container = null;
      for (Container c : appContainers) {
        if (c.getId().equals(containerId)) {
          container = c;
        }
      }
      if (container != null
          && containerStatusMap.get(container).getState() != null) {
        statuses.add(containerStatusMap.get(container));
      }
    }
    return GetContainerStatusesResponse.newInstance(statuses, null);
  }

  @Override
  @Deprecated
  public IncreaseContainersResourceResponse increaseContainersResource(
      IncreaseContainersResourceRequest request)
          throws YarnException, IOException {
    return null;
  }

  @Override
  public ContainerUpdateResponse updateContainer(ContainerUpdateRequest
      request) throws YarnException, IOException {
    return null;
  }
  //创建节点的状态
  public static org.apache.hadoop.yarn.server.api.records.NodeStatus
  createNodeStatus(NodeId nodeId, List<ContainerStatus> containers) {
    RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
    org.apache.hadoop.yarn.server.api.records.NodeStatus nodeStatus = 
        recordFactory.newRecordInstance(org.apache.hadoop.yarn.server.api.records.NodeStatus.class);
    nodeStatus.setNodeId(nodeId);
    nodeStatus.setContainersStatuses(containers);
    NodeHealthStatus nodeHealthStatus = 
      recordFactory.newRecordInstance(NodeHealthStatus.class);
    nodeHealthStatus.setIsNodeHealthy(true);
    nodeStatus.setNodeHealthStatus(nodeHealthStatus);
    return nodeStatus;
  }

  @Override
  public synchronized SignalContainerResponse signalToContainer(
      SignalContainerRequest request) throws YarnException, IOException {
    throw new YarnException("Not supported yet!");
  }

  @Override
  public ResourceLocalizationResponse localize(
      ResourceLocalizationRequest request) throws YarnException, IOException {
    return null;
  }

  @Override
  public ReInitializeContainerResponse reInitializeContainer(
      ReInitializeContainerRequest request) throws YarnException, IOException {
    return null;
  }

  @Override
  public RestartContainerResponse restartContainer(ContainerId containerId)
      throws YarnException, IOException {
    return null;
  }

  @Override
  public RollbackResponse rollbackLastReInitialization(ContainerId containerId)
      throws YarnException, IOException {
    return null;
  }

  @Override
  public CommitResponse commitLastReInitialization(ContainerId containerId)
      throws YarnException, IOException {
    return null;
  }

  @Override
  public GetLocalizationStatusesResponse getLocalizationStatuses(
      GetLocalizationStatusesRequest request) throws YarnException,
      IOException {
    return null;
  }
}
