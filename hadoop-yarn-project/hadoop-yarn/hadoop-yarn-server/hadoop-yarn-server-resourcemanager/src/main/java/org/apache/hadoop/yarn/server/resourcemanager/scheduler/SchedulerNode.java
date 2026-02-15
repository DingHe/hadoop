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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.commons.lang3.builder.CompareToBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.util.resource.Resources;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;


/**
 * Represents a YARN Cluster Node from the viewpoint of the scheduler.
 */
//YARN 资源管理器中代表集群中节点的调度视角。
// 它负责在 YARN 调度器中表示一个节点的资源状况，包括已分配和未分配资源、节点的容器信息等。
// 该类提供了用于更新节点资源、管理容器的分配、跟踪节点状态等功能，供资源管理器和调度器使用。
// 简而言之，它作为调度器的视图，帮助调度器管理和监控集群中每个节点的资源使用情况
@Private
@Unstable
public abstract class SchedulerNode {

  private static final Logger LOG =
      LoggerFactory.getLogger(SchedulerNode.class);
  //表示当前节点上尚未分配的资源，初始为零。每当容器被分配时，这个资源值会减少
  private Resource unallocatedResource = Resource.newInstance(0, 0);
  //表示已经分配给容器的资源，初始为零。当分配容器时，这个资源值会增加
  private Resource allocatedResource = Resource.newInstance(0, 0);
  //表示节点的总资源，表示节点可用的所有资源。这个值在节点更新时会变化
  private Resource totalResource;
  //表示被保留的容器，调度器可能会为某些应用程序保留资源
  private RMContainer reservedContainer;
  //表示当前节点上已经分配的容器数量。该值是线程安全的，使用 volatile 来确保多线程环境下的数据一致性
  private volatile int numContainers;
  //表示当前节点上所有容器的资源利用率，包括 CPU、内存等。它会实时更新，表示容器的资源利用情况
  private volatile ResourceUtilization containersUtilization =
      ResourceUtilization.newInstance(0, 0, 0f);
  //表示整个节点的资源利用率，包括当前节点上所有资源的总体使用情况
  private volatile ResourceUtilization nodeUtilization =
      ResourceUtilization.newInstance(0, 0, 0f);
  /** Time stamp for overcommitted resources to time out. */
  //表示资源过度承诺的超时值。若资源被过度承诺，调度器会在超时后开始回收容器以恢复资源的正常状态
  private long overcommitTimeout = -1;

  /* set of containers that are allocated containers */
  //存储所有已分配的容器的映射，容器 ID 作为键，ContainerInfo 对象作为值。ContainerInfo 用于存储容器的详细信息
  private final Map<ContainerId, ContainerInfo> launchedContainers =
      new HashMap<>();
  //表示节点本身，它是 YARN 中的一个核心节点对象，包含有关节点的详细信息
  private final RMNode rmNode;
  //节点的名称，通常是主机名，调度决策时使用。它可以包含端口信息，具体取决于配置
  private final String nodeName;
  private final RMContext rmContext;
  //节点的标签集合，用于节点标签匹配，决定哪些容器可以在该节点上运行
  private volatile Set<String> labels = null;
  //节点的属性集合，存储关于节点的各种属性，例如硬件规格、软件环境等
  private volatile Set<NodeAttribute> nodeAttributes = null;

  // Last updated time
  //节点最后一次心跳的时间戳，用于监控节点的健康状况
  private volatile long lastHeartbeatMonotonicTime;

  public SchedulerNode(RMNode node, boolean usePortForNodeName,
      Set<String> labels) {
    this.rmNode = node;
    this.rmContext = node.getRMContext();
    this.unallocatedResource = Resources.clone(node.getTotalCapability());
    this.totalResource = Resources.clone(node.getTotalCapability());
    if (usePortForNodeName) {
      nodeName = rmNode.getHostName() + ":" + node.getNodeID().getPort();
    } else {
      nodeName = rmNode.getHostName();
    }
    this.labels = ImmutableSet.copyOf(labels);
    this.lastHeartbeatMonotonicTime = Time.monotonicNow();
  }

  public SchedulerNode(RMNode node, boolean usePortForNodeName) {
    this(node, usePortForNodeName, CommonNodeLabelsManager.EMPTY_STRING_SET);
  }

  public RMNode getRMNode() {
    return this.rmNode;
  }

  /**
   * Set total resources on the node.
   * @param resource Total resources on the node.
   */
  public synchronized void updateTotalResource(Resource resource){
    this.totalResource = resource;
    this.unallocatedResource = Resources.subtract(totalResource,
        this.allocatedResource);
  }

  /**
   * Set the timeout for the node to stop overcommitting the resources. After
   * this time the scheduler will start killing containers until the resources
   * are not overcommitted anymore. This may reset a previous timeout.
   * @param timeOut Time out in milliseconds.
   */
  public synchronized void setOvercommitTimeOut(long timeOut) {
    if (timeOut >= 0) {
      if (this.overcommitTimeout != -1) {
        LOG.debug("The overcommit timeout for {} was already set to {}",
            getNodeID(), this.overcommitTimeout);
      }
      this.overcommitTimeout = Time.now() + timeOut;
    }
  }

  /**
   * Check if the time out has passed.
   * @return If the node is overcommitted.
   */
  public synchronized boolean isOvercommitTimedOut() {
    return this.overcommitTimeout >= 0 && Time.now() >= this.overcommitTimeout;
  }

  /**
   * Check if the node has a time out for overcommit resources.
   * @return If the node has a time out for overcommit resources.
   */
  public synchronized boolean isOvercommitTimeOutSet() {
    return this.overcommitTimeout >= 0;
  }

  /**
   * Get the ID of the node which contains both its hostname and port.
   * @return The ID of the node.
   */
  public NodeId getNodeID() {
    return this.rmNode.getNodeID();
  }

  /**
   * Get HTTP address for the node.
   * @return HTTP address for the node.
   */
  public String getHttpAddress() {
    return this.rmNode.getHttpAddress();
  }

  /**
   * Get the name of the node for scheduling matching decisions.
   * <p>
   * Typically this is the 'hostname' reported by the node, but it could be
   * configured to be 'hostname:port' reported by the node via the
   * {@link YarnConfiguration#RM_SCHEDULER_INCLUDE_PORT_IN_NODE_NAME} constant.
   * The main usecase of this is YARN minicluster to be able to differentiate
   * node manager instances by their port number.
   * @return Name of the node for scheduling matching decisions.
   */
  public String getNodeName() {
    return nodeName;
  }

  /**
   * Get rackname.
   * @return rackname
   */
  public String getRackName() {
    return this.rmNode.getRackName();
  }

  /**
   * The Scheduler has allocated containers on this node to the given
   * application.
   * @param rmContainer Allocated container
   */
  public void allocateContainer(RMContainer rmContainer) {
    allocateContainer(rmContainer, false);
  }

  /**
   * The Scheduler has allocated containers on this node to the given
   * application.
   * @param rmContainer Allocated container
   * @param launchedOnNode True if the container has been launched
   */
  protected synchronized void allocateContainer(RMContainer rmContainer,
      boolean launchedOnNode) {
    Container container = rmContainer.getContainer();
    if (rmContainer.getExecutionType() == ExecutionType.GUARANTEED) {
      deductUnallocatedResource(container.getResource());
      ++numContainers;
    }

    launchedContainers.put(container.getId(),
        new ContainerInfo(rmContainer, launchedOnNode));
  }

  /**
   * Get unallocated resources on the node.
   * @return Unallocated resources on the node
   */
  public synchronized Resource getUnallocatedResource() {
    return this.unallocatedResource;
  }

  /**
   * Get allocated resources on the node.
   * @return Allocated resources on the node
   */
  public synchronized Resource getAllocatedResource() {
    return this.allocatedResource;
  }

  /**
   * Get total resources on the node.
   * @return Total resources on the node.
   */
  public synchronized Resource getTotalResource() {
    return this.totalResource;
  }

  /**
   * Check if a container is launched by this node.
   *
   * @param containerId containerId.
   * @return If the container is launched by the node.
   */
  public synchronized boolean isValidContainer(ContainerId containerId) {
    if (launchedContainers.containsKey(containerId)) {
      return true;
    }
    return false;
  }

  /**
   * Update the resources of the node when releasing a container.
   * @param container Container to release.
   */
  protected synchronized void updateResourceForReleasedContainer(
      Container container) {
    if (container.getExecutionType() == ExecutionType.GUARANTEED) {
      addUnallocatedResource(container.getResource());
      --numContainers;
    }
  }

  /**
   * Release an allocated container on this node.
   * @param containerId ID of container to be released.
   * @param releasedByNode whether the release originates from a node update.
   */
  public synchronized void releaseContainer(ContainerId containerId,
      boolean releasedByNode) {
    ContainerInfo info = launchedContainers.get(containerId);
    if (info == null) {
      return;
    }
    if (!releasedByNode && info.launchedOnNode) {
      // wait until node reports container has completed
      return;
    }

    launchedContainers.remove(containerId);
    Container container = info.container.getContainer();

    // We remove allocation tags when a container is actually
    // released on NM. This is to avoid running into situation
    // when AM releases a container and NM has some delay to
    // actually release it, then the tag can still be visible
    // at RM so that RM can respect it during scheduling new containers.
    if (rmContext != null && rmContext.getAllocationTagsManager() != null) {
      rmContext.getAllocationTagsManager()
          .removeContainer(container.getNodeId(),
              container.getId(), container.getAllocationTags());
    }

    updateResourceForReleasedContainer(container);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Released container " + container.getId() + " of capacity "
              + container.getResource() + " on host " + rmNode.getNodeAddress()
              + ", which currently has " + numContainers + " containers, "
              + getAllocatedResource() + " used and " + getUnallocatedResource()
              + " available" + ", release resources=" + true);
    }
  }

  /**
   * Inform the node that a container has launched.
   * @param containerId ID of the launched container
   */
  public synchronized void containerStarted(ContainerId containerId) {
    ContainerInfo info = launchedContainers.get(containerId);
    if (info != null) {
      info.launchedOnNode = true;
    }
  }

  /**
   * Add unallocated resources to the node. This is used when unallocating a
   * container.
   * @param resource Resources to add.
   */
  private synchronized void addUnallocatedResource(Resource resource) {
    if (resource == null) {
      LOG.error("Invalid resource addition of null resource for "
          + rmNode.getNodeAddress());
      return;
    }
    Resources.addTo(unallocatedResource, resource);
    Resources.subtractFrom(allocatedResource, resource);
  }

  /**
   * Deduct unallocated resources from the node. This is used when allocating a
   * container.
   * @param resource Resources to deduct.
   */
  @VisibleForTesting
  public synchronized void deductUnallocatedResource(Resource resource) {
    if (resource == null) {
      LOG.error("Invalid deduction of null resource for "
          + rmNode.getNodeAddress());
      return;
    }
    Resources.subtractFrom(unallocatedResource, resource);
    Resources.addTo(allocatedResource, resource);
  }

  /**
   * Reserve container for the attempt on this node.
   * @param attempt Application attempt asking for the reservation.
   * @param schedulerKey Priority of the reservation.
   * @param container Container reserving resources for.
   */
  public abstract void reserveResource(SchedulerApplicationAttempt attempt,
      SchedulerRequestKey schedulerKey, RMContainer container);

  /**
   * Unreserve resources on this node.
   * @param attempt Application attempt that had done the reservation.
   */
  public abstract void unreserveResource(SchedulerApplicationAttempt attempt);

  @Override
  public String toString() {
    return "host: " + rmNode.getNodeAddress() + " #containers="
        + getNumContainers() + " available=" + getUnallocatedResource()
        + " used=" + getAllocatedResource();
  }

  /**
   * Get number of active containers on the node.
   * @return Number of active containers on the node.
   */
  public int getNumContainers() {
    return numContainers;
  }

  /**
   * Get the containers running on the node.
   * @return A copy of containers running on the node.
   */
  public synchronized List<RMContainer> getCopiedListOfRunningContainers() {
    List<RMContainer> result = new ArrayList<>(launchedContainers.size());
    for (ContainerInfo info : launchedContainers.values()) {
      result.add(info.container);
    }
    return result;
  }

  /**
   * Get the containers running on the node with AM containers at the end.
   * @return A copy of running containers with AM containers at the end.
   */
  public synchronized List<RMContainer> getRunningContainersWithAMsAtTheEnd() {
    LinkedList<RMContainer> result = new LinkedList<>();
    for (ContainerInfo info : launchedContainers.values()) {
      if(info.container.isAMContainer()) {
        result.addLast(info.container);
      } else {
        result.addFirst(info.container);
      }
    }
    return result;
  }

  /**
   * Get the containers running on the node ordered by which to kill first. It
   * tries to kill AMs last, then GUARANTEED containers, and it kills
   * OPPORTUNISTIC first. If the same time, it uses the creation time.
   * @return A copy of the running containers ordered by which to kill first.
   */
  public List<RMContainer> getContainersToKill() {
    List<RMContainer> result = getLaunchedContainers();
    Collections.sort(result, (c1, c2) -> {
      return new CompareToBuilder()
          .append(c1.isAMContainer(), c2.isAMContainer())
          .append(c2.getExecutionType(), c1.getExecutionType()) // reversed
          .append(c2.getCreationTime(), c1.getCreationTime()) // reversed
          .toComparison();
    });
    return result;
  }

  /**
   * Get the launched containers in the node.
   * @return List of launched containers.
   */
  protected synchronized List<RMContainer> getLaunchedContainers() {
    List<RMContainer> result = new ArrayList<>();
    for (ContainerInfo info : launchedContainers.values()) {
      result.add(info.container);
    }
    return result;
  }

  /**
   * Get the container for the specified container ID.
   * @param containerId The container ID
   * @return The container for the specified container ID
   */
  protected synchronized RMContainer getContainer(ContainerId containerId) {
    RMContainer container = null;
    ContainerInfo info = launchedContainers.get(containerId);
    if (info != null) {
      container = info.container;
    }
    return container;
  }

  /**
   * Get the reserved container in the node.
   * @return Reserved container in the node.
   */
  public synchronized RMContainer getReservedContainer() {
    return reservedContainer;
  }

  /**
   * Set the reserved container in the node.
   * @param reservedContainer Reserved container in the node.
   */
  public synchronized void
  setReservedContainer(RMContainer reservedContainer) {
    this.reservedContainer = reservedContainer;
  }

  /**
   * Recover a container.
   * @param rmContainer Container to recover.
   */
  public synchronized void recoverContainer(RMContainer rmContainer) {
    if (rmContainer.getState().equals(RMContainerState.COMPLETED)) {
      return;
    }
    allocateContainer(rmContainer, true);
  }

  /**
   * Get the labels for the node.
   * @return Set of labels for the node.
   */
  public Set<String> getLabels() {
    return labels;
  }

  /**
   * Update the labels for the node.
   * @param labels Set of labels for the node.
   */
  public void updateLabels(Set<String> labels) {
    this.labels = labels;
  }

  /**
   * Get partition of which the node belongs to, if node-labels of this node is
   * empty or null, it belongs to NO_LABEL partition. And since we only support
   * one partition for each node (YARN-2694), first label will be its partition.
   * @return Partition for the node.
   */
  public String getPartition() {
    if (this.labels == null || this.labels.isEmpty()) {
      return RMNodeLabelsManager.NO_LABEL;
    } else {
      return this.labels.iterator().next();
    }
  }

  /**
   * Set the resource utilization of the containers in the node.
   * @param containersUtilization Resource utilization of the containers.
   */
  public void setAggregatedContainersUtilization(
      ResourceUtilization containersUtilization) {
    this.containersUtilization = containersUtilization;
  }

  /**
   * Get the resource utilization of the containers in the node.
   * @return Resource utilization of the containers.
   */
  public ResourceUtilization getAggregatedContainersUtilization() {
    return this.containersUtilization;
  }

  /**
   * Set the resource utilization of the node. This includes the containers.
   * @param nodeUtilization Resource utilization of the node.
   */
  public void setNodeUtilization(ResourceUtilization nodeUtilization) {
    this.nodeUtilization = nodeUtilization;
  }

  /**
   * Get the resource utilization of the node.
   * @return Resource utilization of the node.
   */
  public ResourceUtilization getNodeUtilization() {
    return this.nodeUtilization;
  }

  public long getLastHeartbeatMonotonicTime() {
    return lastHeartbeatMonotonicTime;
  }

  /**
   * This will be called for each node heartbeat.
   */
  public void notifyNodeUpdate() {
    this.lastHeartbeatMonotonicTime = Time.monotonicNow();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SchedulerNode)) {
      return false;
    }

    SchedulerNode that = (SchedulerNode) o;

    return getNodeID().equals(that.getNodeID());
  }

  @Override
  public int hashCode() {
    return getNodeID().hashCode();
  }

  public Set<NodeAttribute> getNodeAttributes() {
    return nodeAttributes;
  }

  public void updateNodeAttributes(Set<NodeAttribute> attributes) {
    this.nodeAttributes = attributes;
  }

  private static class ContainerInfo {
    private final RMContainer container;
    private boolean launchedOnNode;

    public ContainerInfo(RMContainer container, boolean launchedOnNode) {
      this.container = container;
      this.launchedOnNode = launchedOnNode;
    }
  }
}
