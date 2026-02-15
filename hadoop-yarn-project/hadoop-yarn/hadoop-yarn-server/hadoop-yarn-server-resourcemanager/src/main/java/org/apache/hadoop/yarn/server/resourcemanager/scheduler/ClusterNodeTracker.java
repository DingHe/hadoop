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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.yarn.server.resourcemanager.ClusterMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Helper library that:
 * - tracks the state of all cluster {@link SchedulerNode}s
 * - provides convenience methods to filter and sort nodes
 */
//ClusterNodeTracker 是 YARN 资源管理器（ResourceManager）中的一个关键组件，它主要用于：
// 跟踪集群中所有SchedulerNode（调度节点）的状态；
// 提供便捷的方法来对节点进行筛选和排序；
// 维护集群资源信息，包括总资源容量、最大可分配资源等。
// 它的主要作用是维护 YARN 集群中所有计算节点的状态，并提供高效的查询和更新方法，以便调度器能够合理分配资源。

@InterfaceAudience.Private
public class ClusterNodeTracker<N extends SchedulerNode> {
  private static final Logger LOG =
      LoggerFactory.getLogger(ClusterNodeTracker.class);

  private ReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);
  private Lock readLock = readWriteLock.readLock();
  private Lock writeLock = readWriteLock.writeLock();
  //存储集群中的所有调度节点（SchedulerNode），NodeId 作为键
  private HashMap<NodeId, N> nodes = new HashMap<>();
  //存储节点名到 SchedulerNode 的映射，方便通过名称快速查找
  private Map<String, N> nodeNameToNodeMap = new HashMap<>();
  //按机架（rack）分类存储 SchedulerNode，便于机架级别的调度优化
  private Map<String, List<N>> nodesPerRack = new HashMap<>();
  //按标签（label）分类存储 SchedulerNode，便于基于标签的资源调度
  private Map<String, List<N>> nodesPerLabel = new HashMap<>();
  //当前集群的总资源容量，随节点增加/删除而更新
  private Resource clusterCapacity = Resources.createResource(0, 0);
  //缓存的集群资源信息，主要用于避免频繁计算
  private volatile Resource staleClusterCapacity =
      Resources.clone(Resources.none());

  // Max allocation
  //存储集群中最大可分配资源值，如最大 CPU 和内存，也就是单个节点的资源上限
  private final long[] maxAllocation;
  //用户配置的最大资源分配值
  private Resource configuredMaxAllocation;
  //是否强制使用 configuredMaxAllocation 作为最大资源分配值
  private boolean forceConfiguredMaxAllocation = true;
  //等待最大资源分配生效的时间
  private long configuredMaxAllocationWaitTime;
  //是否已经报告过最大资源分配值
  private boolean reportedMaxAllocation = false;

  public ClusterNodeTracker() {
    maxAllocation = new long[ResourceUtils.getNumberOfCountableResourceTypes()];
    Arrays.fill(maxAllocation, -1);
  }
  //向 ClusterNodeTracker 添加一个新的 SchedulerNode，并更新资源信息
  public void addNode(N node) {
    writeLock.lock();
    try {
      //将节点添加到 nodes 和 nodeNameToNodeMap，以便通过 NodeId 和名称快速查找
      nodes.put(node.getNodeID(), node);
      nodeNameToNodeMap.put(node.getNodeName(), node);
      //按标签存储节点，更新 nodesPerLabel
      List<N> nodesPerLabels = nodesPerLabel.get(node.getPartition());

      if (nodesPerLabels == null) {
        nodesPerLabels = new ArrayList<N>();
      }
      nodesPerLabels.add(node);

      // Update new set of nodes for given partition.
      nodesPerLabel.put(node.getPartition(), nodesPerLabels);

      // Update nodes per rack as well
      //按机架存储节点，更新 nodesPerRack
      String rackName = node.getRackName();
      List<N> nodesList = nodesPerRack.get(rackName);
      if (nodesList == null) {
        nodesList = new ArrayList<>();
        nodesPerRack.put(rackName, nodesList);
      }
      nodesList.add(node);

      // Update cluster capacity
      //更新集群的总资源容量，并更新 ClusterMetrics 统计信息
      Resources.addTo(clusterCapacity, node.getTotalResource());
      staleClusterCapacity = Resources.clone(clusterCapacity);
      ClusterMetrics.getMetrics().incrCapability(node.getTotalResource());

      // Update maximumAllocation
      //更新最大资源分配 maxAllocation，如果新节点的资源比当前最大值大，则更新 maxAllocation
      updateMaxResources(node, true);
    } finally {
      writeLock.unlock();
    }
  }
  //检查节点是否存在
  public boolean exists(NodeId nodeId) {
    readLock.lock();
    try {
      return nodes.containsKey(nodeId);
    } finally {
      readLock.unlock();
    }
  }
  //获取 NodeId 对应的 SchedulerNode
  public N getNode(NodeId nodeId) {
    readLock.lock();
    try {
      return nodes.get(nodeId);
    } finally {
      readLock.unlock();
    }
  }
  //获取 NodeId 对应的 SchedulerNodeReport（节点状态报告）
  public SchedulerNodeReport getNodeReport(NodeId nodeId) {
    readLock.lock();
    try {
      N n = nodes.get(nodeId);
      return n == null ? null : new SchedulerNodeReport(n);
    } finally {
      readLock.unlock();
    }
  }
  //返回当前集群中节点的总数
  public int nodeCount() {
    readLock.lock();
    try {
      return nodes.size();
    } finally {
      readLock.unlock();
    }
  }
  //返回指定 rack 上的节点数
  public int nodeCount(String rackName) {
    readLock.lock();
    String rName = rackName == null ? "NULL" : rackName;
    try {
      List<N> nodesList = nodesPerRack.get(rName);
      return nodesList == null ? 0 : nodesList.size();
    } finally {
      readLock.unlock();
    }
  }
  //获取集群的资源容量
  public Resource getClusterCapacity() {
    return staleClusterCapacity;
  }
  //从 ClusterNodeTracker 移除一个 SchedulerNode，并更新资源信息
  public N removeNode(NodeId nodeId) {
    writeLock.lock();
    try {
      N node = nodes.remove(nodeId);
      if (node == null) {
        LOG.warn("Attempting to remove a non-existent node " + nodeId);
        return null;
      }
      nodeNameToNodeMap.remove(node.getNodeName());

      // Update nodes per rack as well
      String rackName = node.getRackName();
      List<N> nodesList = nodesPerRack.get(rackName);
      if (nodesList == null) {
        LOG.error("Attempting to remove node from an empty rack " + rackName);
      } else {
        nodesList.remove(node);
        if (nodesList.isEmpty()) {
          nodesPerRack.remove(rackName);
        }
      }

      List<N> nodesPerPartition = nodesPerLabel.get(node.getPartition());
      nodesPerPartition.remove(node);

      // Update new set of nodes for given partition.
      if (nodesPerPartition.isEmpty()) {
        nodesPerLabel.remove(node.getPartition());
      } else {
        nodesPerLabel.put(node.getPartition(), nodesPerPartition);
      }

      // Update cluster capacity
      Resources.subtractFrom(clusterCapacity, node.getTotalResource());
      staleClusterCapacity = Resources.clone(clusterCapacity);
      ClusterMetrics.getMetrics().decrCapability(node.getTotalResource());

      // Update maximumAllocation
      updateMaxResources(node, false);

      return node;
    } finally {
      writeLock.unlock();
    }
  }
  //设置集群的最大可分配资源
  public void setConfiguredMaxAllocation(Resource resource) {
    writeLock.lock();
    try {
      configuredMaxAllocation = Resources.clone(resource);
    } finally {
      writeLock.unlock();
    }
  }

  public void setConfiguredMaxAllocationWaitTime(
      long configuredMaxAllocationWaitTime) {
    writeLock.lock();
    try {
      this.configuredMaxAllocationWaitTime =
          configuredMaxAllocationWaitTime;
    } finally {
      writeLock.unlock();
    }
  }
  //获取当前集群允许的最大资源分配
  public Resource getMaxAllowedAllocation() {
    readLock.lock();
    try {
      if (forceConfiguredMaxAllocation &&
          System.currentTimeMillis() - ResourceManager.getClusterTimeStamp()
              > configuredMaxAllocationWaitTime) {
        forceConfiguredMaxAllocation = false;
      }

      if (forceConfiguredMaxAllocation || !reportedMaxAllocation) {
        return configuredMaxAllocation;
      }

      Resource ret = Resources.clone(configuredMaxAllocation);

      for (int i = 0; i < maxAllocation.length; i++) {
        ResourceInformation info = ret.getResourceInformation(i);

        if (info.getValue() > maxAllocation[i]) {
          info.setValue(maxAllocation[i]);
        }
      }

      return ret;
    } finally {
      readLock.unlock();
    }
  }

  @VisibleForTesting
  public void setForceConfiguredMaxAllocation(boolean flag) {
    writeLock.lock();
    try {
      forceConfiguredMaxAllocation = flag;
    } finally {
      writeLock.unlock();
    }
  }
  // 更新集群中可用资源的最大分配值 (maxAllocation)。
  // 当新的 SchedulerNode 加入或移除时，该方法调整 maxAllocation 数组，以确保资源管理器能正确计算最大可用资源
  //node：类型为 SchedulerNode，表示当前需要被添加或移除的节点
  //add：布尔值，true 表示新增节点，false 表示移除节点
  private void updateMaxResources(SchedulerNode node, boolean add) {
    //获取节点资源
    Resource totalResource = node.getTotalResource();
    ResourceInformation[] totalResources;

    if (totalResource != null) {
      totalResources = totalResource.getResources();
    } else {
      LOG.warn(node.getNodeName() + " reported in with null resources, which "
          + "indicates a problem in the source code. Please file an issue at "
          + "https://issues.apache.org/jira/secure/CreateIssue!default.jspa");

      return;
    }

    writeLock.lock();

    try {
      //处理新增节点
      if (add) { // added node
        // If we add a node, we must have a max allocation for all resource
        // types
        reportedMaxAllocation = true;

        for (int i = 0; i < maxAllocation.length; i++) {
          long value = totalResources[i].getValue();
          //如果当前节点的资源值大于 maxAllocation[i]，则更新 maxAllocation[i]
          if (value > maxAllocation[i]) {
            maxAllocation[i] = value;
          }
        }
      } else {  // removed node  处理移除节点
        boolean recalculate = false;

        for (int i = 0; i < maxAllocation.length; i++) {
          //如果 totalResources[i] 的值 恰好等于 maxAllocation[i]，说明当前节点提供了最大资源
          //将 maxAllocation[i] 置为 -1，表示需要重新计算最大分配资源
          if (totalResources[i].getValue() == maxAllocation[i]) {
            // No need to set reportedMaxAllocation to false here because we
            // will recalculate before we release the lock.
            maxAllocation[i] = -1;
            recalculate = true;
          }
        }

        // We only have to iterate through the nodes if the current max memory
        // or vcores was equal to the removed node's
        if (recalculate) {
          // Treat it like an empty cluster and add nodes
          reportedMaxAllocation = false;
          nodes.values().forEach(n -> updateMaxResources(n, true));
        }
      }
    } finally {
      writeLock.unlock();
    }
  }
  //获取所有的集群节点
  public List<N> getAllNodes() {
    return getNodes(null);
  }

  /**
   * Convenience method to filter nodes based on a condition.
   *
   * @param nodeFilter A {@link NodeFilter} for filtering the nodes
   * @return A list of filtered nodes
   */
  public List<N> getNodes(NodeFilter nodeFilter) {
    List<N> nodeList = new ArrayList<>();
    readLock.lock();
    try {
      if (nodeFilter == null) {
        nodeList.addAll(nodes.values());
      } else {
        for (N node : nodes.values()) {
          if (nodeFilter.accept(node)) {
            nodeList.add(node);
          }
        }
      }
    } finally {
      readLock.unlock();
    }
    return nodeList;
  }

  public List<NodeId> getAllNodeIds() {
    return getNodeIds(null);
  }

  /**
   * Convenience method to filter nodes based on a condition.
   *
   * @param nodeFilter A {@link NodeFilter} for filtering the nodes
   * @return A list of filtered nodes
   */
  public List<NodeId> getNodeIds(NodeFilter nodeFilter) {
    List<NodeId> nodeList = new ArrayList<>();
    readLock.lock();
    try {
      if (nodeFilter == null) {
        for (N node : nodes.values()) {
          nodeList.add(node.getNodeID());
        }
      } else {
        for (N node : nodes.values()) {
          if (nodeFilter.accept(node)) {
            nodeList.add(node.getNodeID());
          }
        }
      }
    } finally {
      readLock.unlock();
    }
    return nodeList;
  }

  /**
   * Convenience method to sort nodes.
   * Nodes can change while being sorted. Using a standard sort will fail
   * without locking each node, the TreeSet handles this without locks.
   *
   * @param comparator the comparator to sort the nodes with
   * @return sorted set of nodes in the form of a TreeSet
   */
  //根据comparator排序所有的集群节点
  public TreeSet<N> sortedNodeSet(Comparator<N> comparator) {
    TreeSet<N> sortedSet = new TreeSet<>(comparator);
    readLock.lock();
    try {
      sortedSet.addAll(nodes.values());
    } finally {
      readLock.unlock();
    }
    return sortedSet;
  }

  /**
   * Convenience method to return list of nodes corresponding to resourceName
   * passed in the {@link ResourceRequest}.
   *
   * @param resourceName Host/rack name of the resource, or
   * {@link ResourceRequest#ANY}
   * @return list of nodes that match the resourceName
   */
  public List<N> getNodesByResourceName(final String resourceName) {
    Preconditions.checkArgument(
        resourceName != null && !resourceName.isEmpty());
    List<N> retNodes = new ArrayList<>();
    if (ResourceRequest.ANY.equals(resourceName)) {
      retNodes.addAll(getAllNodes());
    } else if (nodeNameToNodeMap.containsKey(resourceName)) {
      retNodes.add(nodeNameToNodeMap.get(resourceName));
    } else if (nodesPerRack.containsKey(resourceName)) {
      retNodes.addAll(nodesPerRack.get(resourceName));
    } else {
      LOG.info(
          "Could not find a node matching given resourceName " + resourceName);
    }
    return retNodes;
  }

  /**
   * Convenience method to return list of {@link NodeId} corresponding to
   * resourceName passed in the {@link ResourceRequest}.
   *
   * @param resourceName Host/rack name of the resource, or
   * {@link ResourceRequest#ANY}
   * @return list of {@link NodeId} that match the resourceName
   */
  public List<NodeId> getNodeIdsByResourceName(final String resourceName) {
    Preconditions.checkArgument(
        resourceName != null && !resourceName.isEmpty());
    List<NodeId> retNodes = new ArrayList<>();
    if (ResourceRequest.ANY.equals(resourceName)) {
      retNodes.addAll(getAllNodeIds());
    } else if (nodeNameToNodeMap.containsKey(resourceName)) {
      retNodes.add(nodeNameToNodeMap.get(resourceName).getNodeID());
    } else if (nodesPerRack.containsKey(resourceName)) {
      for (N node : nodesPerRack.get(resourceName)) {
        retNodes.add(node.getNodeID());
      }
    } else {
      LOG.info(
          "Could not find a node matching given resourceName " + resourceName);
    }
    return retNodes;
  }

  /**
   * update cached nodes per partition on a node label change event.
   * @param partition nodeLabel
   * @param nodeIds List of Node IDs
   */
  public void updateNodesPerPartition(String partition, Set<NodeId> nodeIds) {
    writeLock.lock();
    try {
      // Clear all entries.
      nodesPerLabel.remove(partition);

      List<N> nodesPerPartition = new ArrayList<N>();
      for (NodeId nodeId : nodeIds) {
        N n = getNode(nodeId);
        if (n != null) {
          nodesPerPartition.add(n);
        }
      }

      // Update new set of nodes for given partition.
      nodesPerLabel.put(partition, nodesPerPartition);
    } finally {
      writeLock.unlock();
    }
  }

  public List<N> getNodesPerPartition(String partition) {
    List<N> nodesPerPartition = null;
    readLock.lock();
    try {
      if (nodesPerLabel.containsKey(partition)) {
        nodesPerPartition = new ArrayList<N>(nodesPerLabel.get(partition));
      }
    } finally {
      readLock.unlock();
    }
    return nodesPerPartition;
  }

  public List<String> getPartitions() {
    List<String> partitions = null;
    readLock.lock();
    try {
      partitions = new ArrayList(nodesPerLabel.keySet());
    } finally {
      readLock.unlock();
    }
    return partitions;
  }
}