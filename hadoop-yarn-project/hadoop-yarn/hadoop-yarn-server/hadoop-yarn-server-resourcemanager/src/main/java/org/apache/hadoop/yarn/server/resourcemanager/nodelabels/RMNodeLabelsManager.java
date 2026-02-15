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

package org.apache.hadoop.yarn.server.resourcemanager.nodelabels;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.nodelabels.RMNodeLabel;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeLabelsUpdateSchedulerEvent;
import org.apache.hadoop.yarn.util.resource.Resources;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;

//用于管理节点标签（Node Labels）的组件。节点标签是 YARN 提供的一种资源分配机制，可以用来对集群中的节点进行分类，从而实现资源的精细化管理。
// 例如，可以通过节点标签将特定类型的作业调度到特定的节点上。
//该类继承自 CommonNodeLabelsManager，扩展了基本的节点标签管理功能，提供了额外的机制：
//管理队列 (Queue) 对标签的访问权限，确保资源调度时不会违反标签约束。
//更新节点标签映射关系，确保节点状态变更时，其标签信息能正确传播到调度器。
//确保删除标签时不会影响仍在使用该标签的队列，避免潜在的调度冲突。
//提供并发安全的资源更新，以确保资源管理的正确性
public class RMNodeLabelsManager extends CommonNodeLabelsManager {
  protected static class Queue {
    //该队列可以访问的节点标签集合，确保某些任务只能被调度到特定标签的节点上
    protected Set<String> accessibleNodeLabels;
    //队列所管理的资源总量
    protected Resource resource;

    protected Queue() {
      accessibleNodeLabels =
          Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
      resource = Resource.newInstance(0, 0);
    }
  }
  //存储了 YARN 资源调度队列 (Queue) 及其关联的 Queue 对象，包括可访问的节点标签信息
  ConcurrentMap<String, Queue> queueCollections =
      new ConcurrentHashMap<String, Queue>();
  private YarnAuthorizationProvider authorizer;
  private RMContext rmContext = null;
  
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    authorizer = YarnAuthorizationProvider.getInstance(conf);
  }
  // 用于将标签添加到节点，并在更新后同步资源映射
  // addedLabelsToNode：一个 Map<NodeId, Set<String>>，表示要添加的节点标签映射关系
  @Override
  public void addLabelsToNode(Map<NodeId, Set<String>> addedLabelsToNode)
      throws IOException {
    writeLock.lock();
    try {
      // get nodesCollection before edition
      Map<String, Host> before = cloneNodeMap(addedLabelsToNode.keySet());

      super.addLabelsToNode(addedLabelsToNode);

      // get nodesCollection after edition
      Map<String, Host> after = cloneNodeMap(addedLabelsToNode.keySet());

      // update running nodes resources
      updateResourceMappings(before, after);
    } finally {
      writeLock.unlock();
    }
  }

  protected void checkRemoveFromClusterNodeLabelsOfQueue(
      Collection<String> labelsToRemove) throws IOException {
    // Check if label to remove doesn't existed or null/empty, will throw
    // exception if any of labels to remove doesn't meet requirement
    for (String label : labelsToRemove) {
      label = normalizeLabel(label);

      // check if any queue contains this label
      for (Entry<String, Queue> entry : queueCollections.entrySet()) {
        String queueName = entry.getKey();
        Set<String> queueLabels = entry.getValue().accessibleNodeLabels;
        if (queueLabels.contains(label)) {
          throw new IOException("Cannot remove label=" + label
              + ", because queue=" + queueName + " is using this label. "
              + "Please remove label on queue before remove the label");
        }
      }
    }
  }

  @Override
  public void removeFromClusterNodeLabels(Collection<String> labelsToRemove)
      throws IOException {
    writeLock.lock();
    try {
      if (!isInitNodeLabelStoreInProgress()) {
        // We cannot remove node labels from collection when some queue(s) are
        // using any of them.
        // We will not do remove when recovery is in prpgress. During
        // service starting, we will replay edit logs and recover state. It is
        // possible that a history operation removed some labels which were not
        // used by some queues in the past but are used by current queues.
        checkRemoveFromClusterNodeLabelsOfQueue(labelsToRemove);
      }
      // copy before NMs
      Map<String, Host> before = cloneNodeMap();

      super.removeFromClusterNodeLabels(labelsToRemove);

      updateResourceMappings(before, nodeCollections);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void addToCluserNodeLabels(Collection<NodeLabel> labels)
      throws IOException {
    writeLock.lock();
    try {
      super.addToCluserNodeLabels(labels);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void
      removeLabelsFromNode(Map<NodeId, Set<String>> removeLabelsFromNode)
          throws IOException {
    writeLock.lock();
    try {
      // get nodesCollection before edition
      Map<String, Host> before =
          cloneNodeMap(removeLabelsFromNode.keySet());

      super.removeLabelsFromNode(removeLabelsFromNode);

      // get nodesCollection before edition
      Map<String, Host> after = cloneNodeMap(removeLabelsFromNode.keySet());

      // update running nodes resources
      updateResourceMappings(before, after);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void replaceLabelsOnNode(Map<NodeId, Set<String>> replaceLabelsToNode)
      throws IOException {
    writeLock.lock();
    try {
      Map<NodeId, Set<String>> effectiveModifiedLabelMappings =
          getModifiedNodeLabelsMappings(replaceLabelsToNode);

      if(effectiveModifiedLabelMappings.isEmpty()) {
        LOG.info("No Modified Node label Mapping to replace");
        return;
      }

      // get nodesCollection before edition
      Map<String, Host> before =
          cloneNodeMap(effectiveModifiedLabelMappings.keySet());

      super.replaceLabelsOnNode(effectiveModifiedLabelMappings);

      // get nodesCollection after edition
      Map<String, Host> after =
          cloneNodeMap(effectiveModifiedLabelMappings.keySet());

      // update running nodes resources
      updateResourceMappings(before, after);
    } finally {
      writeLock.unlock();
    }
  }

  private Map<NodeId, Set<String>> getModifiedNodeLabelsMappings(
      Map<NodeId, Set<String>> replaceLabelsToNode) {
    Map<NodeId, Set<String>> effectiveModifiedLabels = new HashMap<>();
    for (Entry<NodeId, Set<String>> nodeLabelMappingEntry : replaceLabelsToNode
        .entrySet()) {
      NodeId nodeId = nodeLabelMappingEntry.getKey();
      Set<String> modifiedNodeLabels = nodeLabelMappingEntry.getValue();
      Set<String> labelsBeforeModification = null;
      Host host = nodeCollections.get(nodeId.getHost());
      if (host == null) {
        effectiveModifiedLabels.put(nodeId, modifiedNodeLabels);
        continue;
      } else if (nodeId.getPort() == WILDCARD_PORT) {
        labelsBeforeModification = host.labels;
      } else if (host.nms.get(nodeId) != null) {
        labelsBeforeModification = host.nms.get(nodeId).labels;
      }
      if (labelsBeforeModification == null
          || labelsBeforeModification.size() != modifiedNodeLabels.size()
          || !labelsBeforeModification.containsAll(modifiedNodeLabels)) {
        effectiveModifiedLabels.put(nodeId, modifiedNodeLabels);
      }
    }
    return effectiveModifiedLabels;
  }

  /*
   * Following methods are used for setting if a node is up and running, and it
   * will update running nodes resource
   */
  public void activateNode(NodeId nodeId, Resource resource) {
    writeLock.lock();
    try {
      // save if we have a node before
      Map<String, Host> before = cloneNodeMap(ImmutableSet.of(nodeId));
      
      createHostIfNonExisted(nodeId.getHost());
      try {
        createNodeIfNonExisted(nodeId);
      } catch (IOException e) {
        LOG.error("This shouldn't happen, cannot get host in nodeCollection"
            + " associated to the node being activated");
        return;
      }

      Node nm = getNMInNodeSet(nodeId);
      nm.resource = resource;
      nm.running = true;

      // Add node in labelsCollection
      Set<String> labelsForNode = getLabelsByNode(nodeId);
      if (labelsForNode != null) {
        for (String label : labelsForNode) {
          RMNodeLabel labelInfo = labelCollections.get(label);
          if(labelInfo != null) {
            labelInfo.addNodeId(nodeId);
          }
        }
      }
      
      // get the node after edition
      Map<String, Host> after = cloneNodeMap(ImmutableSet.of(nodeId));
      
      updateResourceMappings(before, after);
    } finally {
      writeLock.unlock();
    }
  }
  
  /*
   * Following methods are used for setting if a node unregistered to RM
   */
  public void deactivateNode(NodeId nodeId) {
    writeLock.lock();
    try {
      // save if we have a node before
      Map<String, Host> before = cloneNodeMap(ImmutableSet.of(nodeId));
      Node nm = getNMInNodeSet(nodeId);
      if (null != nm) {
        if (isNodeLabelExplicit(nm.nodeId)) {
          // When node deactivated, remove the nm from node collection if no
          // labels explicitly set for this particular nm

          // Save labels first, we need to remove label->nodes relation later
          Set<String> savedNodeLabels = getLabelsOnNode(nodeId);
          
          // Remove this node in nodes collection
          nodeCollections.get(nodeId.getHost()).nms.remove(nodeId);
          
          // Remove this node in labels->node
          removeNodeFromLabels(nodeId, savedNodeLabels);
        } else {
          // set nm is not running, and its resource = 0
          nm.running = false;
          nm.resource = Resource.newInstance(0, 0);
        }
      }
      
      // get the node after edition
      Map<String, Host> after = cloneNodeMap(ImmutableSet.of(nodeId));
      
      updateResourceMappings(before, after);
    } finally {
      writeLock.unlock();
    }
  }

  public void updateNodeResource(NodeId node, Resource newResource) {
    deactivateNode(node);
    activateNode(node, newResource);
  }

  public void reinitializeQueueLabels(Map<String, Set<String>> queueToLabels) {
    writeLock.lock();
    try {
      // clear before set
      this.queueCollections.clear();

      for (Entry<String, Set<String>> entry : queueToLabels.entrySet()) {
        String queue = entry.getKey();
        Queue q = new Queue();
        this.queueCollections.put(queue, q);

        Set<String> labels = entry.getValue();
        if (labels.contains(ANY)) {
          continue;
        }

        q.accessibleNodeLabels.addAll(labels);
        for (Host host : nodeCollections.values()) {
          for (Entry<NodeId, Node> nentry : host.nms.entrySet()) {
            NodeId nodeId = nentry.getKey();
            Node nm = nentry.getValue();
            if (nm.running && isNodeUsableByQueue(getLabelsByNode(nodeId), q)) {
              Resources.addTo(q.resource, nm.resource);
            }
          }
        }
      }
    } finally {
      writeLock.unlock();
    }
  }
  
  public Resource getQueueResource(String queueName, Set<String> queueLabels,
      Resource clusterResource) {
    readLock.lock();
    try {
      if (queueLabels.contains(ANY)) {
        return clusterResource;
      }
      Queue q = queueCollections.get(queueName);
      if (null == q) {
        return Resources.none();
      }
      return q.resource;
    } finally {
      readLock.unlock();
    }
  }
  
  /*
   * Get active node count based on label.
   */
  public int getActiveNMCountPerLabel(String label) {
    if (label == null) {
      return 0;
    }
    readLock.lock();
    try {
      RMNodeLabel labelInfo = labelCollections.get(label);
      return (labelInfo == null) ? 0 : labelInfo.getNumActiveNMs();
    } finally {
      readLock.unlock();
    }
  }

  public Set<String> getLabelsOnNode(NodeId nodeId) {
    readLock.lock();
    try {
      Set<String> nodeLabels = getLabelsByNode(nodeId);
      return Collections.unmodifiableSet(nodeLabels);
    } finally {
      readLock.unlock();
    }
  }
  
  public boolean containsNodeLabel(String label) {
    readLock.lock();
    try {
      return label != null
          && (label.isEmpty() || labelCollections.containsKey(label));
    } finally {
      readLock.unlock();
    }
  }

  private Map<String, Host> cloneNodeMap(Set<NodeId> nodesToCopy) {
    Map<String, Host> map = new HashMap<String, Host>();
    for (NodeId nodeId : nodesToCopy) {
      if (!map.containsKey(nodeId.getHost())) {
        Host originalN = nodeCollections.get(nodeId.getHost());
        if (null == originalN) {
          continue;
        }
        Host n = originalN.copy();
        n.nms.clear();
        map.put(nodeId.getHost(), n);
      }

      Host n = map.get(nodeId.getHost());
      if (WILDCARD_PORT == nodeId.getPort()) {
        for (Entry<NodeId, Node> entry : nodeCollections
            .get(nodeId.getHost()).nms.entrySet()) {
          n.nms.put(entry.getKey(), entry.getValue().copy());
        }
      } else {
        Node nm = getNMInNodeSet(nodeId);
        if (null != nm) {
          n.nms.put(nodeId, nm.copy());
        }
      }
    }
    return map;
  }

  @SuppressWarnings("unchecked")
  //用于更新节点标签变更后对资源的影响，并通知资源管理器（RM，Resource Manager）进行调度调整。其主要功能包括：
  //获取所有受影响的节点（包括变更前和变更后的节点）。
  //根据标签变更调整资源分配：
  //如果一个节点的标签被移除或更改，则从相应的 Queue 和 RMNodeLabel 资源集合中移除该节点的资源。
  //如果一个节点的标签被添加或修改，则更新 Queue 和 RMNodeLabel 资源信息，并添加新的资源。
  //将新的节点标签映射提交给调度器，以便资源调度能够基于最新的标签分布进行决策
  //before：更新前的节点信息快照，包含 Host（主机）及其上的 Node（节点）数据。
  //after：更新后的节点信息快照。
  private void updateResourceMappings(Map<String, Host> before,
      Map<String, Host> after) {
    // Get NMs in before only
    //计算受影响的所有节点，最终 allNMs 包含了所有可能受影响的节点
    Set<NodeId> allNMs = new HashSet<NodeId>();
    for (Entry<String, Host> entry : before.entrySet()) {
      allNMs.addAll(entry.getValue().nms.keySet());
    }
    for (Entry<String, Host> entry : after.entrySet()) {
      allNMs.addAll(entry.getValue().nms.keySet());
    }
    
    // Map used to notify RM
    Map<NodeId, Set<String>> newNodeToLabelsMap =
        new HashMap<NodeId, Set<String>>();

    // traverse all nms
    //遍历所有受影响的节点，处理资源变更
    for (NodeId nodeId : allNMs) {
      Node oldNM;
      //获取 before 中的 NodeId 对应的 Node（如果存在
      if ((oldNM = getNMInNodeSet(nodeId, before, true)) != null) {
        //获取 oldLabels（旧的标签集合）
        Set<String> oldLabels = getLabelsByNode(nodeId, before);
        // no label in the past
        if (oldLabels.isEmpty()) {
          // update labels
          //若该节点以前没有标签
          //从 NO_LABEL（默认无标签）对应的 RMNodeLabel 中移除该节点资源
          RMNodeLabel label = labelCollections.get(NO_LABEL);
          label.removeNode(oldNM.resource);

          // update queues, all queue can access this node
          //从所有队列中扣除该节点的资源，因为无标签节点可被所有队列访问
          for (Queue q : queueCollections.values()) {
            Resources.subtractFrom(q.resource, oldNM.resource);
          }
        } else {
          // update labels
          for (String labelName : oldLabels) {
            //如果节点以前有标签
            RMNodeLabel label = labelCollections.get(labelName);
            if (null == label) {
              continue;
            }
            //从原有标签的 RMNodeLabel 资源集合中移除节点
            label.removeNode(oldNM.resource);
          }

          // update queues, only queue can access this node will be subtract
          //仅从可使用该标签的 Queue 里移除资源
          for (Queue q : queueCollections.values()) {
            if (isNodeUsableByQueue(oldLabels, q)) {
              Resources.subtractFrom(q.resource, oldNM.resource);
            }
          }
        }
      }
      //处理新节点（变更后的资源添加）
      Node newNM;
      //获取 after 中 NodeId 对应的新 Node（如果存在）
      if ((newNM = getNMInNodeSet(nodeId, after, true)) != null) {
        //获取 newLabels（新标签集合）
        Set<String> newLabels = getLabelsByNode(nodeId, after);
        //记录 节点新标签映射
        newNodeToLabelsMap.put(nodeId, ImmutableSet.copyOf(newLabels));
        
        // no label in the past
        if (newLabels.isEmpty()) {
          //如果新节点没有标签
          // update labels
          //将该节点资源加入到 NO_LABEL（无标签）的 RMNodeLabel 资源集合
          RMNodeLabel label = labelCollections.get(NO_LABEL);
          label.addNode(newNM.resource);

          // update queues, all queue can access this node
          //增加所有队列的资源，因为无标签节点对所有队列可用
          for (Queue q : queueCollections.values()) {
            Resources.addTo(q.resource, newNM.resource);
          }
        } else {
          // update labels
          //如果新节点有标签
          for (String labelName : newLabels) {
            //将该节点资源加入到相应标签的 RMNodeLabel 资源集合
            RMNodeLabel label = labelCollections.get(labelName);
            label.addNode(newNM.resource);
          }

          // update queues, only queue can access this node will be subtract
          //仅为可以使用该标签的 Queue 增加资源
          for (Queue q : queueCollections.values()) {
            if (isNodeUsableByQueue(newLabels, q)) {
              Resources.addTo(q.resource, newNM.resource);
            }
          }
        }
      }
    }
    
    // Notify RM
    //通知 YARN 资源管理器
    if (rmContext != null && rmContext.getDispatcher() != null) {
      rmContext.getDispatcher().getEventHandler().handle(
          new NodeLabelsUpdateSchedulerEvent(newNodeToLabelsMap));
    }
  }
  
  public Resource getResourceByLabel(String label, Resource clusterResource) {
    label = normalizeLabel(label);
    if (label.equals(NO_LABEL)) {
      return noNodeLabel.getResource();
    }
    readLock.lock();
    try {
      RMNodeLabel nodeLabel = labelCollections.get(label);
      if (nodeLabel == null) {
        return Resources.none();
      }
      return nodeLabel.getResource();
    } finally {
      readLock.unlock();
    }
  }

  private boolean isNodeUsableByQueue(Set<String> nodeLabels, Queue q) {
    // node without any labels can be accessed by any queue
    if (nodeLabels == null || nodeLabels.isEmpty()
        || (nodeLabels.size() == 1 && nodeLabels.contains(NO_LABEL))) {
      return true;
    }

    for (String label : nodeLabels) {
      if (q.accessibleNodeLabels.contains(label)) {
        return true;
      }
    }

    return false;
  }

  private Map<String, Host> cloneNodeMap() {
    Set<NodeId> nodesToCopy = new HashSet<NodeId>();
    for (String nodeName : nodeCollections.keySet()) {
      nodesToCopy.add(NodeId.newInstance(nodeName, WILDCARD_PORT));
    }
    return cloneNodeMap(nodesToCopy);
  }

  public boolean checkAccess(UserGroupInformation user) {
    // make sure only admin can invoke
    // this method
    if (authorizer.isAdmin(user)) {
      return true;
    }
    return false;
  }
  
  public void setRMContext(RMContext rmContext) {
    this.rmContext = rmContext;
  }

  public List<RMNodeLabel> pullRMNodeLabelsInfo() {
    readLock.lock();
    try {
      List<RMNodeLabel> infos = new ArrayList<RMNodeLabel>();

      for (Entry<String, RMNodeLabel> entry : labelCollections.entrySet()) {
        RMNodeLabel label = entry.getValue();
        infos.add(label.getCopy());
      }

      Collections.sort(infos);
      return infos;
    } finally {
      readLock.unlock();
    }
  }
}
