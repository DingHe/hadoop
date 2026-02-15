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

package org.apache.hadoop.yarn.nodelabels;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.nodelabels.event.NodeLabelsStoreEvent;
import org.apache.hadoop.yarn.nodelabels.event.NodeLabelsStoreEventType;
import org.apache.hadoop.yarn.nodelabels.event.RemoveClusterNodeLabels;
import org.apache.hadoop.yarn.nodelabels.event.StoreNewClusterNodeLabels;
import org.apache.hadoop.yarn.nodelabels.event.UpdateNodeToLabelsMappingsEvent;
import org.apache.hadoop.yarn.util.resource.Resources;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
//主要负责管理YARN集群中的节点标签（Node Labels）。节点标签是YARN用于资源调度的一个重要特性，允许管理员对集群中的节点进行分类，
// 并为不同的应用分配特定的节点资源。例如，可以将某些节点标记为“GPU”，然后让某些任务仅在这些节点上运行。
//该类的核心功能包括：
//管理全局节点标签（添加、删除、查询节点标签）。
//维护节点与标签的映射关系。
//提供并发访问控制，确保多个线程可以安全地读取/修改标签数据。
//支持持久化存储，使节点标签配置在 YARN 重启后仍然有效。
@Private
public class CommonNodeLabelsManager extends AbstractService {
  protected static final Logger LOG =
      LoggerFactory.getLogger(CommonNodeLabelsManager.class);
  public static final Set<String> EMPTY_STRING_SET = Collections
      .unmodifiableSet(new HashSet<String>(0)); //空的字符串集合
  public static final Set<NodeLabel> EMPTY_NODELABEL_SET = Collections
      .unmodifiableSet(new HashSet<NodeLabel>(0));//空的 NodeLabel 集合，用于返回默认空值，避免 null 指针异常
  public static final String ANY = "*"; //表示某个操作可以适用于任何节点标签
  public static final Set<String> ACCESS_ANY_LABEL_SET = ImmutableSet.of(ANY); //包含 ANY 的不可变集合，表示一个可以访问任何标签的集合
  public static final int WILDCARD_PORT = 0;
  // Flag to identify startup for removelabel
  //指示是否正在进行节点标签存储的初始化。如果为 true，说明系统在启动过程中可能会移除旧的节点标签
  private boolean initNodeLabelStoreInProgress = false;

  /**
   * Error messages
   */
  //该错误消息用于提示管理员，如果 YARN 未启用节点标签调度
  @VisibleForTesting
  public static final String NODE_LABELS_NOT_ENABLED_ERR =
      "Node-label-based scheduling is disabled. Please check "
          + YarnConfiguration.NODE_LABELS_ENABLED;

  /**
   * If a user doesn't specify label of a queue or node, it belongs
   * DEFAULT_LABEL
   */
  //默认的无标签值（空字符串 ""），表示如果某个队列或节点未指定标签，则默认属于 NO_LABEL
  public static final String NO_LABEL = "";

  protected Dispatcher dispatcher;
  //存储全局的 NodeLabel（节点标签）信息，key 是标签名称，value 是 RMNodeLabel（表示 YARN 资源管理器中的标签信息）
  protected ConcurrentMap<String, RMNodeLabel> labelCollections =
      new ConcurrentHashMap<String, RMNodeLabel>();
  //存储主机与标签的映射关系，key 是主机名，value 是 Host 对象，该对象记录了该主机上的节点标签
  protected ConcurrentMap<String, Host> nodeCollections =
      new ConcurrentHashMap<String, Host>();
  //用于记录 节点标签是否来自于主机级别，key 是 NodeId（节点 ID），value 是 Boolean，true 表示该节点的标签是从主机继承的
  private ConcurrentMap<NodeId, Boolean> isNodeLabelFromHost =
      new ConcurrentHashMap<NodeId, Boolean>();
  //默认的 RMNodeLabel 实例，表示无标签的情况（NO_LABEL）
  protected RMNodeLabel noNodeLabel;

  protected final ReadLock readLock;
  protected final WriteLock writeLock;
  //用于持久化存储节点标签
  protected NodeLabelsStore store;
  //是否启用了节点标签功能，默认 false，需要通过 YarnConfiguration.NODE_LABELS_ENABLED 配置
  private boolean nodeLabelsEnabled = false;
  //是否采用集中式的标签配置管理，默认 true，意味着标签配置由 ResourceManager 统一管理
  private boolean isCentralizedNodeLabelConfiguration = true;

  /**
   * A <code>Host</code> can have multiple <code>Node</code>s 
   */
  //表示 集群中的一台物理主机。该主机可以包含多个 Node（YARN 计算节点），并且可以被分配多个标签（labels）
  public static class Host {
    public Set<String> labels;//存储该主机上的标签
    public Map<NodeId, Node> nms;//存储该主机上的所有 Node（YARN 计算节点）
    
    protected Host() {
      labels =
          Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
      nms = new ConcurrentHashMap<NodeId, Node>();
    }
    
    public Host copy() {
      Host c = new Host();
      c.labels = new HashSet<String>(labels);
      for (Entry<NodeId, Node> entry : nms.entrySet()) {
        c.nms.put(entry.getKey(), entry.getValue().copy());
      }
      return c;
    }
  }
  //表示 YARN 集群中的一个计算节点，用于管理该节点的资源、状态和标签等信息
  protected static class Node {
    public Set<String> labels;//存储当前计算节点的 标签集合
    public Resource resource;//存储当前计算节点的 资源信息（CPU、内存等）
    public boolean running;//表示当前节点是否 处于运行状态
    public NodeId nodeId;//存储当前 Node 的唯一标识
    
    protected Node(NodeId nodeid) {
      labels = null;
      resource = Resource.newInstance(0, 0);
      running = false;
      nodeId = nodeid;
    }
    
    public Node copy() {
      Node c = new Node(nodeId);
      if (labels != null) {
        c.labels =
            Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
        c.labels.addAll(labels);
      } else {
        c.labels = null;
      }
      c.resource = Resources.clone(resource);
      c.running = running;
      return c;
    }
  }
  
  private enum NodeLabelUpdateOperation {
    ADD,
    REMOVE,
    REPLACE
  }
  //事件handle
  private final class ForwardingEventHandler implements
      EventHandler<NodeLabelsStoreEvent> {

    @Override
    public void handle(NodeLabelsStoreEvent event) {
      handleStoreEvent(event);
    }
  }
  
  // Dispatcher related code
  protected void handleStoreEvent(NodeLabelsStoreEvent event) {
    try {
      switch (event.getType()) {
      case ADD_LABELS:
        StoreNewClusterNodeLabels storeNewClusterNodeLabelsEvent =
            (StoreNewClusterNodeLabels) event;
        store.storeNewClusterNodeLabels(storeNewClusterNodeLabelsEvent
             .getLabels());
        break;
      case REMOVE_LABELS:
        RemoveClusterNodeLabels removeClusterNodeLabelsEvent =
            (RemoveClusterNodeLabels) event;
        store.removeClusterNodeLabels(removeClusterNodeLabelsEvent.getLabels());
        break;
      case STORE_NODE_TO_LABELS:
        UpdateNodeToLabelsMappingsEvent updateNodeToLabelsMappingsEvent =
            (UpdateNodeToLabelsMappingsEvent) event;
        store.updateNodeToLabelsMappings(updateNodeToLabelsMappingsEvent
            .getNodeToLabels());
        break;
      }
    } catch (IOException e) {
      LOG.error("Failed to store label modification to storage");
      throw new YarnRuntimeException(e);
    }
  }

  public CommonNodeLabelsManager() {
    super(CommonNodeLabelsManager.class.getName());
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();
  }

  // for UT purpose
  protected void initDispatcher(Configuration conf) {
    // create async handler
    dispatcher = new AsyncDispatcher("NodeLabelManager dispatcher");
    AsyncDispatcher asyncDispatcher = (AsyncDispatcher) dispatcher;
    asyncDispatcher.init(conf);
    asyncDispatcher.setDrainEventsOnStop();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // set if node labels enabled
    nodeLabelsEnabled = YarnConfiguration.areNodeLabelsEnabled(conf);

    isCentralizedNodeLabelConfiguration  =
        YarnConfiguration.isCentralizedNodeLabelConfiguration(conf);

    noNodeLabel = new RMNodeLabel(NO_LABEL);
    labelCollections.put(NO_LABEL, noNodeLabel);
  }

  /**
   * @return the isStartup
   */
  protected boolean isInitNodeLabelStoreInProgress() {
    return initNodeLabelStoreInProgress;
  }

  /**
   * @return true if node label configuration type is not distributed.
   */
  public boolean isCentralizedConfiguration() {
    return isCentralizedNodeLabelConfiguration;
  }

  protected void initNodeLabelStore(Configuration conf) throws Exception {
    this.store =
        ReflectionUtils
            .newInstance(
                conf.getClass(YarnConfiguration.FS_NODE_LABELS_STORE_IMPL_CLASS,
                    FileSystemNodeLabelsStore.class, NodeLabelsStore.class),
                conf);
    this.store.init(conf, this);
    this.store.recover();
  }

  // for UT purpose
  protected void startDispatcher() {
    // start dispatcher
    AsyncDispatcher asyncDispatcher = (AsyncDispatcher) dispatcher;
    asyncDispatcher.start();
  }

  @Override
  protected void serviceStart() throws Exception {
    if (nodeLabelsEnabled) {
      setInitNodeLabelStoreInProgress(true);
      initNodeLabelStore(getConfig());
      setInitNodeLabelStoreInProgress(false);
    }
    
    // init dispatcher only when service start, because recover will happen in
    // service init, we don't want to trigger any event handling at that time.
    initDispatcher(getConfig());

    if (null != dispatcher) {
      dispatcher.register(NodeLabelsStoreEventType.class,
          new ForwardingEventHandler());
    }
    
    startDispatcher();
  }
  
  // for UT purpose
  protected void stopDispatcher() {
    AsyncDispatcher asyncDispatcher = (AsyncDispatcher) dispatcher;
    if (null != asyncDispatcher) {
      asyncDispatcher.stop();
    }
  }
  
  @Override
  protected void serviceStop() throws Exception {
    // finalize store
    stopDispatcher();
    
    // only close store when we enabled store persistent
    if (null != store) {
      store.close();
    }
  }

  @SuppressWarnings("unchecked")
  public void addToCluserNodeLabels(Collection<NodeLabel> labels)
      throws IOException {
    if (!nodeLabelsEnabled) {
      LOG.error(NODE_LABELS_NOT_ENABLED_ERR);
      throw new IOException(NODE_LABELS_NOT_ENABLED_ERR);
    }
    if (null == labels || labels.isEmpty()) {
      return;
    }
    List<NodeLabel> newLabels = new ArrayList<NodeLabel>();
    normalizeNodeLabels(labels);
    // check any mismatch in exclusivity no mismatch with skip
    checkExclusivityMatch(labels);
    // do a check before actual adding them, will throw exception if any of them
    // doesn't meet label name requirement
    for (NodeLabel label : labels) {
      NodeLabelUtil.checkAndThrowLabelName(label.getName());
    }

    for (NodeLabel label : labels) {
      // shouldn't overwrite it to avoid changing the Label.resource
      if (this.labelCollections.get(label.getName()) == null) {
        this.labelCollections.put(label.getName(), new RMNodeLabel(label));
        newLabels.add(label);
      }
    }
    if (null != dispatcher && !newLabels.isEmpty()) {
      dispatcher.getEventHandler().handle(
          new StoreNewClusterNodeLabels(newLabels));
    }

    LOG.info("Add labels: [" + StringUtils.join(labels.iterator(), ",") + "]");
  }

  /**
   * Add multiple node labels to repository.
   *
   * @param labels
   *          new node labels added
   * @throws IOException io error occur.
   */
  @VisibleForTesting
  public void addToCluserNodeLabelsWithDefaultExclusivity(Set<String> labels)
      throws IOException {
    Set<NodeLabel> nodeLabels = new HashSet<NodeLabel>();
    for (String label : labels) {
      nodeLabels.add(NodeLabel.newInstance(label));
    }
    addToCluserNodeLabels(nodeLabels);
  }
  
  protected void checkAddLabelsToNode(
      Map<NodeId, Set<String>> addedLabelsToNode) throws IOException {
    if (null == addedLabelsToNode || addedLabelsToNode.isEmpty()) {
      return;
    }

    // check all labels being added existed
    Set<String> knownLabels = labelCollections.keySet();
    for (Entry<NodeId, Set<String>> entry : addedLabelsToNode.entrySet()) {
      NodeId nodeId = entry.getKey();
      Set<String> labels = entry.getValue();
      
      if (!knownLabels.containsAll(labels)) {
        String msg =
            "Not all labels being added contained by known "
                + "label collections, please check" + ", added labels=["
                + StringUtils.join(labels, ",") + "]";
        LOG.error(msg);
        throw new IOException(msg);
      }
      
      // In YARN-2694, we temporarily disable user add more than 1 labels on a
      // same host
      if (!labels.isEmpty()) {
        Set<String> newLabels = new HashSet<String>(getLabelsByNode(nodeId));
        newLabels.addAll(labels);
        // we don't allow number of labels on a node > 1 after added labels
        if (newLabels.size() > 1) {
          String msg =
              String.format(
                      "%d labels specified on host=%s after add labels to node"
                          + ", please note that we do not support specifying multiple"
                          + " labels on a single host for now.",
                      newLabels.size(), nodeId.getHost());
          LOG.error(msg);
          throw new IOException(msg);
        }
      }
    }
  }
  
  /**
   * add more labels to nodes.
   * 
   * @param addedLabelsToNode node {@literal ->} labels map
   * @throws IOException io error occur.
   */
  //添加多个标签到多个节点
  public void addLabelsToNode(Map<NodeId, Set<String>> addedLabelsToNode)
      throws IOException {
    if (!nodeLabelsEnabled) {
      LOG.error(NODE_LABELS_NOT_ENABLED_ERR);
      throw new IOException(NODE_LABELS_NOT_ENABLED_ERR);
    }
    addedLabelsToNode = normalizeNodeIdToLabels(addedLabelsToNode);
    checkAddLabelsToNode(addedLabelsToNode);
    internalUpdateLabelsOnNodes(addedLabelsToNode, NodeLabelUpdateOperation.ADD);
  }
  
  protected void checkRemoveFromClusterNodeLabels(
      Collection<String> labelsToRemove) throws IOException {
    if (null == labelsToRemove || labelsToRemove.isEmpty()) {
      return;
    }

    // Check if label to remove doesn't existed or null/empty, will throw
    // exception if any of labels to remove doesn't meet requirement
    for (String label : labelsToRemove) {
      label = normalizeLabel(label);
      if (label == null || label.isEmpty()) {
        throw new IOException("Label to be removed is null or empty");
      }
      
      if (!labelCollections.containsKey(label)) {
        throw new IOException("Node label=" + label
            + " to be removed doesn't existed in cluster "
            + "node labels collection.");
      }
    }
  }
  
  @SuppressWarnings("unchecked")
  protected void internalRemoveFromClusterNodeLabels(Collection<String> labelsToRemove) {
    // remove labels from nodes
    for (Map.Entry<String,Host> nodeEntry : nodeCollections.entrySet()) {
      Host host = nodeEntry.getValue();
      if (null != host) {
        host.labels.removeAll(labelsToRemove);
        for (Node nm : host.nms.values()) {
          if (nm.labels != null) {
            nm.labels.removeAll(labelsToRemove);
          }
        }
      }
    }

    // remove labels from node labels collection
    for (String label : labelsToRemove) {
      labelCollections.remove(label);
    }

    // create event to remove labels
    if (null != dispatcher) {
      dispatcher.getEventHandler().handle(
          new RemoveClusterNodeLabels(labelsToRemove));
    }

    LOG.info("Remove labels: ["
        + StringUtils.join(labelsToRemove.iterator(), ",") + "]");
  }

  /**
   * Remove multiple node labels from repository
   * 
   * @param labelsToRemove
   *          node labels to remove
   * @throws IOException io error occur.
   */
  public void removeFromClusterNodeLabels(Collection<String> labelsToRemove)
      throws IOException {
    if (!nodeLabelsEnabled) {
      LOG.error(NODE_LABELS_NOT_ENABLED_ERR);
      throw new IOException(NODE_LABELS_NOT_ENABLED_ERR);
    }
    
    labelsToRemove = normalizeLabels(labelsToRemove);
    
    checkRemoveFromClusterNodeLabels(labelsToRemove);

    internalRemoveFromClusterNodeLabels(labelsToRemove);
  }
  
  protected void checkRemoveLabelsFromNode(
      Map<NodeId, Set<String>> removeLabelsFromNode) throws IOException {
    // check all labels being added existed
    Set<String> knownLabels = labelCollections.keySet();
    for (Entry<NodeId, Set<String>> entry : removeLabelsFromNode.entrySet()) {
      NodeId nodeId = entry.getKey();
      Set<String> labels = entry.getValue();
      
      if (!knownLabels.containsAll(labels)) {
        String msg =
            "Not all labels being removed contained by known "
                + "label collections, please check" + ", removed labels=["
                + StringUtils.join(labels, ",") + "]";
        LOG.error(msg);
        throw new IOException(msg);
      }
      
      Set<String> originalLabels = null;
      
      boolean nodeExisted = false;
      if (WILDCARD_PORT != nodeId.getPort()) {
        Node nm = getNMInNodeSet(nodeId);
        if (nm != null) {
          originalLabels = nm.labels;
          nodeExisted = true;
        }
      } else {
        Host host = nodeCollections.get(nodeId.getHost());
        if (null != host) {
          originalLabels = host.labels;
          nodeExisted = true;
        }
      }
      
      if (!nodeExisted) {
        String msg =
            "Try to remove labels from NM=" + nodeId
                + ", but the NM doesn't existed";
        LOG.error(msg);
        throw new IOException(msg);
      }

      // the labels will never be null
      if (labels.isEmpty()) {
        continue;
      }

      // originalLabels may be null,
      // because when a Node is created, Node.labels can be null.
      if (originalLabels == null || !originalLabels.containsAll(labels)) {
        String msg =
            "Try to remove labels = [" + StringUtils.join(labels, ",")
                + "], but not all labels contained by NM=" + nodeId;
        LOG.error(msg);
        throw new IOException(msg);
      }
    }
  }

  private void addNodeToLabels(NodeId node, Set<String> labels) {
    for(String l : labels) {
      labelCollections.get(l).addNodeId(node);
    }
  }

  protected void removeNodeFromLabels(NodeId node, Set<String> labels) {
    for(String l : labels) {
      labelCollections.get(l).removeNodeId(node);
    }
  }

  private void replaceNodeForLabels(NodeId node, Set<String> oldLabels,
      Set<String> newLabels) {
    if(oldLabels != null) {
      removeNodeFromLabels(node, oldLabels);
    }
    addNodeToLabels(node, newLabels);
  }

  private void addLabelsToNodeInHost(NodeId node, Set<String> labels)
       throws IOException {
    Host host = nodeCollections.get(node.getHost());
    if (null == host) {
      throw new IOException("Cannot add labels to a host that "
              + "does not exist. Create the host before adding labels to it.");
    }
    Node nm = host.nms.get(node);
    if (nm != null) {
      Node newNm = nm.copy();
      if (newNm.labels == null) {
        newNm.labels =
            Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
      }
      newNm.labels.addAll(labels);
      host.nms.put(node, newNm);
    }
  }

  protected void removeLabelsFromNodeInHost(NodeId node, Set<String> labels)
      throws IOException {
    Host host = nodeCollections.get(node.getHost());
    if (null == host) {
      throw new IOException("Cannot remove labels from a host that "
              + "does not exist. Create the host before adding labels to it.");
    }
    Node nm = host.nms.get(node);
    if (nm != null) {
      if (nm.labels == null) {
        nm.labels = new HashSet<String>();
      } else {
        nm.labels.removeAll(labels);
      }
    }
  }

  private void replaceLabelsForNode(NodeId node, Set<String> oldLabels,
      Set<String> newLabels) throws IOException {
    if(oldLabels != null) {
      removeLabelsFromNodeInHost(node, oldLabels);
    }
    addLabelsToNodeInHost(node, newLabels);
  }

  protected boolean isNodeLabelExplicit(NodeId nodeId) {
    return !isNodeLabelFromHost.containsKey(nodeId) ||
        isNodeLabelFromHost.get(nodeId);
  }

  @SuppressWarnings("unchecked")
  protected void internalUpdateLabelsOnNodes(
      Map<NodeId, Set<String>> nodeToLabels, NodeLabelUpdateOperation op)
      throws IOException {
    // do update labels from nodes
    Map<NodeId, Set<String>> newNMToLabels =
        new HashMap<NodeId, Set<String>>();
    Set<String> oldLabels;
    for (Entry<NodeId, Set<String>> entry : nodeToLabels.entrySet()) {
      NodeId nodeId = entry.getKey();
      Set<String> labels = entry.getValue();
      
      createHostIfNonExisted(nodeId.getHost());
      if (nodeId.getPort() == WILDCARD_PORT) {
        Host host = nodeCollections.get(nodeId.getHost());
        switch (op) {
        case REMOVE:
          removeNodeFromLabels(nodeId, labels);
          host.labels.removeAll(labels);
          for (Node node : host.nms.values()) {
            if (node.labels != null) {
              node.labels.removeAll(labels);
            }
            removeNodeFromLabels(node.nodeId, labels);
          }
          break;
        case ADD:
          addNodeToLabels(nodeId, labels);
          host.labels.addAll(labels);
          for (Node node : host.nms.values()) {
            if (node.labels != null) {
              node.labels.addAll(labels);
            }
            addNodeToLabels(node.nodeId, labels);
            isNodeLabelFromHost.put(node.nodeId, true);
          }
          break;
        case REPLACE:
          replaceNodeForLabels(nodeId, host.labels, labels);
          replaceLabelsForNode(nodeId, host.labels, labels);
          host.labels.clear();
          host.labels.addAll(labels);
          for (Node node : host.nms.values()) {
            replaceNodeForLabels(node.nodeId, node.labels, labels);
            replaceLabelsForNode(node.nodeId, node.labels, labels);
            node.labels = null;
            isNodeLabelFromHost.put(node.nodeId, true);
          }
          break;
        default:
          break;
        }
        newNMToLabels.put(nodeId, host.labels);
      } else {
        if (EnumSet.of(NodeLabelUpdateOperation.ADD,
            NodeLabelUpdateOperation.REPLACE).contains(op)) {
          // Add and replace
          createNodeIfNonExisted(nodeId);
          Node nm = getNMInNodeSet(nodeId);
          switch (op) {
          case ADD:
            addNodeToLabels(nodeId, labels);
            if (nm.labels == null) { 
              nm.labels = new HashSet<String>();
            }
            nm.labels.addAll(labels);
            isNodeLabelFromHost.put(nm.nodeId, false);
            break;
          case REPLACE:
            oldLabels = getLabelsByNode(nodeId);
            replaceNodeForLabels(nodeId, oldLabels, labels);
            replaceLabelsForNode(nodeId, oldLabels, labels);
            if (nm.labels == null) { 
              nm.labels = new HashSet<String>();
            }
            nm.labels.clear();
            nm.labels.addAll(labels);
            isNodeLabelFromHost.put(nm.nodeId, false);
            break;
          default:
            break;
          }
          newNMToLabels.put(nodeId, nm.labels);
        } else {
          // remove
          removeNodeFromLabels(nodeId, labels);
          Node nm = getNMInNodeSet(nodeId);
          if (nm.labels != null) {
            nm.labels.removeAll(labels);
            newNMToLabels.put(nodeId, nm.labels);
          }
        }
      }
    }
    
    if (null != dispatcher && isCentralizedNodeLabelConfiguration) {
      // In case of DistributedNodeLabelConfiguration or
      // DelegatedCentralizedNodeLabelConfiguration, no need to save the
      // NodeLabels Mapping to the back-end store, as on RM restart/failover
      // NodeLabels are collected from NM through Register/Heartbeat again
      // in case of DistributedNodeLabelConfiguration and collected from
      // RMNodeLabelsMappingProvider in case of
      // DelegatedCentralizedNodeLabelConfiguration
      dispatcher.getEventHandler().handle(
          new UpdateNodeToLabelsMappingsEvent(newNMToLabels));
    }

    // shows node->labels we added
    LOG.info(op.name() + " labels on nodes:");
    for (Entry<NodeId, Set<String>> entry : newNMToLabels.entrySet()) {
      LOG.info("  NM=" + entry.getKey() + ", labels=["
          + StringUtils.join(entry.getValue().iterator(), ",") + "]");
    }
  }
  
  /**
   * remove labels from nodes, labels being removed most be contained by these
   * nodes.
   * 
   * @param removeLabelsFromNode node {@literal ->} labels map
   * @throws IOException io error occur.
   */
  public void
      removeLabelsFromNode(Map<NodeId, Set<String>> removeLabelsFromNode)
          throws IOException {
    if (!nodeLabelsEnabled) {
      LOG.error(NODE_LABELS_NOT_ENABLED_ERR);
      throw new IOException(NODE_LABELS_NOT_ENABLED_ERR);
    }
    
    removeLabelsFromNode = normalizeNodeIdToLabels(removeLabelsFromNode);
    
    checkRemoveLabelsFromNode(removeLabelsFromNode);

    internalUpdateLabelsOnNodes(removeLabelsFromNode,
        NodeLabelUpdateOperation.REMOVE);
  }
  
  protected void checkReplaceLabelsOnNode(
      Map<NodeId, Set<String>> replaceLabelsToNode) throws IOException {
    if (null == replaceLabelsToNode || replaceLabelsToNode.isEmpty()) {
      return;
    }
    
    // check all labels being added existed
    Set<String> knownLabels = labelCollections.keySet();
    for (Entry<NodeId, Set<String>> entry : replaceLabelsToNode.entrySet()) {
      NodeId nodeId = entry.getKey();
      Set<String> labels = entry.getValue();
      
      // As in YARN-2694, we disable user add more than 1 labels on a same host
      if (labels.size() > 1) {
        String msg = String.format("%d labels specified on host=%s"
            + ", please note that we do not support specifying multiple"
            + " labels on a single host for now.", labels.size(),
            nodeId.getHost());
        LOG.error(msg);
        throw new IOException(msg);
      }
      
      if (!knownLabels.containsAll(labels)) {
        String msg =
            "Not all labels being replaced contained by known "
                + "label collections, please check" + ", new labels=["
                + StringUtils.join(labels, ",") + "]";
        LOG.error(msg);
        throw new IOException(msg);
      }
    }
  }

  /**
   * replace labels to nodes
   * 
   * @param replaceLabelsToNode node {@literal ->} labels map
   * @throws IOException io error occur.
   */
  public void replaceLabelsOnNode(Map<NodeId, Set<String>> replaceLabelsToNode)
      throws IOException {
    if (!nodeLabelsEnabled) {
      LOG.error(NODE_LABELS_NOT_ENABLED_ERR);
      throw new IOException(NODE_LABELS_NOT_ENABLED_ERR);
    }
    
    replaceLabelsToNode = normalizeNodeIdToLabels(replaceLabelsToNode);
    
    checkReplaceLabelsOnNode(replaceLabelsToNode);

    internalUpdateLabelsOnNodes(replaceLabelsToNode,
        NodeLabelUpdateOperation.REPLACE);
  }

  /**
   * Get mapping of nodes to labels
   * 
   * @return nodes to labels map
   */
  public Map<NodeId, Set<String>> getNodeLabels() {
    Map<NodeId, Set<String>> nodeToLabels =
        generateNodeLabelsInfoPerNode(String.class);
    return nodeToLabels;
  }

  /**
   * Get mapping of nodes to label info
   *
   * @return nodes to labels map
   */
  public Map<NodeId, Set<NodeLabel>> getNodeLabelsInfo() {
    Map<NodeId, Set<NodeLabel>> nodeToLabels =
        generateNodeLabelsInfoPerNode(NodeLabel.class);
    return nodeToLabels;
  }

  @SuppressWarnings("unchecked")
  private <T> Map<NodeId, Set<T>> generateNodeLabelsInfoPerNode(Class<T> type) {
    readLock.lock();
    try {
      Map<NodeId, Set<T>> nodeToLabels = new HashMap<>();
      for (Entry<String, Host> entry : nodeCollections.entrySet()) {
        String hostName = entry.getKey();
        Host host = entry.getValue();
        for (NodeId nodeId : host.nms.keySet()) {
          if (type.isAssignableFrom(String.class)) {
            Set<String> nodeLabels = getLabelsByNode(nodeId);
            if (nodeLabels == null || nodeLabels.isEmpty()) {
              continue;
            }
            nodeToLabels.put(nodeId, (Set<T>) nodeLabels);
          } else {
            Set<NodeLabel> nodeLabels = getLabelsInfoByNode(nodeId);
            if (nodeLabels == null || nodeLabels.isEmpty()) {
              continue;
            }
            nodeToLabels.put(nodeId, (Set<T>) nodeLabels);
          }
        }
        if (!host.labels.isEmpty()) {
          if (type.isAssignableFrom(String.class)) {
            nodeToLabels.put(NodeId.newInstance(hostName, WILDCARD_PORT),
                (Set<T>) host.labels);
          } else {
            nodeToLabels.put(NodeId.newInstance(hostName, WILDCARD_PORT),
                (Set<T>) createNodeLabelFromLabelNames(host.labels));
          }
        }
      }
      return Collections.unmodifiableMap(nodeToLabels);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Get nodes that have no labels.
   *
   * @return set of nodes with no labels
   */
  public Set<NodeId> getNodesWithoutALabel() {
    readLock.lock();
    try {
      Set<NodeId> nodes = new HashSet<>();
      for (Host host : nodeCollections.values()) {
        for (NodeId nodeId : host.nms.keySet()) {
          if (getLabelsByNode(nodeId).isEmpty()) {
            nodes.add(nodeId);
          }
        }
      }
      return Collections.unmodifiableSet(nodes);
    } finally {
      readLock.unlock();
    }
  }


  /**
   * Get mapping of labels to nodes for all the labels.
   *
   * @return labels to nodes map
   */
  public Map<String, Set<NodeId>> getLabelsToNodes() {
    readLock.lock();
    try {
      return getLabelsToNodes(labelCollections.keySet());
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Get mapping of labels to nodes for specified set of labels.
   *
   * @param labels set of labels for which labels to nodes mapping will be
   *        returned.
   * @return labels to nodes map
   */
  public Map<String, Set<NodeId>> getLabelsToNodes(Set<String> labels) {
    readLock.lock();
    try {
      Map<String, Set<NodeId>> labelsToNodes = getLabelsToNodesMapping(labels,
          String.class);
      return Collections.unmodifiableMap(labelsToNodes);
    } finally {
      readLock.unlock();
    }
  }


  /**
   * Get mapping of labels to nodes for all the labels.
   *
   * @return labels to nodes map
   */
  public Map<NodeLabel, Set<NodeId>> getLabelsInfoToNodes() {
    readLock.lock();
    try {
      return getLabelsInfoToNodes(labelCollections.keySet());
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Get mapping of labels info to nodes for specified set of labels.
   *
   * @param labels
   *          set of nodelabels for which labels to nodes mapping will be
   *          returned.
   * @return labels to nodes map
   */
  public Map<NodeLabel, Set<NodeId>> getLabelsInfoToNodes(Set<String> labels) {
    readLock.lock();
    try {
      Map<NodeLabel, Set<NodeId>> labelsToNodes = getLabelsToNodesMapping(
          labels, NodeLabel.class);
      return Collections.unmodifiableMap(labelsToNodes);
    } finally {
      readLock.unlock();
    }
  }

  private <T> Map<T, Set<NodeId>> getLabelsToNodesMapping(Set<String> labels,
      Class<T> type) {
    Map<T, Set<NodeId>> labelsToNodes = new HashMap<T, Set<NodeId>>();
    for (String label : labels) {
      if (label.equals(NO_LABEL)) {
        continue;
      }
      RMNodeLabel nodeLabelInfo = labelCollections.get(label);
      if (nodeLabelInfo != null) {
        Set<NodeId> nodeIds = nodeLabelInfo.getAssociatedNodeIds();
        if (!nodeIds.isEmpty()) {
          if (type.isAssignableFrom(String.class)) {
            labelsToNodes.put(type.cast(label), nodeIds);
          } else {
            labelsToNodes.put(type.cast(nodeLabelInfo.getNodeLabel()), nodeIds);
          }
        }
      } else {
        LOG.warn("getLabelsToNodes : Label [" + label + "] cannot be found");
      }
    }
    return labelsToNodes;
  }

  /**
   * Get existing valid labels in repository
   * 
   * @return existing valid labels in repository
   */
  public Set<String> getClusterNodeLabelNames() {
    readLock.lock();
    try {
      Set<String> labels = new HashSet<String>(labelCollections.keySet());
      labels.remove(NO_LABEL);
      return Collections.unmodifiableSet(labels);
    } finally {
      readLock.unlock();
    }
  }
  
  public List<NodeLabel> getClusterNodeLabels() {
    readLock.lock();
    try {
      List<NodeLabel> nodeLabels = new ArrayList<>();
      for (RMNodeLabel label : labelCollections.values()) {
        if (!label.getLabelName().equals(NO_LABEL)) {
          nodeLabels.add(NodeLabel.newInstance(label.getLabelName(),
              label.getIsExclusive()));
        }
      }
      return nodeLabels;
    } finally {
      readLock.unlock();
    }
  }

  public boolean isExclusiveNodeLabel(String nodeLabel) throws IOException {
    if (nodeLabel.equals(NO_LABEL)) {
      return noNodeLabel.getIsExclusive();
    }
    readLock.lock();
    try {
      RMNodeLabel label = labelCollections.get(nodeLabel);
      if (label == null) {
        String message =
          "Getting is-exclusive-node-label, node-label = " + nodeLabel
            + ", is not existed.";
        LOG.error(message);
        throw new IOException(message);
      }
      return label.getIsExclusive();
    } finally {
      readLock.unlock();
    }
  }

  private void checkExclusivityMatch(Collection<NodeLabel> labels)
      throws IOException {
    ArrayList<NodeLabel> mismatchlabels = new ArrayList<NodeLabel>();
    for (NodeLabel label : labels) {
      RMNodeLabel rmNodeLabel = this.labelCollections.get(label.getName());
      if (rmNodeLabel != null
          && rmNodeLabel.getIsExclusive() != label.isExclusive()) {
        mismatchlabels.add(label);
      }
    }
    if (mismatchlabels.size() > 0) {
      throw new IOException(
          "Exclusivity cannot be modified for an existing label with : "
              + StringUtils.join(mismatchlabels.iterator(), ","));
    }
  }

  protected String normalizeLabel(String label) {
    if (label != null) {
      return label.trim();
    }
    return NO_LABEL;
  }

  private Set<String> normalizeLabels(Collection<String> labels) {
    Set<String> newLabels = new HashSet<String>();
    for (String label : labels) {
      newLabels.add(normalizeLabel(label));
    }
    return newLabels;
  }
  
  private void normalizeNodeLabels(Collection<NodeLabel> labels) {
    for (NodeLabel label : labels) {
      label.setName(normalizeLabel(label.getName()));
    }
  }

  protected Node getNMInNodeSet(NodeId nodeId) {
    return getNMInNodeSet(nodeId, nodeCollections);
  }
  
  protected Node getNMInNodeSet(NodeId nodeId, Map<String, Host> map) {
    return getNMInNodeSet(nodeId, map, false);
  }
  //获取Node集合中的Node
  protected Node getNMInNodeSet(NodeId nodeId, Map<String, Host> map,
      boolean checkRunning) {
    Host host = map.get(nodeId.getHost());
    if (null == host) {
      return null;
    }
    Node nm = host.nms.get(nodeId);
    if (null == nm) {
      return null;
    }
    if (checkRunning) {
      return nm.running ? nm : null; 
    }
    return nm;
  }
  
  protected Set<String> getLabelsByNode(NodeId nodeId) {
    return getLabelsByNode(nodeId, nodeCollections);
  }
  
  protected Set<String> getLabelsByNode(NodeId nodeId, Map<String, Host> map) {
    Host host = map.get(nodeId.getHost());
    if (null == host) {
      return EMPTY_STRING_SET;
    }
    Node nm = host.nms.get(nodeId);
    if (null != nm && null != nm.labels) {
      return nm.labels;
    } else {
      return host.labels;
    }
  }
  
  public Set<NodeLabel> getLabelsInfoByNode(NodeId nodeId) {
    readLock.lock();
    try {
      Set<String> labels = getLabelsByNode(nodeId, nodeCollections);
      if (labels.isEmpty()) {
        return EMPTY_NODELABEL_SET;
      }
      Set<NodeLabel> nodeLabels = createNodeLabelFromLabelNames(labels);
      return nodeLabels;
    } finally {
      readLock.unlock();
    }
  }

  private Set<NodeLabel> createNodeLabelFromLabelNames(Set<String> labels) {
    Set<NodeLabel> nodeLabels = new HashSet<NodeLabel>();
    for (String label : labels) {
      if (label.equals(NO_LABEL)) {
        continue;
      }
      RMNodeLabel rmLabel = labelCollections.get(label);
      if (rmLabel == null) {
        continue;
      }
      nodeLabels.add(rmLabel.getNodeLabel());
    }
    return nodeLabels;
  }

  protected void createNodeIfNonExisted(NodeId nodeId) throws IOException {
    Host host = nodeCollections.get(nodeId.getHost());
    if (null == host) {
      throw new IOException("Should create host before creating node.");
    }
    Node nm = host.nms.get(nodeId);
    if (null == nm) {
      host.nms.put(nodeId, new Node(nodeId));
    }
  }
  
  protected void createHostIfNonExisted(String hostName) {
    Host host = nodeCollections.get(hostName);
    if (null == host) {
      host = new Host();
      nodeCollections.put(hostName, host);
    }
  }
  
  protected Map<NodeId, Set<String>> normalizeNodeIdToLabels(
      Map<NodeId, Set<String>> nodeIdToLabels) {
    Map<NodeId, Set<String>> newMap = new TreeMap<NodeId, Set<String>>();
    for (Entry<NodeId, Set<String>> entry : nodeIdToLabels.entrySet()) {
      NodeId id = entry.getKey();
      Set<String> labels = entry.getValue();
      newMap.put(id, normalizeLabels(labels)); 
    }
    return newMap;
  }

  public void setInitNodeLabelStoreInProgress(
      boolean initNodeLabelStoreInProgress) {
    this.initNodeLabelStoreInProgress = initNodeLabelStoreInProgress;
  }
}
