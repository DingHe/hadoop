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

package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import org.apache.hadoop.thirdparty.com.google.common.base.Strings;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityState;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;
import java.util.stream.Collectors;

/*
 * DAO object to display node information in allocation tree.
 * It corresponds to "ActivityNode" class.
 */
//ActivityNodeInfo 用于展示调度器中的单个活动节点（ActivityNode）信息，它对应 ActivityNode 类。
//该类的主要功能：
//表示YARN资源分配树中的一个节点，用于跟踪资源调度的详细信息。
//提供节点的名称、优先级、分配状态、诊断信息，帮助 分析调度决策。
//支持层次结构，可用于 展示调度过程的树形结构。
//支持按照不同维度（如应用、请求）分组，便于 资源调度分析
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class ActivityNodeInfo {
  private String name;  // The name for activity node 该活动节点的名称，通常是 队列、请求 ID、应用 ID
  private Integer appPriority;  //如果该节点是应用（Application），表示该应用的优先级
  private Integer requestPriority; //如果该节点是 请求（Request），表示该请求的优先级
  private Long allocationRequestId; //该资源请求的 唯一 ID，用于区分不同的请求。
  private String allocationState;   //该节点的 分配状态，取自 ActivityState（如 ALLOCATED, REJECTED）。
  private String diagnostic;   //调度失败或跳过时的诊断信息，用于分析调度原因
  private String nodeId; //该请求所在的节点 ID，如果是 null，表示不属于具体节点。

  // Used for groups of activities
  private Integer count;  //组内节点的个数，用于 按维度分组（如多个节点共享相同的请求）
  private List<String> nodeIds; //多个节点的 ID 列表，用于 分组显示。

  protected List<ActivityNodeInfo> children;  //该节点的子节点，表示 调度树中的下一层级。

  ActivityNodeInfo() {
  }

  public ActivityNodeInfo(String name, ActivityState activityState,
      String diagnostic, NodeId nId) {
    this.name = name;
    this.allocationState = activityState.name();
    this.diagnostic = diagnostic;
    setNodeId(nId);
  }

  public ActivityNodeInfo(ActivityState groupActivityState,
      String groupDiagnostic, List<String> groupNodeIds) {
    this.allocationState = groupActivityState.name();
    this.diagnostic = groupDiagnostic;
    this.count = groupNodeIds.size();
    this.nodeIds = groupNodeIds;
  }

  ActivityNodeInfo(ActivityNode node,
      RMWSConsts.ActivitiesGroupBy groupBy) {
    this.name = node.getName();
    setPriority(node);
    setNodeId(node.getNodeId());
    this.allocationState = node.getState().name();
    this.diagnostic = node.getDiagnostic();
    this.requestPriority = node.getRequestPriority();
    this.allocationRequestId = node.getAllocationRequestId();
    // only consider grouping for request type
    if (node.isRequestType()) {
      this.children = ActivitiesUtils
          .getRequestActivityNodeInfos(node.getChildren(), groupBy);
    } else {
      this.children = node.getChildren().stream()
          .map(e -> new ActivityNodeInfo(e, groupBy))
          .collect(Collectors.toList());
    }
  }

  public void setNodeId(NodeId nId) {
    if (nId != null && !Strings.isNullOrEmpty(nId.getHost())) {
      this.nodeId = nId.toString();
    }
  }

  private void setPriority(ActivityNode node) {
    if (node.isAppType()) {
      this.appPriority = node.getAppPriority();
    } else {
      this.requestPriority = node.getRequestPriority();
    }
  }

  public String getNodeId() {
    return nodeId;
  }

  public void setNodeIds(List<String> nodeIds) {
    this.nodeIds = nodeIds;
  }

  public Long getAllocationRequestId() {
    return allocationRequestId;
  }

  public Integer getCount() {
    return count;
  }

  public List<String> getNodeIds() {
    return nodeIds;
  }

  public List<ActivityNodeInfo> getChildren() {
    return children;
  }

  public String getAllocationState() {
    return allocationState;
  }

  public String getName() {
    return name;
  }

  public Integer getAppPriority() {
    return appPriority;
  }

  public Integer getRequestPriority() {
    return requestPriority;
  }

  public String getDiagnostic() {
    return diagnostic;
  }
}
