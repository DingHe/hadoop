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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * It contains allocation information for one application within a period of
 * time.
 * Each application allocation may have several allocation attempts.
 */
// AppAllocation 类用于记录单个应用程序在一段时间内的资源分配情况。
// 每个应用的资源分配可能会有多个尝试（allocation attempts），该类用于存储这些尝试的详细信息，并提供相关的管理和查询方法。
// 它主要用于 YARN 资源管理器的调度过程中，帮助追踪应用程序的资源申请、分配状态以及失败或成功的分配尝试
public class AppAllocation {
  private Priority priority; //该应用的优先级，影响调度的先后顺序
  private NodeId nodeId; //该应用分配资源的节点 ID
  private ContainerId containerId; //成功分配的容器 ID（如果有）
  private ActivityState activityState;//记录当前应用的调度状态，如 SKIPPED、REJECTED、ALLOCATED 等
  private String diagnostic; //诊断信息，记录分配失败或其他状态变更的原因
  private String queueName; //该应用所属的调度队列名称
  private List<ActivityNode> allocationAttempts; //记录该应用的所有分配尝试，包括每次尝试的状态、节点、请求信息等
  private long timestamp; //记录最近一次状态变更的时间戳

  public AppAllocation(Priority priority, NodeId nodeId, String queueName) {
    this.priority = priority;
    this.nodeId = nodeId;
    this.allocationAttempts = new ArrayList<>();
    this.queueName = queueName;
  }

  public void updateAppContainerStateAndTime(ContainerId cId,
      ActivityState appState, long ts, String diagnostic) {
    this.timestamp = ts;
    this.containerId = cId;
    this.activityState = appState;
    this.diagnostic = diagnostic;
  }

  public void addAppAllocationActivity(String cId, Integer reqPriority,
      ActivityState state, String diagnose, ActivityLevel level, NodeId nId,
      Long allocationRequestId) {
    ActivityNode container = new ActivityNode(cId, null, reqPriority,
        state, diagnose, level, nId, allocationRequestId);
    this.allocationAttempts.add(container);
    if (state == ActivityState.REJECTED) {
      this.activityState = ActivityState.SKIPPED;
    } else {
      this.activityState = state;
    }
  }

  public String getNodeId() {
    return nodeId == null ? null : nodeId.toString();
  }

  public String getQueueName() {
    return queueName;
  }

  public ActivityState getActivityState() {
    return activityState;
  }

  public Priority getPriority() {
    return priority;
  }

  public String getContainerId() {
    if (containerId == null) {
      return null;
    }
    return containerId.toString();
  }

  public String getDiagnostic() {
    return diagnostic;
  }

  public long getTime() {
    return this.timestamp;
  }

  public List<ActivityNode> getAllocationAttempts() {
    return allocationAttempts;
  }
  //requestPriorities：指定需要保留的请求优先级集合
  //allocationRequestIds：指定需要保留的分配请求 ID 集合

  public AppAllocation filterAllocationAttempts(Set<Integer> requestPriorities,
      Set<Long> allocationRequestIds) {
    AppAllocation appAllocation =
        new AppAllocation(this.priority, this.nodeId, this.queueName);
    appAllocation.activityState = this.activityState;
    appAllocation.containerId = this.containerId;
    appAllocation.timestamp = this.timestamp;
    appAllocation.diagnostic = this.diagnostic;
    Predicate<ActivityNode> predicate = (e) ->
        (CollectionUtils.isEmpty(requestPriorities) || requestPriorities
            .contains(e.getRequestPriority())) && (
            CollectionUtils.isEmpty(allocationRequestIds)
                || allocationRequestIds.contains(e.getAllocationRequestId()));
    appAllocation.allocationAttempts =
        this.allocationAttempts.stream().filter(predicate)
            .collect(Collectors.toList());
    return appAllocation;
  }

  public void setAllocationAttempts(List<ActivityNode> allocationAttempts) {
    this.allocationAttempts = allocationAttempts;
  }
}
