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

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.AppAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/*
 * DAO object to display application allocation detailed information.
 */
// AppAllocationInfo是一个数据传输对象（DAO），用于展示 YARN 应用程序的资源分配详细信息。
// 它用于YARN资源管理器（ResourceManager）Web UI和REST API，帮助用户查看：
// 应用在哪个节点（Node）分配资源
// 所属的队列
// 应用的优先级
// 分配状态（如 ACCEPTED, ALLOCATED, REJECTED）
// 调度的时间
// 分配失败时的诊断信息
// 应用资源分配的子请求（AppRequestAllocationInfo 列表）
// 该类的数据来源于 AppAllocation，并经过转换，以便前端 UI 进行展示。
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class AppAllocationInfo {
  private String nodeId;          //资源分配所在的 节点 ID（如 host:port）
  private Long timestamp;         //资源分配发生的时间戳（毫秒）
  private String dateTime;        //timestamp 的 可读日期格式（Date.toString()）
  private String queueName;       //应用所在的调度队列（Queue Name）
  private Integer appPriority;    //应用的优先级，数值越高优先级越高。
  private String allocationState; //资源分配的状态，取自ActivityState（如 ACCEPTED, ALLOCATED, REJECTED）
  private String diagnostic;      //调度失败的诊断信息（若分配失败，会包含失败原因）
  private List<AppRequestAllocationInfo> children; //子分配请求列表，表示应用的多个资源请求（如多个容器）

  AppAllocationInfo() {
  }

  AppAllocationInfo(AppAllocation allocation,
      RMWSConsts.ActivitiesGroupBy groupBy) {
    this.children = new ArrayList<>();
    this.nodeId = allocation.getNodeId();
    this.queueName = allocation.getQueueName();
    this.appPriority = allocation.getPriority() == null ?
        null : allocation.getPriority().getPriority();
    this.timestamp = allocation.getTime();
    this.dateTime = new Date(allocation.getTime()).toString();
    this.allocationState = allocation.getActivityState().name();
    this.diagnostic = allocation.getDiagnostic();
    Map<String, List<ActivityNode>> requestToActivityNodes =
        allocation.getAllocationAttempts().stream().collect(Collectors
            .groupingBy((e) -> e.getRequestPriority() + "_" + e
                .getAllocationRequestId(), Collectors.toList()));
    for (List<ActivityNode> requestActivityNodes : requestToActivityNodes
        .values()) {
      AppRequestAllocationInfo requestAllocationInfo =
          new AppRequestAllocationInfo(requestActivityNodes, groupBy);
      this.children.add(requestAllocationInfo);
    }
  }

  public String getNodeId() {
    return nodeId;
  }

  public String getQueueName() {
    return queueName;
  }

  public Integer getAppPriority() {
    return appPriority;
  }

  public Long getTimestamp() {
    return timestamp;
  }

  public String getDateTime() {
    return dateTime;
  }

  public String getAllocationState() {
    return allocationState;
  }

  public List<AppRequestAllocationInfo> getChildren() {
    return children;
  }

  public String getDiagnostic() {
    return diagnostic;
  }
}
