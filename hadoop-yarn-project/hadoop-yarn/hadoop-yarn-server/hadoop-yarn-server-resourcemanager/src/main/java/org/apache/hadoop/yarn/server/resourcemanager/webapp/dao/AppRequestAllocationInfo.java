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

import org.apache.hadoop.thirdparty.com.google.common.collect.Iterables;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityNode;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

/**
 * DAO object to display request allocation detailed information.
 */
//AppRequestAllocationInfo 是一个数据传输对象（DAO），用于展示单个资源请求的分配详细信息。
//该类的主要作用是：
//表示 YARN 应用的一个资源请求（如一个容器的请求）
//提供请求的优先级、分配状态、诊断信息
//包含子级 ActivityNodeInfo，展示调度的详细过程
//数据来源于ActivityNode，转换为可读格式，用于Web UI或REST API
//它用于 YARN 资源管理器（ResourceManager）Web 界面和 REST API，帮助用户分析资源请求的调度过程。
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class AppRequestAllocationInfo {
  private Integer requestPriority;  //该资源请求的优先级，数值越高，优先级越高
  private Long allocationRequestId; //该资源请求的唯一ID，用于区分不同的请求
  private String allocationState;   //该请求的分配状态，取自ActivityState（如ALLOCATED, REJECTED）
  private String diagnostic;        //如果 请求失败或跳过，这里会存储 失败原因或调度诊断信息
  private List<ActivityNodeInfo> children;  //该资源请求的 调度过程详情，即该请求经历的所有调度步骤

  AppRequestAllocationInfo() {
  }

  AppRequestAllocationInfo(List<ActivityNode> activityNodes,
      RMWSConsts.ActivitiesGroupBy groupBy) {
    ActivityNode lastActivityNode = Iterables.getLast(activityNodes);
    this.requestPriority = lastActivityNode.getRequestPriority();
    this.allocationRequestId = lastActivityNode.getAllocationRequestId();
    this.allocationState = lastActivityNode.getState().name();
    if (lastActivityNode.isRequestType()
        && lastActivityNode.getDiagnostic() != null) {
      this.diagnostic = lastActivityNode.getDiagnostic();
    }
    this.children = ActivitiesUtils
        .getRequestActivityNodeInfos(activityNodes, groupBy);
  }

  public Integer getRequestPriority() {
    return requestPriority;
  }

  public Long getAllocationRequestId() {
    return allocationRequestId;
  }

  public String getAllocationState() {
    return allocationState;
  }

  public List<ActivityNodeInfo> getChildren() {
    return children;
  }

  public String getDiagnostic() {
    return diagnostic;
  }
}
