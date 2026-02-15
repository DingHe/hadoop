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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.AppAllocation;
import org.apache.hadoop.yarn.util.SystemClock;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * DAO object to display application activity.
 */
//AppActivitiesInfo 是一个 数据传输对象（DAO, Data Access Object），用于展示应用程序的调度活动信息。
// 该类主要用于YARN资源管理器的 Web 界面（RM Web UI） 或 API，向用户提供特定应用的调度记录，包括：
//应用ID
//调度诊断信息（如错误原因）
//调度的时间戳和可读日期
//该应用的资源分配记录（封装在 AppAllocationInfo 对象列表中）
@XmlRootElement(name = "appActivities")
@XmlAccessorType(XmlAccessType.FIELD)
public class AppActivitiesInfo {
  private String applicationId;//该对象关联的 YARN 应用 ID（ApplicationId）
  private String diagnostic;//诊断信息，用于存储应用在调度过程中的错误或状态信息。例如：资源不足、调度失败等
  private Long timestamp; //记录该对象创建时的时间戳（毫秒级）。
  private String dateTime; //记录 timestamp 对应的 可读时间（Date.toString() 形式）。
  private List<AppAllocationInfo> allocations; //该应用的资源分配记录，包含多个 AppAllocationInfo，描述应用的多次资源调度尝试

  private static final Logger LOG =
      LoggerFactory.getLogger(AppActivitiesInfo.class);

  public AppActivitiesInfo() {
  }

  public AppActivitiesInfo(String errorMessage, String applicationId) {
    this.diagnostic = errorMessage;
    this.applicationId = applicationId;
    setTime(SystemClock.getInstance().getTime());
  }

  public AppActivitiesInfo(List<AppAllocation> appAllocations,
      ApplicationId applicationId,
      RMWSConsts.ActivitiesGroupBy groupBy) {
    this.applicationId = applicationId.toString();
    this.allocations = new ArrayList<>();

    if (appAllocations == null) {
      diagnostic = "waiting for display";
      setTime(SystemClock.getInstance().getTime());
    } else {
      for (int i = appAllocations.size() - 1; i > -1; i--) {
        AppAllocation appAllocation = appAllocations.get(i);
        AppAllocationInfo appAllocationInfo = new AppAllocationInfo(
            appAllocation, groupBy);
        this.allocations.add(appAllocationInfo);
      }
    }
  }

  private void setTime(long ts) {
    this.timestamp = ts;
    this.dateTime = new Date(ts).toString();
  }

  @VisibleForTesting
  public List<AppAllocationInfo> getAllocations() {
    return allocations;
  }

  public Long getTimestamp() {
    return timestamp;
  }

  public String getDateTime() {
    return dateTime;
  }

  public String getApplicationId() {
    return applicationId;
  }

  public String getDiagnostic() {
    return diagnostic;
  }
}
