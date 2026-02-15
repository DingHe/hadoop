/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.event;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.server.resourcemanager.placement
    .ApplicationPlacementContext;
//表示一个新的应用程序被添加到调度器中的事件。
// 这个事件会在资源管理器接收到应用程序的提交请求时触发，调度器通过此事件来处理应用程序的相关信息，包括应用程序ID、优先级、队列、用户信息、
// 是否为恢复中的应用程序等
public class AppAddedSchedulerEvent extends SchedulerEvent {
  //表示应用程序的唯一标识符。每个应用程序在 YARN 集群中都有一个唯一的 ID
  private final ApplicationId applicationId;
  //表示该应用程序所提交的队列名称。YARN 会根据队列的资源策略来分配资源
  private final String queue;
  //表示提交应用程序的用户的用户名。这个信息通常用于权限控制和资源分配决策
  private final String user;
  //表示如果应用程序属于某个预留的资源集群，该属性将包含该预留ID。如果应用程序没有预留资源，则该值为 null
  private final ReservationId reservationID;
  //表示应用程序是否处于恢复状态
  private final boolean isAppRecovering;
  //表示应用程序的优先级
  private final Priority appPriority;
  //表示应用程序的放置上下文。它包含关于应用程序资源放置策略的信息，决定如何将资源分配给应用程序
  private final ApplicationPlacementContext placementContext;
  //表示应用程序是否是一个非托管的 ApplicationMaster (AM)。如果为 true，表示该应用程序没有由 YARN 管理的 ApplicationMaster
  private boolean unmanagedAM = false;

  public AppAddedSchedulerEvent(ApplicationId applicationId, String queue,
      String user) {
    this(applicationId, queue, user, false, null, Priority.newInstance(0),
        null);
  }

  public AppAddedSchedulerEvent(ApplicationId applicationId, String queue,
      String user, ApplicationPlacementContext placementContext) {
    this(applicationId, queue, user, false, null, Priority.newInstance(0),
        placementContext);
  }

  public AppAddedSchedulerEvent(ApplicationId applicationId, String queue,
      String user, ReservationId reservationID, Priority appPriority) {
    this(applicationId, queue, user, false, reservationID, appPriority, null);
  }

  public AppAddedSchedulerEvent(String user,
      ApplicationSubmissionContext submissionContext, boolean isAppRecovering,
      Priority appPriority) {
    this(submissionContext.getApplicationId(), submissionContext.getQueue(),
        user, isAppRecovering, submissionContext.getReservationID(),
        appPriority, null);
    this.unmanagedAM = submissionContext.getUnmanagedAM();
  }

  public AppAddedSchedulerEvent(String user,
      ApplicationSubmissionContext submissionContext, boolean isAppRecovering,
      Priority appPriority, ApplicationPlacementContext placementContext) {
    this(submissionContext.getApplicationId(), submissionContext.getQueue(),
        user, isAppRecovering, submissionContext.getReservationID(),
        appPriority, placementContext);
    this.unmanagedAM = submissionContext.getUnmanagedAM();
  }

  public AppAddedSchedulerEvent(ApplicationId applicationId, String queue,
      String user, boolean isAppRecovering, ReservationId reservationID,
      Priority appPriority, ApplicationPlacementContext placementContext) {
    super(SchedulerEventType.APP_ADDED);
    this.applicationId = applicationId;
    this.queue = queue;
    this.user = user;
    this.reservationID = reservationID;
    this.isAppRecovering = isAppRecovering;
    this.appPriority = appPriority;
    this.placementContext = placementContext;
  }

  public ApplicationId getApplicationId() {
    return applicationId;
  }

  public String getQueue() {
    return queue;
  }

  public String getUser() {
    return user;
  }

  public boolean getIsAppRecovering() {
    return isAppRecovering;
  }

  public ReservationId getReservationID() {
    return reservationID;
  }

  public Priority getApplicatonPriority() {
    return appPriority;
  }

  public ApplicationPlacementContext getPlacementContext() {
    return placementContext;
  }

  public boolean isUnmanagedAM() {
    return unmanagedAM;
  }
}
