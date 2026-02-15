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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.event;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
//一个新的应用程序尝试（Application Attempt）被添加到调度器中的事件。
// 在 YARN 中，当应用程序的一个新的尝试（即 AM 重新启动或新的容器尝试）启动时，
// 调度器会接收到这个事件，并在调度器中处理该尝试的资源调度、状态管理等操作
public class AppAttemptAddedSchedulerEvent extends SchedulerEvent {
  //表示应用程序尝试的唯一标识符
  private final ApplicationAttemptId applicationAttemptId;
  //表示是否需要从前一个应用程序尝试中转移状态。如果值为 true，表示需要将前一个尝试的状态（如容器、资源等）迁移到当前尝试
  private final boolean transferStateFromPreviousAttempt;
  //表示当前应用程序尝试是否处于恢复状态
  private final boolean isAttemptRecovering;

  public AppAttemptAddedSchedulerEvent(
      ApplicationAttemptId applicationAttemptId,
      boolean transferStateFromPreviousAttempt) {
    this(applicationAttemptId, transferStateFromPreviousAttempt, false);
  }

  public AppAttemptAddedSchedulerEvent(
      ApplicationAttemptId applicationAttemptId,
      boolean transferStateFromPreviousAttempt,
      boolean isAttemptRecovering) {
    super(SchedulerEventType.APP_ATTEMPT_ADDED);
    this.applicationAttemptId = applicationAttemptId;
    this.transferStateFromPreviousAttempt = transferStateFromPreviousAttempt;
    this.isAttemptRecovering = isAttemptRecovering;
  }

  public ApplicationAttemptId getApplicationAttemptId() {
    return applicationAttemptId;
  }

  public boolean getTransferStateFromPreviousAttempt() {
    return transferStateFromPreviousAttempt;
  }

  public boolean getIsAttemptRecovering() {
    return isAttemptRecovering;
  }
}
