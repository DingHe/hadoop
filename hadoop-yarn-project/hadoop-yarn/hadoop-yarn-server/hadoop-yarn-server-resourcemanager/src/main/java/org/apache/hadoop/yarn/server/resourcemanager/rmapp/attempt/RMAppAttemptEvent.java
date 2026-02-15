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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.event.AbstractEvent;
//表示 应用尝试（Application Attempt） 相关的事件
public class RMAppAttemptEvent extends AbstractEvent<RMAppAttemptEventType> {

  private final ApplicationAttemptId appAttemptId;//应用尝试 ID
  private final String diagnosticMsg;//诊断信息，用于存储与该事件相关的诊断信息，例如错误原因或调试信息

  public RMAppAttemptEvent(ApplicationAttemptId appAttemptId,
      RMAppAttemptEventType type) {
    this(appAttemptId, type, "");
  }

  public RMAppAttemptEvent(ApplicationAttemptId appAttemptId,
      RMAppAttemptEventType type, String diagnostics) {
    super(type);
    this.appAttemptId = appAttemptId;
    this.diagnosticMsg = diagnostics;
  }

  public RMAppAttemptEvent(ApplicationAttemptId appAttemptId,
                           RMAppAttemptEventType type, long timeStamp) {
    super(type, timeStamp);
    this.appAttemptId = appAttemptId;
    this.diagnosticMsg = "";
  }

  public ApplicationAttemptId getApplicationAttemptId() {
    return this.appAttemptId;
  }

  public String getDiagnosticMsg() {
    return diagnosticMsg;
  }
}
