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
//表示应用程序尝试启动事件的类
public class RMAppStartAttemptEvent extends RMAppAttemptEvent {

  private final boolean transferStateFromPreviousAttempt;

  public RMAppStartAttemptEvent(ApplicationAttemptId appAttemptId,//应用程序尝试的唯一标识符
      boolean transferStateFromPreviousAttempt) { //指示是否将前一个应用程序尝试的状态转移到当前尝试
    super(appAttemptId, RMAppAttemptEventType.START);
    this.transferStateFromPreviousAttempt = transferStateFromPreviousAttempt;
  }

  public boolean getTransferStateFromPreviousAttempt() {
    return transferStateFromPreviousAttempt;
  }
}
