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

package org.apache.hadoop.yarn.server.resourcemanager.recovery.records;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.ApplicationStateDataProto;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.util.Records;

/**
 * Contains all the state data that needs to be stored persistently 
 * for an Application
 */
//用于持久化存储应用程序状态的关键类。它记录了一个应用程序（Application）在 RM 中的所有必要状态信息，
// 如提交时间、启动时间、用户信息、提交上下文、应用状态、诊断信息、启动时间、完成时间等。该类是一个抽象类，具体的存储实现可能由子类完成
@Public
@Unstable
public abstract class ApplicationStateData {
  public Map<ApplicationAttemptId, ApplicationAttemptStateData> attempts =
      new HashMap<ApplicationAttemptId, ApplicationAttemptStateData>();
  
  public static ApplicationStateData newInstance(long submitTime,
      long startTime, String user,
      ApplicationSubmissionContext submissionContext, RMAppState state,
      String diagnostics, long launchTime, long finishTime,
      CallerContext callerContext) {
    ApplicationStateData appState = Records.newRecord(ApplicationStateData.class);
    appState.setSubmitTime(submitTime); //应用程序提交时间
    appState.setStartTime(startTime); //应用程序开始执行的时间
    appState.setUser(user); //提交该应用程序的用户
    appState.setApplicationSubmissionContext(submissionContext); //应用程序提交上下文（ApplicationSubmissionContext），包含了应用程序的基本信息
    appState.setState(state); //应用程序当前的状态（RMAppState）
    appState.setDiagnostics(diagnostics); //应用程序的诊断信息，通常用于记录错误或警告
    appState.setLaunchTime(launchTime); //应用程序启动的时间
    appState.setFinishTime(finishTime); //应用程序结束的时间
    appState.setCallerContext(callerContext); //调用上下文（CallerContext），用于追踪应用程序的调用来源
    return appState;
  }

  public static ApplicationStateData newInstance(long submitTime,
      long startTime, String user,
      ApplicationSubmissionContext submissionContext, RMAppState state,
      String diagnostics, long launchTime, long finishTime,
      CallerContext callerContext,
      Map<ApplicationTimeoutType, Long> applicationTimeouts) {
    ApplicationStateData appState =
        Records.newRecord(ApplicationStateData.class);
    appState.setSubmitTime(submitTime);
    appState.setStartTime(startTime);
    appState.setUser(user);
    appState.setApplicationSubmissionContext(submissionContext);
    appState.setState(state);
    appState.setDiagnostics(diagnostics);
    appState.setLaunchTime(launchTime);
    appState.setFinishTime(finishTime);
    appState.setCallerContext(callerContext);
    appState.setApplicationTimeouts(applicationTimeouts);
    return appState;
  }

  public static ApplicationStateData newInstance(long submitTime,
      long startTime, ApplicationSubmissionContext context, String user,
      CallerContext callerContext) {
    return newInstance(submitTime, startTime, user, context, null, "", 0, 0,
        callerContext);
  }
  
  public static ApplicationStateData newInstance(long submitTime,
      long startTime, ApplicationSubmissionContext context, String user) {
    return newInstance(submitTime, startTime, context, user,
        (CallerContext) null);
  }

  public static ApplicationStateData newInstance(long submitTime,
      long startTime, String user, String realUser,
      ApplicationSubmissionContext submissionContext, RMAppState state,
      String diagnostics, long launchTime, long finishTime,
      CallerContext callerContext) {
    ApplicationStateData appState =
        newInstance(submitTime, startTime, user, submissionContext, state,
            diagnostics, launchTime, finishTime, callerContext);
    if (realUser != null) {
      appState.setRealUser(realUser);
    }
    return appState;
  }

  public static ApplicationStateData newInstance(long submitTime,
      long startTime, String user, String realUser,
      ApplicationSubmissionContext submissionContext, RMAppState state,
      String diagnostics, long launchTime, long finishTime,
      CallerContext callerContext,
      Map<ApplicationTimeoutType, Long> applicationTimeouts) {
    ApplicationStateData appState =
        newInstance(submitTime, startTime, user, submissionContext, state,
            diagnostics, launchTime, finishTime, callerContext, applicationTimeouts);
    if (realUser != null) {
      appState.setRealUser(realUser);
    }
    return appState;
  }

  public static ApplicationStateData newInstance(long submitTime,
      long startTime, ApplicationSubmissionContext context, String user,
      String realUser, CallerContext callerContext) {
    return newInstance(submitTime, startTime, user, realUser, context, null, "",
        0, 0, callerContext);
  }

  public int getAttemptCount() {
    return attempts.size();
  }

  public ApplicationAttemptStateData getAttempt(
      ApplicationAttemptId  attemptId) {
    return attempts.get(attemptId);
  }

  public int getFirstAttemptId() {
    int min = Integer.MAX_VALUE;
    for(ApplicationAttemptId attemptId : attempts.keySet()) {
      if (attemptId.getAttemptId() < min) {
        min = attemptId.getAttemptId();
      }
    }
    return min == Integer.MAX_VALUE ? 1 : min;
  }

  public abstract ApplicationStateDataProto getProto();

  /**
   * The time at which the application was received by the Resource Manager
   * @return submitTime
   */
  @Public
  @Unstable
  public abstract long getSubmitTime();
  
  @Public
  @Unstable
  public abstract void setSubmitTime(long submitTime);

  /**
   * Get the <em>start time</em> of the application.
   * @return <em>start time</em> of the application
   */
  @Public
  @Stable
  public abstract long getStartTime();

  @Private
  @Unstable
  public abstract void setStartTime(long startTime);



  /**
   * Get the <em>launch time</em> of the application.
   * @return <em>launch time</em> of the application
   */
  @Public
  @Stable
  public abstract long getLaunchTime();

  @Private
  @Unstable
  public abstract void setLaunchTime(long launchTime);

  /**
   * The application submitter.
   * @param user submitter user name.
   */
  @Public
  @Unstable
  public abstract void setUser(String user);
  
  @Public
  @Unstable
  public abstract String getUser();
  
  /**
   * The {@link ApplicationSubmissionContext} for the application
   * {@link ApplicationId} can be obtained from the this
   * @return ApplicationSubmissionContext
   */
  @Public
  @Unstable
  public abstract ApplicationSubmissionContext getApplicationSubmissionContext();
  
  @Public
  @Unstable
  public abstract void setApplicationSubmissionContext(
      ApplicationSubmissionContext context);

  /**
   * Get the final state of the application.
   * @return the final state of the application.
   */
  public abstract RMAppState getState();

  public abstract void setState(RMAppState state);

  /**
   * Get the diagnostics information for the application master.
   * @return the diagnostics information for the application master.
   */
  public abstract String getDiagnostics();

  public abstract void setDiagnostics(String diagnostics);

  /**
   * The finish time of the application.
   * @return the finish time of the application.,
   */
  public abstract long getFinishTime();

  public abstract void setFinishTime(long finishTime);
  
  public abstract CallerContext getCallerContext();
  
  public abstract void setCallerContext(CallerContext callerContext);

  @Public
  public abstract Map<ApplicationTimeoutType, Long> getApplicationTimeouts();

  @Public
  public abstract void setApplicationTimeouts(
      Map<ApplicationTimeoutType, Long> applicationTimeouts);

  public abstract String getRealUser();

  public abstract void setRealUser(String realUser);
}
