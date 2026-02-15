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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.api.records.CollectorInfo;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeUpdateType;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.api.records.AppCollectorData;
import org.apache.hadoop.yarn.server.resourcemanager.placement
    .ApplicationPlacementContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;

/**
 * The interface to an Application in the ResourceManager. Take a
 * look at {@link RMAppImpl} for its implementation. This interface
 * exposes methods to access various updates in application status/report.
 */
//管理和跟踪应用程序的接口。它代表一个正在执行的应用程序，并提供各种方法来访问应用程序的状态、信息和配置。
// 此类允许访问应用程序的多个状态和报告，处理与应用程序相关的事件，并提供有关应用程序的详细信息，尤其是应用程序的启动、运行、完成等过程中的各种信息
public interface RMApp extends EventHandler<RMAppEvent> {

  /**
   * The application id for this {@link RMApp}.
   * @return the {@link ApplicationId} for this {@link RMApp}.
   */
  //返回应用程序的唯一标识符 ApplicationId，用于标识当前应用
  ApplicationId getApplicationId();
  
  /**
   * The application submission context for this {@link RMApp}
   * @return the {@link ApplicationSubmissionContext} for this {@link RMApp}
   */
  //返回应用程序提交时的上下文
  ApplicationSubmissionContext getApplicationSubmissionContext();

  /**
   * The current state of the {@link RMApp}.
   * @return the current state {@link RMAppState} for this application.
   */
  //返回应用程序的当前状态
  RMAppState getState();

  /**
   * The user who submitted this application.
   * @return the user who submitted the application.
   */
  //返回提交该应用程序的用户名称
  String getUser();

  /**
   * Progress of application.
   * @return the progress of the {@link RMApp}.
   */
  //返回应用程序的执行进度，值在 0 到 1 之间
  float getProgress();

  /**
   * {@link RMApp} can have multiple application attempts {@link RMAppAttempt}.
   * This method returns the {@link RMAppAttempt} corresponding to
   *  {@link ApplicationAttemptId}.
   * @param appAttemptId the application attempt id
   * @return  the {@link RMAppAttempt} corresponding to the {@link ApplicationAttemptId}.
   */
  //根据应用程序尝试 ID 返回对应的应用程序尝试对象 RMAppAttempt
  RMAppAttempt getRMAppAttempt(ApplicationAttemptId appAttemptId);

  /**
   * Each Application is submitted to a queue decided by {@link
   * ApplicationSubmissionContext#setQueue(String)}.
   * This method returns the queue to which an application was submitted.
   * @return the queue to which the application was submitted to.
   */
  //返回应用程序提交到的队列名称
  String getQueue();
  
  /**
   * Reflects a change in the application's queue from the one specified in the
   * {@link ApplicationSubmissionContext}.
   * @param name the new queue name
   */
  //设置应用程序所属的队列名称
  void setQueue(String name);

  /**
   * The name of the application as set in {@link
   * ApplicationSubmissionContext#setApplicationName(String)}.
   * @return the name of the application.
   */
  //返回应用程序的名称
  String getName();

  /**
   * {@link RMApp} can have multiple application attempts {@link RMAppAttempt}.
   * This method returns the current {@link RMAppAttempt}.
   * @return the current {@link RMAppAttempt}
   */
  //返回当前正在执行的应用程序尝试
  RMAppAttempt getCurrentAppAttempt();

  /**
   * {@link RMApp} can have multiple application attempts {@link RMAppAttempt}.
   * This method returns the all {@link RMAppAttempt}s for the RMApp.
   * @return all {@link RMAppAttempt}s for the RMApp.
   */
  //返回所有的应用程序尝试
  Map<ApplicationAttemptId, RMAppAttempt> getAppAttempts();

  /**
   * To get the status of an application in the RM, this method can be used.
   * If full access is not allowed then the following fields in the report
   * will be stubbed:
   * <ul>
   *   <li>host - set to "N/A"</li>
   *   <li>RPC port - set to -1</li>
   *   <li>client token - set to "N/A"</li>
   *   <li>diagnostics - set to "N/A"</li>
   *   <li>tracking URL - set to "N/A"</li>
   *   <li>original tracking URL - set to "N/A"</li>
   *   <li>resource usage report - all values are -1</li>
   * </ul>
   *
   * @param clientUserName the user name of the client requesting the report
   * @param allowAccess whether to allow full access to the report
   * @return the {@link ApplicationReport} detailing the status of the application.
   */
  //创建并返回应用程序的详细报告 ApplicationReport，包括各种应用程序状态、诊断信息等
  ApplicationReport createAndGetApplicationReport(String clientUserName,
      boolean allowAccess);
  
  /**
   * To receive the collection of all {@link RMNode}s whose updates have been
   * received by the RMApp. Updates can be node becoming lost or becoming
   * healthy etc. The method clears the information from the {@link RMApp}. So
   * each call to this method gives the delta from the previous call.
   * @param updatedNodes Map into which the updates are transferred, with each
   * node updates as the key, and the {@link NodeUpdateType} for that update
   * as the corresponding value.
   * @return the number of nodes added to the {@link Map}
   */
  //获取已更新的节点信息，并返回被更新的节点数
  int pullRMNodeUpdates(Map<RMNode, NodeUpdateType> updatedNodes);

  /**
   * The finish time of the {@link RMApp}
   * @return the finish time of the application.,
   */
  //返回应用程序的完成时间
  long getFinishTime();

  /**
   * the start time of the application.
   * @return the start time of the application.
   */
  //返回应用程序的启动时间
  long getStartTime();

  /**
   * the submit time of the application.
   * @return the submit time of the application.
   */
  //返回应用程序的提交时间
  long getSubmitTime();

  /**
   * The launch time of the application.
   * Since getStartTime() returns what is essentially submit time,
   * this new field is to prevent potential backwards compatibility issues.
   * @return the launch time of the application.
   */
  //返回应用程序的启动时间（与 getStartTime() 类似）
  long getLaunchTime();

  /**
   * The tracking url for the application master.
   * @return the tracking url for the application master.
   */
  //返回应用程序的追踪 URL，供用户查看应用程序的状态和日志
  String getTrackingUrl();

  /**
   * The timeline collector information for the application. It should be used
   * only if the timeline service v.2 is enabled.
   *
   * @return the data for the application's collector, including collector
   * address, RM ID, version and collector token. Return null if the timeline
   * service v.2 is not enabled.
   */
  //返回应用程序的收集器数据，仅在启用 Timeline Service v2 时可用
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  AppCollectorData getCollectorData();

  /**
   * The timeline collector information to be sent to AM. It should be used
   * only if the timeline service v.2 is enabled.
   *
   * @return collector info, including collector address and collector token.
   * Return null if the timeline service v.2 is not enabled.
   */
  //返回应用程序的收集器信息，仅在启用 Timeline Service v2 时可用
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  CollectorInfo getCollectorInfo();
  /**
   * The original tracking url for the application master.
   * @return the original tracking url for the application master.
   */
  //返回应用程序的原始追踪 URL
  String getOriginalTrackingUrl();

  /**
   * the diagnostics information for the application master.
   * @return the diagnostics information for the application master.
   */
  //返回应用程序的诊断信息
  StringBuilder getDiagnostics();

  /**
   * The final finish state of the AM when unregistering as in
   * {@link FinishApplicationMasterRequest#setFinalApplicationStatus(FinalApplicationStatus)}.
   * @return the final finish state of the AM as set in
   * {@link FinishApplicationMasterRequest#setFinalApplicationStatus(FinalApplicationStatus)}.
   */
  //返回应用程序最终的状态，如成功、失败等
  FinalApplicationStatus getFinalApplicationStatus();

  /**
   * The number of max attempts of the application.
   * @return the number of max attempts of the application.
   */
  //返回应用程序最大尝试次数
  int getMaxAppAttempts();

  /**
   * Returns the application type
   * @return the application type.
   */
  //返回应用程序的类型
  String getApplicationType();

  /**
   * Get tags for the application
   * @return tags corresponding to the application
   */
  //返回与应用程序相关的标签
  Set<String> getApplicationTags();

  /**
   * Check whether this application's state has been saved to the state store.
   * @return the flag indicating whether the applications's state is stored.
   */
  //检查应用程序是否已将最终状态存储
  boolean isAppFinalStateStored();
  
  
  /**
   * Nodes on which the containers for this {@link RMApp} ran.
   * @return the set of nodes that ran any containers from this {@link RMApp}
   * Add more node on which containers for this {@link RMApp} ran
   */
  //返回应用程序运行的节点集
  Set<NodeId> getRanNodes();

  /**
   * Create the external user-facing state of ApplicationMaster from the
   * current state of the {@link RMApp}.
   * @return the external user-facing state of ApplicationMaster.
   */
  //生成用户可见的应用程序状态
  YarnApplicationState createApplicationState();
  
  /**
   * Get RMAppMetrics of the {@link RMApp}.
   * 
   * @return metrics
   */
  //返回应用程序的指标
  RMAppMetrics getRMAppMetrics();
  //返回应用程序的预留 ID
  ReservationId getReservationId();
  //返回应用程序的 AM 资源请求
  List<ResourceRequest> getAMResourceRequests();
  //返回应用程序的日志汇总报告
  Map<NodeId, LogAggregationReport> getLogAggregationReportsForApp();
  //返回应用程序的日志汇总状态
  LogAggregationStatus getLogAggregationStatusForAppReport();

  /**
   * Return the node label expression of the AM container.
   * @return the node label expression.
   */
  //返回 AM 容器的节点标签表达式
  String getAmNodeLabelExpression();
  //返回应用程序的节点标签表达式
  String getAppNodeLabelExpression();
  //返回调用者上下文信息
  CallerContext getCallerContext();
  //返回应用程序的超时信息
  Map<ApplicationTimeoutType, Long> getApplicationTimeouts();

  /**
   * Get priority of the application.
   * @return priority
   */
  //返回应用程序的优先级
  Priority getApplicationPriority();

  /**
   * To verify whether app has reached in its completing/completed states.
   *
   * @return True/False to confirm whether app is in final states
   */
  //检查应用程序是否已进入完成状态
  boolean isAppInCompletedStates();

  /**
   * Get the application -&gt; queue placement context
   * @return ApplicationPlacementContext
   */
  //返回应用程序的队列和调度信息
  ApplicationPlacementContext getApplicationPlacementContext();

  /**
   * Get the application scheduling environment variables.
   * @return Map of envs related to application scheduling preferences.
   */
  //返回应用程序调度环境变量
  Map<String, String> getApplicationSchedulingEnvs();
  //返回应用程序提交者的真实用户名
  String getRealUser();
}
