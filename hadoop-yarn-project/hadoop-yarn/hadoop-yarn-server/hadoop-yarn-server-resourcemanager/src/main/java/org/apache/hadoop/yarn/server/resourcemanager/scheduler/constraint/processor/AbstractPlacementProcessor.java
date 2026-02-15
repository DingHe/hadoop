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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.processor;

import org.apache.hadoop.yarn.ams.ApplicationMasterServiceContext;
import org.apache.hadoop.yarn.ams.ApplicationMasterServiceProcessor;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.PlacementConstraintManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Base class for all PlacementProcessors.
 */
//专门用于处理 应用程序调度约束（PlacementConstraint）。它是 应用程序主服务处理器（ApplicationMasterServiceProcessor） 的一个基类，
// 负责管理应用程序的 调度约束，
// 并确保这些约束在应用程序注册 (registerApplicationMaster) 和完成 (finishApplicationMaster) 时被正确处理。
public abstract class AbstractPlacementProcessor implements
    ApplicationMasterServiceProcessor{
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractPlacementProcessor.class);
  //指向下一个 ApplicationMasterServiceProcessor 实例
  protected ApplicationMasterServiceProcessor nextAMSProcessor;
  //YARN 资源调度器的引用
  protected AbstractYarnScheduler scheduler;
  //用于管理应用的 调度约束（PlacementConstraint）
  private PlacementConstraintManager constraintManager;

  @Override
  public void init(ApplicationMasterServiceContext amsContext,
      ApplicationMasterServiceProcessor nextProcessor) {
    this.nextAMSProcessor = nextProcessor;
    this.scheduler =
        (AbstractYarnScheduler) ((RMContextImpl) amsContext).getScheduler();
    this.constraintManager =
        ((RMContextImpl)amsContext).getPlacementConstraintManager();
  }
  //request（RegisterApplicationMasterRequest）：应用注册请求，可能包含 调度约束（PlacementConstraint）
  @Override
  public void registerApplicationMaster(
      ApplicationAttemptId applicationAttemptId,
      RegisterApplicationMasterRequest request,
      RegisterApplicationMasterResponse response)
      throws IOException, YarnException {
    //获取应用的 调度约束映射
    Map<Set<String>, PlacementConstraint> appPlacementConstraints =
        request.getPlacementConstraints();
    //处理约束
    processPlacementConstraints(applicationAttemptId.getApplicationId(),
        appPlacementConstraints);
    //调用下一个处理器
    nextAMSProcessor.registerApplicationMaster(applicationAttemptId, request,
        response);
  }
  //存储调度约束
  private void processPlacementConstraints(ApplicationId applicationId,
      Map<Set<String>, PlacementConstraint> appPlacementConstraints) {
    if (appPlacementConstraints != null && !appPlacementConstraints.isEmpty()) {
      LOG.info("Constraints added for application [{}] against tags [{}]",
          applicationId, appPlacementConstraints);
      //将约束信息存入 PlacementConstraintManager，供调度器后续使用
      constraintManager.registerApplication(
          applicationId, appPlacementConstraints);
    }
  }
  //处理应用完成
  @Override
  public void finishApplicationMaster(ApplicationAttemptId applicationAttemptId,
      FinishApplicationMasterRequest request,
      FinishApplicationMasterResponse response) {
    //清理调度约束
    constraintManager.unregisterApplication(
        applicationAttemptId.getApplicationId());
    this.nextAMSProcessor.finishApplicationMaster(applicationAttemptId, request,
        response);
  }
}
