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

package org.apache.hadoop.yarn.ams;

import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords
    .FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords
    .RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;

/**
 * Interface to abstract out the the actual processing logic of the
 * Application Master Service.
 */
// 用于抽象 ApplicationMasterService 的实际处理逻辑。
// ApplicationMasterService 是 YARN 资源管理器（ResourceManager）的一部分，负责处理来自 ApplicationMaster（AM）的请求，如注册 AM、资源分配以及 AM 任务的完成通知
public interface ApplicationMasterServiceProcessor {

  /**
   * Initialize with and ApplicationMasterService Context as well as the
   * next processor in the chain.
   * @param amsContext AMSContext.
   * @param nextProcessor next ApplicationMasterServiceProcessor
   */
  //通常用于设置 ApplicationMasterService 的运行环境以及责任链的下一个处理器
  //nextProcessor 表示处理链中的下一个 ApplicationMasterServiceProcessor，用于支持责任链模式，可以层层处理请求
  void init(ApplicationMasterServiceContext amsContext,
      ApplicationMasterServiceProcessor nextProcessor);

  /**
   * Register AM attempt.
   * @param applicationAttemptId applicationAttemptId.
   * @param request Register Request.  表示 ApplicationMaster 的注册请求，包含 AM 需要的资源等信息
   * @param response Register Response.  表示 ResourceManager 对 ApplicationMaster 注册请求的响应，返回如集群资源信息、调度策略等
   * @throws IOException IOException.
   * @throws YarnException in critical situation where invalid
   *         profiles/resources are added.
   */
  //ApplicationMaster 在启动后，首先调用该方法向 ResourceManager 进行注册
  void registerApplicationMaster(ApplicationAttemptId applicationAttemptId,
      RegisterApplicationMasterRequest request,
      RegisterApplicationMasterResponse response)
      throws IOException, YarnException;

  /**
   * Allocate call.
   * @param appAttemptId appAttemptId.
   * @param request Allocate Request. 表示 ApplicationMaster 申请资源或释放资源的请求，通常包括：需要申请的资源（CPU、内存等）、需要释放的容器 ID、任务完成后的更新信息，如 CompletedContainersStatuses
   * @param response Allocate Response.
   * @throws YarnException YarnException.
   */
  //ApplicationMaster 在运行过程中，可以调用 allocate(...) 申请新的资源，或者释放不再使用的资源
  void allocate(ApplicationAttemptId appAttemptId,
      AllocateRequest request, AllocateResponse response) throws YarnException;

  /**
   * Finish AM.
   * @param applicationAttemptId applicationAttemptId.
   * @param request Finish AM Request.
   * @param response Finish AM Response.
   */
  void finishApplicationMaster(
      ApplicationAttemptId applicationAttemptId,
      FinishApplicationMasterRequest request,
      FinishApplicationMasterResponse response);
}
