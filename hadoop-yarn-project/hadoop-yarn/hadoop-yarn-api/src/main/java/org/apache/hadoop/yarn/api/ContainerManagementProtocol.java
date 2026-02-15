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

package org.apache.hadoop.yarn.api;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.protocolrecords.CommitResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ContainerUpdateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ContainerUpdateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetLocalizationStatusesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetLocalizationStatusesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.IncreaseContainersResourceRequest;
import org.apache.hadoop.yarn.api.protocolrecords.IncreaseContainersResourceResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReInitializeContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReInitializeContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceLocalizationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceLocalizationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RestartContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RollbackResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * <p>The protocol between an <code>ApplicationMaster</code> and a 
 * <code>NodeManager</code> to start/stop and increase resource of containers
 * and to get status of running containers.</p>
 *
 * <p>If security is enabled the <code>NodeManager</code> verifies that the
 * <code>ApplicationMaster</code> has truly been allocated the container
 * by the <code>ResourceManager</code> and also verifies all interactions such 
 * as stopping the container or obtaining status information for the container.
 * </p>
 */
//用程序主控（ApplicationMaster）与节点管理器（NodeManager）之间的通信协议，用于启动、停止、增加容器资源，并获取正在运行的容器的状态
@Public
@Stable
public interface ContainerManagementProtocol {

  /**
   * <p>
   * The <code>ApplicationMaster</code> provides a list of
   * {@link StartContainerRequest}s to a <code>NodeManager</code> to
   * <em>start</em> {@link Container}s allocated to it using this interface.
   * </p>
   * 
   * <p>
   * The <code>ApplicationMaster</code> has to provide details such as allocated
   * resource capability, security tokens (if enabled), command to be executed
   * to start the container, environment for the process, necessary
   * binaries/jar/shared-objects etc. via the {@link ContainerLaunchContext} in
   * the {@link StartContainerRequest}.
   * </p>
   * 
   * <p>
   * The <code>NodeManager</code> sends a response via
   * {@link StartContainersResponse} which includes a list of
   * {@link Container}s of successfully launched {@link Container}s, a
   * containerId-to-exception map for each failed {@link StartContainerRequest} in
   * which the exception indicates errors from per container and a
   * allServicesMetaData map between the names of auxiliary services and their
   * corresponding meta-data. Note: None-container-specific exceptions will
   * still be thrown by the API method itself.
   * </p>
   * <p>
   * The <code>ApplicationMaster</code> can use
   * {@link #getContainerStatuses(GetContainerStatusesRequest)} to get updated
   * statuses of the to-be-launched or launched containers.
   * </p>
   * 
   * @param request
   *          request to start a list of containers
   * @return response including conatinerIds of all successfully launched
   *         containers, a containerId-to-exception map for failed requests and
   *         a allServicesMetaData map.
   * @throws YarnException exceptions from yarn servers.
   * @throws IOException io error occur.
   */
  @Public
  @Stable
  //此方法会向 NodeManager 发送一个启动请求。
  // NodeManager 根据容器的详细信息启动容器，并返回一个 StartContainersResponse，其中包含成功启动的容器 ID 列表和每个启动失败的容器的异常信息
  StartContainersResponse startContainers(StartContainersRequest request)
      throws YarnException, IOException;

  /**
   * <p>
   * The <code>ApplicationMaster</code> requests a <code>NodeManager</code> to
   * <em>stop</em> a list of {@link Container}s allocated to it using this
   * interface.
   * </p>
   * 
   * <p>
   * The <code>ApplicationMaster</code> sends a {@link StopContainersRequest}
   * which includes the {@link ContainerId}s of the containers to be stopped.
   * </p>
   * 
   * <p>
   * The <code>NodeManager</code> sends a response via
   * {@link StopContainersResponse} which includes a list of {@link ContainerId}
   * s of successfully stopped containers, a containerId-to-exception map for
   * each failed request in which the exception indicates errors from per
   * container. Note: None-container-specific exceptions will still be thrown by
   * the API method itself. <code>ApplicationMaster</code> can use
   * {@link #getContainerStatuses(GetContainerStatusesRequest)} to get updated
   * statuses of the containers.
   * </p>
   * 
   * @param request
   *          request to stop a list of containers
   * @return response which includes a list of containerIds of successfully
   *         stopped containers, a containerId-to-exception map for failed
   *         requests.
   * @throws YarnException exceptions from yarn servers.
   * @throws IOException io error occur.
   */
  @Public
  @Stable
  //此方法向 NodeManager 发送请求，要求停止指定容器。
  // NodeManager 会返回一个 StopContainersResponse，其中包含成功停止的容器 ID 和停止失败的容器的异常信息
  StopContainersResponse stopContainers(StopContainersRequest request)
      throws YarnException, IOException;

  /**
   * <p>
   * The API used by the <code>ApplicationMaster</code> to request for current
   * statuses of <code>Container</code>s from the <code>NodeManager</code>.
   * </p>
   * 
   * <p>
   * The <code>ApplicationMaster</code> sends a
   * {@link GetContainerStatusesRequest} which includes the {@link ContainerId}s
   * of all containers whose statuses are needed.
   * </p>
   * 
   * <p>
   * The <code>NodeManager</code> responds with
   * {@link GetContainerStatusesResponse} which includes a list of
   * {@link ContainerStatus} of the successfully queried containers and a
   * containerId-to-exception map for each failed request in which the exception
   * indicates errors from per container. Note: None-container-specific
   * exceptions will still be thrown by the API method itself.
   * </p>
   * 
   * @param request
   *          request to get <code>ContainerStatus</code>es of containers with
   *          the specified <code>ContainerId</code>s
   * @return response containing the list of <code>ContainerStatus</code> of the
   *         successfully queried containers and a containerId-to-exception map
   *         for failed requests.
   * 
   * @throws YarnException exceptions from yarn servers.
   * @throws IOException io error occur.
   */
  @Public
  @Stable
  //此方法向 NodeManager 发送请求，获取指定容器的状态信息。
  // NodeManager 会返回 GetContainerStatusesResponse，其中包含容器的状态和相应的异常信息
  GetContainerStatusesResponse getContainerStatuses(
      GetContainerStatusesRequest request) throws YarnException,
      IOException;

  /**
   * <p>
   * The API used by the <code>ApplicationMaster</code> to request for
   * resource increase of running containers on the <code>NodeManager</code>.
   * </p>
   *
   * @param request
   *         request to increase resource of a list of containers
   * @return response which includes a list of containerIds of containers
   *         whose resource has been successfully increased and a
   *         containerId-to-exception map for failed requests.
   *
   * @throws YarnException exceptions from yarn servers.
   * @throws IOException io error occur.
   */
  @Public
  @Unstable
  @Deprecated
  //此方法请求 NodeManager 增加指定容器的资源（如内存、CPU）。
  // NodeManager 会返回一个 IncreaseContainersResourceResponse，包括成功增加资源的容器 ID 和失败的异常信息
  IncreaseContainersResourceResponse increaseContainersResource(
      IncreaseContainersResourceRequest request) throws YarnException,
      IOException;

  /**
   * <p>
   * The API used by the <code>ApplicationMaster</code> to request for
   * resource update of running containers on the <code>NodeManager</code>.
   * </p>
   *
   * @param request
   *         request to update resource of a list of containers
   * @return response which includes a list of containerIds of containers
   *         whose resource has been successfully updated and a
   *         containerId-to-exception map for failed requests.
   *
   * @throws YarnException Exception specific to YARN
   * @throws IOException IOException thrown from NodeManager
   */
  @Public
  @Unstable
  //此方法允许 ApplicationMaster 请求更新容器的资源。
  // NodeManager 会根据请求更新容器的资源，并返回 ContainerUpdateResponse，包括成功更新的容器 ID 和失败的异常信息
  ContainerUpdateResponse updateContainer(ContainerUpdateRequest request)
      throws YarnException, IOException;
  //此方法向 NodeManager 发送请求，发送一个信号到指定容器，通常用于请求容器执行特定操作
  SignalContainerResponse signalToContainer(SignalContainerRequest request)
      throws YarnException, IOException;

  /**
   * Localize resources required by the container.
   * Currently, this API only works for running containers.
   *
   * @param request Specify the resources to be localized.
   * @return Response that the localize request is accepted.
   * @throws YarnException Exception specific to YARN
   * @throws IOException IOException thrown from the RPC layer.
   */
  @Public
  @Unstable
  //此方法用于本地化资源，确保容器能够访问到需要的资源文件。NodeManager 会返回一个 ResourceLocalizationResponse，表示请求已被接受
  ResourceLocalizationResponse localize(ResourceLocalizationRequest request)
    throws YarnException, IOException;

  /**
   * ReInitialize the Container with a new Launch Context.
   * @param request Specify the new ContainerLaunchContext.
   * @return Response that the ReInitialize request is accepted.
   * @throws YarnException Exception specific to YARN.
   * @throws IOException IOException thrown from the RPC layer.
   */
  @Public
  @Unstable
  //此方法重新初始化容器并提供新的启动上下文。NodeManager 会返回一个 ReInitializeContainerResponse，表示请求已被接受
  ReInitializeContainerResponse reInitializeContainer(
      ReInitializeContainerRequest request) throws YarnException, IOException;

  /**
   * Restart the container.
   * @param containerId Container Id.
   * @return Response that the restart request is accepted.
   * @throws YarnException Exception specific to YARN.
   * @throws IOException IOException thrown from the RPC layer.
   */
  @Public
  @Unstable
  //此方法请求 NodeManager 重启指定容器。NodeManager 会返回一个 RestartContainerResponse，表示请求已被接受
  RestartContainerResponse restartContainer(ContainerId containerId)
      throws YarnException, IOException;

  /**
   * Rollback the Last ReInitialization if possible.
   * @param containerId Container Id.
   * @return Response that the rollback request is accepted.
   * @throws YarnException Exception specific to YARN.
   * @throws IOException IOException thrown from the RPC layer.
   */
  @Public
  @Unstable
  //此方法请求回滚上一次的容器初始化操作。如果容器支持回滚，NodeManager 会返回一个 RollbackResponse，表示回滚已被接受
  RollbackResponse rollbackLastReInitialization(ContainerId containerId)
      throws YarnException, IOException;

  /**
   * Commit the Last ReInitialization if possible. Once the reinitialization
   * has been committed, It cannot be rolled back.
   * @param containerId Container Id.
   * @return Response that the commit request is accepted.
   * @throws YarnException Exception specific to YARN.
   * @throws IOException IOException thrown from the RPC layer.
   */
  @Public
  @Unstable
  CommitResponse commitLastReInitialization(ContainerId containerId)
      throws YarnException, IOException;

  /**
   * API to request for the localization statuses of requested containers from
   * the Node Manager.
   * @param request {@link GetLocalizationStatusesRequest} which includes the
   *                container ids of all the containers whose localization
   *                statuses are needed.
   * @return {@link GetLocalizationStatusesResponse} which contains the
   *         localization statuses of all the requested containers.
   * @throws YarnException Exception specific to YARN.
   * @throws IOException IOException thrown from the RPC layer.
   */
  @Public
  @Unstable
  GetLocalizationStatusesResponse getLocalizationStatuses(
      GetLocalizationStatusesRequest request) throws YarnException,
      IOException;
}
