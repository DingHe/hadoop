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

package org.apache.hadoop.yarn.server.api;

import java.nio.ByteBuffer;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * A generic service that will be started by the NodeManager. This is a service
 * that administrators have to configure on each node by setting
 * {@link YarnConfiguration#NM_AUX_SERVICES}.
 * 
 */
//用于定义可以随 NodeManager (NM) 一起启动和运行的附加服务
// 扩展 YARN 功能：允许集群管理员通过配置 yarn.nodemanager.aux-services 启用自定义的、与应用紧密协作的服务，从而扩展 YARN 的能力
//生命周期管理：继承自 AbstractService，因此 NodeManager 可以管理它的启动 (start())、停止 (stop()) 和配置
// 提供 Hook：为 YARN 上的应用程序、容器的启动和停止提供回调机制（Hook），允许辅助服务执行初始化或清理操作
@Public
@Evolving
public abstract class AuxiliaryService extends AbstractService {
  //存储该辅助服务用于持久化状态（以便在 NodeManager 重启后恢复）的本地文件系统路径。如果未启用恢复功能，则为 null
  private Path recoveryPath = null;
  //存储一个实现了 AuxiliaryLocalPathHandler 接口的对象。该对象用于帮助辅助服务智能地选择和访问 NodeManager 配置的本地目录 (yarn.nodemanager.local-dirs) 进行读写操作
  private AuxiliaryLocalPathHandler auxiliaryLocalPathHandler;

  protected AuxiliaryService(String name) {
    super(name);
  }

  /**
   * Get the path specific to this auxiliary service to use for recovery.
   *
   * @return state storage path or null if recovery is not enabled
   */
  //返回由 setRecoveryPath 方法设置的、用于存储可恢复状态的本地路径
  protected Path getRecoveryPath() {
    return recoveryPath;
  }

  /**
   * A new application is started on this NodeManager. This is a signal to
   * this {@link AuxiliaryService} about the application initialization.
   * 
   * @param initAppContext context for the application's initialization
   */
  //当 NodeManager 首次知道有一个新的 YARN 应用程序在其节点上运行时被调用。实现类（如 Shuffle Service）可以在此执行应用程序级别的初始化，例如为该应用设置存储目录
  public abstract void initializeApplication(
      ApplicationInitializationContext initAppContext);

  /**
   * An application is finishing on this NodeManager. This is a signal to this
   * {@link AuxiliaryService} about the same.
   * 
   * @param stopAppContext context for the application termination
   */
  //当 NodeManager 收到应用程序在其节点上终止的信号时被调用。实现类应在此进行应用程序级别的清理和资源释放
  public abstract void stopApplication(
      ApplicationTerminationContext stopAppContext);

  /**
   * Retrieve meta-data for this {@link AuxiliaryService}. Applications using
   * this {@link AuxiliaryService} SHOULD know the format of the meta-data -
   * ideally each service should provide a method to parse out the information
   * to the applications. One example of meta-data is contact information so
   * that applications can access the service remotely. This will only be called
   * after the service's {@link #start()} method has finished. the result may be
   * cached.
   * 
   * <p>
   * The information is passed along to applications via
   * {@link StartContainersResponse#getAllServicesMetaData()} that is returned by
   * {@link ContainerManagementProtocol#startContainers(StartContainersRequest)}
   * </p>
   * 
   * @return meta-data for this service that should be made available to
   *         applications.
   */
  // 返回一个 ByteBuffer 格式的元数据，其中包含辅助服务的联系信息或其他必要信息。
  // NodeManager 会将这个元数据通过 StartContainersResponse 传递给 ApplicationMaster (AM)，使 AM 能够知道如何与该辅助服务通信（例如，Shuffle Service 会传递其监听地址和端口）
  public abstract ByteBuffer getMetaData();

  /**
   * A new container is started on this NodeManager. This is a signal to
   * this {@link AuxiliaryService} about the container initialization.
   * This method is called when the NodeManager receives the container launch
   * command from the ApplicationMaster and before the container process is 
   * launched.
   *
   * @param initContainerContext context for the container's initialization
   */
  //当 NodeManager 接收到启动容器的命令（在容器进程实际启动之前）时被调用。默认实现为空，子类可以重写此方法以执行容器级别的初始化
  public void initializeContainer(ContainerInitializationContext
      initContainerContext) {
  }

  /**
   * A container is finishing on this NodeManager. This is a signal to this
   * {@link AuxiliaryService} about the same.
   *
   * @param stopContainerContext context for the container termination
   */
  //当 NodeManager 知道一个容器在其节点上终止时被调用。默认实现为空，子类可以重写此方法以执行容器级别的清理
  public void stopContainer(ContainerTerminationContext stopContainerContext) {
  }

  /**
   * Set the path for this auxiliary service to use for storing state
   * that will be used during recovery.
   *
   * @param recoveryPath where recoverable state should be stored
   */
  //供 NodeManager 在启动时调用，以设置该服务用于存储状态的持久化路径
  public void setRecoveryPath(Path recoveryPath) {
    this.recoveryPath = recoveryPath;
  }

  /**
   * Method that gets the local dirs path handler for this Auxiliary Service.
   *
   * @return auxiliaryPathHandler object that is used to read from and write to
   * valid local Dirs.
   */
  //返回 NodeManager 提供的 AuxiliaryLocalPathHandler 实例。辅助服务使用它来安全地访问本地磁盘路径
  public AuxiliaryLocalPathHandler getAuxiliaryLocalPathHandler() {
    return this.auxiliaryLocalPathHandler;
  }

  /**
   * Method that sets the local dirs path handler for this Auxiliary Service.
   *
   * @param auxiliaryLocalPathHandler the pathHandler for this auxiliary service
   */
  //供 NodeManager 在启动时调用，将负责管理本地目录的 AuxiliaryLocalPathHandler 实例注入给该辅助服务
  public void setAuxiliaryLocalPathHandler(
      AuxiliaryLocalPathHandler auxiliaryLocalPathHandler) {
    this.auxiliaryLocalPathHandler = auxiliaryLocalPathHandler;
  }
}
