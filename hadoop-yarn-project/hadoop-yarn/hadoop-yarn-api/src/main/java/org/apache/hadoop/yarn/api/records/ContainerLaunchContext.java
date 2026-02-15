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

package org.apache.hadoop.yarn.api.records;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.server.api.ApplicationInitializationContext;
import org.apache.hadoop.yarn.server.api.AuxiliaryService;
import org.apache.hadoop.yarn.util.Records;

/**
 * {@code ContainerLaunchContext} represents all of the information
 * needed by the {@code NodeManager} to launch a container.
 * <p>
 * It includes details such as:
 * <ul>
 *   <li>{@link ContainerId} of the container.</li>
 *   <li>{@link Resource} allocated to the container.</li>
 *   <li>User to whom the container is allocated.</li>
 *   <li>Security tokens (if security is enabled).</li>
 *   <li>
 *     {@link LocalResource} necessary for running the container such
 *     as binaries, jar, shared-objects, side-files etc.
 *   </li>
 *   <li>Optional, application-specific binary service data.</li>
 *   <li>Environment variables for the launched process.</li>
 *   <li>Command to launch the container.</li>
 *   <li>Retry strategy when container exits with failure.</li>
 * </ul>
 * 
 * @see ContainerManagementProtocol#startContainers(org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest)
 */
// 表示启动容器所需的所有信息。这些信息包括容器的标识符、资源、用户、必需的本地资源、环境变量、启动命令、容器的安全令牌以及容器的重试策略等。
// NodeManager 使用这些信息来正确地启动和管理容器
@Public
@Stable
public abstract class ContainerLaunchContext {

  @Public
  @Stable
  public static ContainerLaunchContext newInstance(
      Map<String, LocalResource> localResources,
      Map<String, String> environment, List<String> commands,
      Map<String, ByteBuffer> serviceData,  ByteBuffer tokens,
      Map<ApplicationAccessType, String> acls) {
    return newInstance(localResources, environment, commands, serviceData,
        tokens, acls, null);
  }

  @Public
  @Unstable
  public static ContainerLaunchContext newInstance(
      Map<String, LocalResource> localResources,
      Map<String, String> environment, List<String> commands,
      Map<String, ByteBuffer> serviceData, ByteBuffer tokens,
      Map<ApplicationAccessType, String> acls,
      ContainerRetryContext containerRetryContext) {
    ContainerLaunchContext container =
        Records.newRecord(ContainerLaunchContext.class);
    container.setLocalResources(localResources);//表示容器启动所需的本地资源。这些资源可能包括二进制文件、JAR包、共享对象文件、辅助文件等
    container.setEnvironment(environment);//表示容器启动时所需的环境变量。这些环境变量会传递给容器中运行的进程，用于配置容器运行时的环境
    container.setCommands(commands);//表示启动容器时需要执行的命令。容器启动时会运行这些命令，以启动容器中的应用程序或服务
    container.setServiceData(serviceData);//表示容器启动时传递的应用程序特定的二进制服务数据。这些数据可能包含与应用程序相关的配置信息或状态
    container.setTokens(tokens);//表示用于容器安全认证的令牌。在启用安全性的环境中，容器可能需要使用这些令牌来进行身份验证
    container.setApplicationACLs(acls);//表示与应用程序相关的访问控制列表（ACL）。它控制哪些用户可以查看或修改该容器
    container.setContainerRetryContext(containerRetryContext);//表示容器启动失败时的重试策略。如果容器启动失败，可以根据此策略决定是否进行重试以及重试的次数和间隔
    return container;
  }

  /**
   * Get all the tokens needed by this container. It may include file-system
   * tokens, ApplicationMaster related tokens if this container is an
   * ApplicationMaster or framework level tokens needed by this container to
   * communicate to various services in a secure manner.
   * 
   * @return tokens needed by this container.
   */
  @Public
  @Stable
  public abstract ByteBuffer getTokens();

  /**
   * Set security tokens needed by this container.
   * @param tokens security tokens 
   */
  @Public
  @Stable
  public abstract void setTokens(ByteBuffer tokens);

  /**
   * Get the configuration used by RM to renew tokens.
   * @return The configuration used by RM to renew the tokens.
   */
  @Public
  @Unstable
  public abstract ByteBuffer getTokensConf();

  /**
   * Set the configuration used by RM to renew the tokens.
   * @param tokensConf The configuration used by RM to renew the tokens
   */
  @Public
  @Unstable
  public abstract void setTokensConf(ByteBuffer tokensConf);

  /**
   * Get <code>LocalResource</code> required by the container.
   * @return all <code>LocalResource</code> required by the container
   */
  @Public
  @Stable
  public abstract Map<String, LocalResource> getLocalResources();
  
  /**
   * Set <code>LocalResource</code> required by the container. All pre-existing
   * Map entries are cleared before adding the new Map
   * @param localResources <code>LocalResource</code> required by the container
   */
  @Public
  @Stable
  public abstract void setLocalResources(Map<String, LocalResource> localResources);

  /**
   * <p>
   * Get application-specific binary <em>service data</em>. This is a map keyed
   * by the name of each {@link AuxiliaryService} that is configured on a
   * NodeManager and value correspond to the application specific data targeted
   * for the keyed {@link AuxiliaryService}.
   * </p>
   * 
   * <p>
   * This will be used to initialize this application on the specific
   * {@link AuxiliaryService} running on the NodeManager by calling
   * {@link AuxiliaryService#initializeApplication(ApplicationInitializationContext)}
   * </p>
   * 
   * @return application-specific binary <em>service data</em>
   */
  @Public
  @Stable
  public abstract Map<String, ByteBuffer> getServiceData();
  
  /**
   * <p>
   * Set application-specific binary <em>service data</em>. This is a map keyed
   * by the name of each {@link AuxiliaryService} that is configured on a
   * NodeManager and value correspond to the application specific data targeted
   * for the keyed {@link AuxiliaryService}. All pre-existing Map entries are
   * preserved.
   * </p>
   * 
   * @param serviceData
   *          application-specific binary <em>service data</em>
   */
  @Public
  @Stable
  public abstract void setServiceData(Map<String, ByteBuffer> serviceData);

  /**
   * Get <em>environment variables</em> for the container.
   * @return <em>environment variables</em> for the container
   */
  @Public
  @Stable
  public abstract Map<String, String> getEnvironment();
    
  /**
   * Add <em>environment variables</em> for the container. All pre-existing Map
   * entries are cleared before adding the new Map
   * @param environment <em>environment variables</em> for the container
   */
  @Public
  @Stable
  public abstract void setEnvironment(Map<String, String> environment);

  /**
   * Get the list of <em>commands</em> for launching the container.
   * @return the list of <em>commands</em> for launching the container
   */
  @Public
  @Stable
  public abstract List<String> getCommands();
  
  /**
   * Add the list of <em>commands</em> for launching the container. All
   * pre-existing List entries are cleared before adding the new List
   * @param commands the list of <em>commands</em> for launching the container
   */
  @Public
  @Stable
  public abstract void setCommands(List<String> commands);

  /**
   * Get the <code>ApplicationACL</code>s for the application. 
   * @return all the <code>ApplicationACL</code>s
   */
  @Public
  @Stable
  public abstract  Map<ApplicationAccessType, String> getApplicationACLs();

  /**
   * Set the <code>ApplicationACL</code>s for the application. All pre-existing
   * Map entries are cleared before adding the new Map
   * @param acls <code>ApplicationACL</code>s for the application
   */
  @Public
  @Stable
  public abstract  void setApplicationACLs(Map<ApplicationAccessType, String> acls);

  /**
   * Get the <code>ContainerRetryContext</code> to relaunch container.
   * @return <code>ContainerRetryContext</code> to relaunch container.
   */
  @Public
  @Unstable
  public abstract ContainerRetryContext getContainerRetryContext();

  /**
   * Set the <code>ContainerRetryContext</code> to relaunch container.
   * @param containerRetryContext <code>ContainerRetryContext</code> to
   *                              relaunch container.
   */
  @Public
  @Unstable
  public abstract void setContainerRetryContext(
      ContainerRetryContext containerRetryContext);
}
