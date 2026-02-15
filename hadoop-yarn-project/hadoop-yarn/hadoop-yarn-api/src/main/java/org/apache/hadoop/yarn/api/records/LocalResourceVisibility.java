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

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;

/**
 * {@code LocalResourceVisibility} specifies the <em>visibility</em>
 * of a resource localized by the {@code NodeManager}.
 * <p>
 * The <em>visibility</em> can be one of:
 * <ul>
 *   <li>{@link #PUBLIC} - Shared by all users on the node.</li>
 *   <li>
 *     {@link #PRIVATE} - Shared among all applications of the
 *     <em>same user</em> on the node.
 *   </li>
 *   <li>
 *     {@link #APPLICATION} - Shared only among containers of the
 *     <em>same application</em> on the node.
 *   </li>
 * </ul>
 * 
 * @see LocalResource
 * @see ContainerLaunchContext
 * @see ApplicationSubmissionContext
 * @see ContainerManagementProtocol#startContainers(org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest)
 */
//用于指定 YARN 中 NodeManager 本地化资源的 可见性。资源的可见性决定了该资源在节点上可以被哪些用户或应用访问。
// 通过设置不同的可见性选项，用户可以控制资源的共享范围，从而影响资源的隔离性
@Public
@Stable
public enum LocalResourceVisibility {
  /** 
   * Shared by all users on the node.
   */
  //该资源对节点上的所有用户 可见，即任何用户的容器都可以访问这个资源
  PUBLIC, 
  
  /** 
   * Shared among all applications of the <em>same user</em> on the node.
   */
  //该资源仅对 同一用户的所有应用 可见。换句话说，只有该用户在节点上运行的应用（容器）才能访问这个资源，而其他用户无法访问
  PRIVATE, 
  
  /** 
   * Shared only among containers of the <em>same application</em> on the node.
   */
  //该资源仅对 同一应用的容器 可见。换句话说，只有属于同一个应用的容器可以访问该资源，而同一节点上的其他应用的容器无法访问
  APPLICATION
}
