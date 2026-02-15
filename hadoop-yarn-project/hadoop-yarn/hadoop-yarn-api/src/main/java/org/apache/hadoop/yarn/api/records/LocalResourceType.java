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
 * {@code LocalResourceType} specifies the <em>type</em>
 * of a resource localized by the {@code NodeManager}.
 * <p>
 * The <em>type</em> can be one of:
 * <ul>
 *   <li>
 *     {@link #FILE} - Regular file i.e. uninterpreted bytes.
 *   </li>
 *   <li>
 *     {@link #ARCHIVE} - Archive, which is automatically unarchived by the
 *     <code>NodeManager</code>.
 *   </li>
 *   <li>
 *     {@link #PATTERN} - A hybrid between {@link #ARCHIVE} and {@link #FILE}.
 *   </li>
 * </ul>
 *
 * @see LocalResource
 * @see ContainerLaunchContext
 * @see ApplicationSubmissionContext
 * @see ContainerManagementProtocol#startContainers(org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest)
 */
//资源类型可以是普通文件、归档文件或混合类型（部分文件解压）
@Public
@Stable
public enum LocalResourceType {
  
  /**
   * Archive, which is automatically unarchived by the <code>NodeManager</code>.
   */
  ARCHIVE, //表示资源是一个 归档文件（例如 .tar、.zip、.jar）。当 NodeManager 本地化该资源时，它会自动解压归档文件
  
  /**
   * Regular file i.e. uninterpreted bytes.
   */
  FILE,  //普通文件，即没有任何解压的操作，资源以原始的二进制字节流存在
  
  /**
   * A hybrid between archive and file.  Only part of the file is unarchived,
   * and the original file is left in place, but in the same directory as the
   * unarchived part.  The part that is unarchived is determined by pattern
   * in #{@link LocalResource}.  Currently only jars support pattern, all
   * others will be treated like a #{@link LocalResourceType#ARCHIVE}.
   */
  PATTERN // 混合类型，即既像归档文件一样包含多个文件，也有像普通文件一样的特点。这个类型用于支持部分解压的资源（目前只支持 JAR 文件）
}
