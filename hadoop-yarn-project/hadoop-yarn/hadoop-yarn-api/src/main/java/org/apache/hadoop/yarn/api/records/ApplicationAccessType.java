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

/**
 * Application access types.
 */
//表示 YARN 中对应用程序的 访问类型。它定义了两种不同的访问权限，分别是 查看 和 修改，用于控制不同角色对应用程序的访问权限
@Public
@Stable
public enum ApplicationAccessType {

  /**
   * Access-type representing 'viewing' application. ACLs against this type
   * dictate who can 'view' some or all of the application related details.
   */
  //使用这种访问权限的用户可以查看与应用程序相关的详细信息（如应用状态、资源使用情况等），但无法对应用进行修改
  VIEW_APP,

  /**
   * Access-type representing 'modifying' application. ACLs against this type
   * dictate who can 'modify' the application for e.g., by killing the
   * application
   */
  //使用这种访问权限的用户可以对应用进行修改，如终止应用程序或其他管理操作
  MODIFY_APP;
}
