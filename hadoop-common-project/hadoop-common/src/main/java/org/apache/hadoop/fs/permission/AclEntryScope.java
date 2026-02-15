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
package org.apache.hadoop.fs.permission;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**指定 ACL（Access Control List，访问控制列表） 条目范围或用途的枚举类
 * Specifies the scope or intended usage of an ACL entry.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public enum AclEntryScope {
  /**
   * An ACL entry that is inspected during permission checks to enforce
   * permissions.
   */
  ACCESS,//访问 ACL 条目，用于检查权限，决定用户或用户组对文件的访问权限。

  /**
   * An ACL entry to be applied to a directory's children that do not otherwise
   * have their own ACL defined.  Unlike an access ACL entry, a default ACL
   * entry is not inspected as part of permission enforcement on the directory
   * that owns it.
   */
  DEFAULT;//默认 ACL 条目，用于继承权限，作用于目录的子项，但不影响自身权限检查。
  //只适用于目录，不适用于文件
  //当新文件或子目录在此目录中创建时，会继承这些默认 ACL
}
