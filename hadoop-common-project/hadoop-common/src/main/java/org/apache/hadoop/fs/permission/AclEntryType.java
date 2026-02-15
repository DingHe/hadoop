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

/** 表示 Hadoop 文件系统中 ACL（Access Control List，访问控制列表）条目的类型。ACL 是一种细粒度的权限控制机制，允许对文件和目录的访问进行更精确的控制
 * Specifies the type of an ACL entry.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public enum AclEntryType {
  /**
   * An ACL entry applied to a specific user.  These ACL entries can be unnamed,
   * which applies to the file owner, or named, which applies to the specific
   * named user.
   */
  USER,//针对特定用户的 ACL 条目。可以是未命名（适用于文件的所有者）或命名（适用于指定用户

  /**
   * An ACL entry applied to a specific group.  These ACL entries can be
   * unnamed, which applies to the file's group, or named, which applies to the
   * specific named group.
   */
  GROUP,//针对特定用户组的 ACL 条目。可以是未命名（适用于文件所属组）或命名（适用于特定的用户组）

  /**
   * An ACL mask entry.  Mask entries are unnamed.  During permission checks,
   * the mask entry interacts with all ACL entries that are members of the group
   * class.  This consists of all named user entries, the unnamed group entry,
   * and all named group entries.  For each such entry, any permissions that are
   * absent from the mask entry are removed from the effective permissions used
   * during the permission check.
   */
  MASK,//掩码条目，用于限制特定用户和组的有效权限。它与组类成员的权限交互，决定最终的有效权限

  /**
   * An ACL entry that applies to all other users that were not covered by one
   * of the more specific ACL entry types.
   */
  OTHER;//适用于所有其他未被USER或GROUP明确覆盖的用户的 ACL 条目

  @Override
  @InterfaceStability.Unstable
  public String toString() {
    // This currently just delegates to the stable string representation, but it
    // is permissible for the output of this method to change across versions.
    return toStringStable();
  }

  /**
   * Returns a string representation guaranteed to be stable across versions to
   * satisfy backward compatibility requirements, such as for shell command
   * output or serialization.
   *
   * @return stable, backward compatible string representation
   */
  public String toStringStable() {
    // The base implementation uses the enum value names, which are public API
    // and therefore stable.
    return super.toString();
  }
}
