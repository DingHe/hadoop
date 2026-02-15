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

/**表示 文件系统权限 的枚举类，遵循 POSIX 风格的权限模型（r、w、x）。
 * 每个枚举值表示一种文件访问权限，并提供了一些基本操作（如与、或、非等）
 * File system actions, e.g. read, write, etc.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public enum FsAction {
  // POSIX style
  NONE("---"), //000	无权限
  EXECUTE("--x"),//001	仅执行权限
  WRITE("-w-"),//010	仅写权限
  WRITE_EXECUTE("-wx"),//011	写和执行权限
  READ("r--"),//100	仅读权限
  READ_EXECUTE("r-x"),//101	读和执行权限
  READ_WRITE("rw-"),//110	读和写权限
  ALL("rwx");//111	读、写、执行三种权限全部具备

  /** Retain reference to value array. */
  //缓存所有枚举值，避免多次调用 values()，提高性能
  private final static FsAction[] vals = values();

  /** Symbolic representation */
  //每个枚举项对应的 POSIX 字符串形式，方便输出和比较
  public final String SYMBOL;

  private FsAction(String s) {
    SYMBOL = s;
  }

  /**
   * Return true if this action implies that action.
   * @param that FsAction that. that 是要检查是否被当前权限包含的目标 FsAction 对象
   * @return if implies true,not false.
   */
  public boolean implies(FsAction that) {
    if (that != null) {
      return (ordinal() & that.ordinal()) == that.ordinal();
    }
    return false;
  }

  /**
   * AND operation.
   * @param that FsAction that.
   * @return FsAction.
   */
  public FsAction and(FsAction that) {
    return vals[ordinal() & that.ordinal()];
  }
  /**
   * OR operation.
   * @param that FsAction that.
   * @return FsAction.
   */
  public FsAction or(FsAction that) {
    return vals[ordinal() | that.ordinal()];
  }
  /**
   * NOT operation.
   * @return FsAction.
   */
  public FsAction not() {
    return vals[7 - ordinal()];
  }

  /**
   * Get the FsAction enum for String representation of permissions
   * 
   * @param permission
   *          3-character string representation of permission. ex: rwx
   * @return Returns FsAction enum if the corresponding FsAction exists for permission.
   *         Otherwise returns null
   */
  public static FsAction getFsAction(String permission) {
    for (FsAction fsAction : vals) {
      if (fsAction.SYMBOL.equals(permission)) {
        return fsAction;
      }
    }
    return null;
  }
}
