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

package org.apache.hadoop.hdfs.server.namenode;

import java.util.List;

import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.util.LongBitFormat;

/**
 * Class to pack an AclEntry into an integer. <br>
 * An ACL entry is represented by a 32-bit integer in Big Endian format. <br>
 *
 * Note:  this format is used both in-memory and on-disk.  Changes will be
 * incompatible.
 *
 */
//主要功能是将 ACL（Access Control List，访问控制列表） 中的每个 AclEntry 对象编码为一个 32 位整数，
// 并提供了将其编码、解码的方法
//ACL（Access Control List） 是一种文件系统级别的访问控制机制，
// 它比传统的文件权限（如 POSIX 权限）更加细粒度，允许为文件或目录指定多个用户或用户组的权限。
// 通过 ACL，系统管理员可以为特定的用户、用户组和其他用户设置不同的读取、写入和执行权限
//传统文件权限（如 Linux 的 rwx 权限）只允许对文件或目录设置三类权限：所有者、所属组和其他用户
//ACL 提供了更加细粒度的控制，可以为不同的用户和组设置不同的权限。它不仅支持为单个用户和组设置权限，还支持为“其他用户”和“掩码”设置权限
public enum AclEntryStatusFormat implements LongBitFormat.Enum {

  PERMISSION(null, 3),//文件操作权限
  TYPE(PERMISSION.BITS, 2),//ACL 条目类型
  SCOPE(TYPE.BITS, 1),//ACL 条目作用范围
  NAME(SCOPE.BITS, 24);//用户或用户组的标识符

  //静态数组缓存了对应的枚举值，方便通过索引快速获取枚举对象，避免多次调用 values() 方法
  private static final FsAction[] FSACTION_VALUES = FsAction.values();
  private static final AclEntryScope[] ACL_ENTRY_SCOPE_VALUES =
      AclEntryScope.values();
  private static final AclEntryType[] ACL_ENTRY_TYPE_VALUES =
      AclEntryType.values();
  //使用 LongBitFormat 类对每个字段进行编码和解码，处理位操作的细节
  private final LongBitFormat BITS;

  private AclEntryStatusFormat(LongBitFormat previous, int length) {
    BITS = new LongBitFormat(name(), previous, length, 0);
  }
  //aclEntry：32 位编码的 ACL 条目

  static AclEntryScope getScope(int aclEntry) {
    int ordinal = (int) SCOPE.BITS.retrieve(aclEntry);
    return ACL_ENTRY_SCOPE_VALUES[ordinal];
  }

  static AclEntryType getType(int aclEntry) {
    int ordinal = (int) TYPE.BITS.retrieve(aclEntry);
    return ACL_ENTRY_TYPE_VALUES[ordinal];
  }

  static FsAction getPermission(int aclEntry) {
    int ordinal = (int) PERMISSION.BITS.retrieve(aclEntry);
    return FSACTION_VALUES[ordinal];
  }

  static String getName(int aclEntry) {
    return getName(aclEntry, null);
  }
   //使用序列号从 stringTable 中获取实际的用户名或用户组名
  static String getName(int aclEntry,
                        SerialNumberManager.StringTable stringTable) {
    SerialNumberManager snm = getSerialNumberManager(getType(aclEntry));
    if (snm != null) {
      int nid = (int)NAME.BITS.retrieve(aclEntry);
      return snm.getString(nid, stringTable);
    }
    return null;
  }
  //参数：aclEntry：要编码的 ACL 条目
  //返回编码后的整数
  static int toInt(AclEntry aclEntry) {
    long aclEntryInt = 0;
    aclEntryInt = SCOPE.BITS
        .combine(aclEntry.getScope().ordinal(), aclEntryInt);
    aclEntryInt = TYPE.BITS.combine(aclEntry.getType().ordinal(), aclEntryInt);
    aclEntryInt = PERMISSION.BITS.combine(aclEntry.getPermission().ordinal(),
        aclEntryInt);
    SerialNumberManager snm = getSerialNumberManager(aclEntry.getType());
    if (snm != null) {
      int nid = snm.getSerialNumber(aclEntry.getName());
      aclEntryInt = NAME.BITS.combine(nid, aclEntryInt);
    }
    return (int) aclEntryInt;
  }

  static AclEntry toAclEntry(int aclEntry) {
    return toAclEntry(aclEntry, null);
  }

  static AclEntry toAclEntry(int aclEntry,
                             SerialNumberManager.StringTable stringTable) {
    return new AclEntry.Builder()
        .setScope(getScope(aclEntry))
        .setType(getType(aclEntry))
        .setPermission(getPermission(aclEntry))
        .setName(getName(aclEntry, stringTable))
        .build();
  }

  public static int[] toInt(List<AclEntry> aclEntries) {
    int[] entries = new int[aclEntries.size()];
    for (int i = 0; i < entries.length; i++) {
      entries[i] = toInt(aclEntries.get(i));
    }
    return entries;
  }

  private static SerialNumberManager getSerialNumberManager(AclEntryType type) {
    switch (type) {
      case USER:
        return SerialNumberManager.USER;
      case GROUP:
        return SerialNumberManager.GROUP;
      default:
        return null;
    }
  }

  @Override
  public int getLength() {
    return BITS.getLength();
  }
}