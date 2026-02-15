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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.namenode.INodeWithAdditionalFields.PermissionStatusFormat;
import org.apache.hadoop.hdfs.server.namenode.XAttrFeature;

/**表示 INode（索引节点） 属性的接口，定义了与文件或目录元数据相关的方法。
 * 这些属性主要包括权限、用户、组、访问时间、修改时间、ACL（访问控制列表）、扩展属性（XAttr）等信息
 * The attributes of an inode.
 */
@InterfaceAudience.Private
public interface INodeAttributes {
  //判断当前节点是目录还是文件
  public boolean isDirectory();

  /**获取文件或目录的名称，返回字节数组以节省空间
   * @return null if the local name is null;
   *         otherwise, return the local name byte array.
   */
  public byte[] getLocalNameBytes();

  /** @return the user name. 获取拥有此 INode 的用户名称*/
  public String getUserName();

  /** @return the group name. 获取文件或目录的用户组名称*/
  public String getGroupName();
  
  /** @return the permission. 获取标准的 HDFS 文件或目录权限（如 rwxr-xr-x）*/
  public FsPermission getFsPermission();

  /** @return the permission as a short. 以紧凑的 short 类型返回 HDFS 文件的权限*/
  public short getFsPermissionShort();
  
  /** @return the permission information as a long.获取完整的权限信息，适用于持久化存储和权限计算 */
  public long getPermissionLong();

  /** @return the ACL feature. 返回该 INode 的 ACL 特性，若没有 ACL 返回 null*/
  public AclFeature getAclFeature();
  
  /** @return the XAttrs feature. 获取 INode 的扩展属性，如自定义标签或额外信息*/
  public XAttrFeature getXAttrFeature();

  /** @return the modification time. 获取文件或目录的最后修改时间*/
  public long getModificationTime();

  /** @return the access time. 获取文件或目录的最后访问时间*/
  public long getAccessTime();

  /** A read-only copy of the inode attributes. */
  public static abstract class SnapshotCopy implements INodeAttributes {
    private final byte[] name;
    private final long permission;
    private final AclFeature aclFeature;
    private final long modificationTime;
    private final long accessTime;
    private XAttrFeature xAttrFeature;

    SnapshotCopy(byte[] name, PermissionStatus permissions,
        AclFeature aclFeature, long modificationTime, long accessTime, 
        XAttrFeature xAttrFeature) {
      this.name = name;
      this.permission = PermissionStatusFormat.toLong(permissions);
      if (aclFeature != null) {
        aclFeature = AclStorage.addAclFeature(aclFeature);
      }
      this.aclFeature = aclFeature;
      this.modificationTime = modificationTime;
      this.accessTime = accessTime;
      this.xAttrFeature = xAttrFeature;
    }

    SnapshotCopy(INode inode) {
      this.name = inode.getLocalNameBytes();
      this.permission = inode.getPermissionLong();
      if (inode.getAclFeature() != null) {
        aclFeature = AclStorage.addAclFeature(inode.getAclFeature());
      } else {
        aclFeature = null;
      }
      this.modificationTime = inode.getModificationTime();
      this.accessTime = inode.getAccessTime();
      this.xAttrFeature = inode.getXAttrFeature();
    }

    @Override
    public final byte[] getLocalNameBytes() {
      return name;
    }

    @Override
    public final String getUserName() {
      return PermissionStatusFormat.getUser(permission);
    }

    @Override
    public final String getGroupName() {
      return PermissionStatusFormat.getGroup(permission);
    }

    @Override
    public final FsPermission getFsPermission() {
      return new FsPermission(getFsPermissionShort());
    }

    @Override
    public final short getFsPermissionShort() {
      return PermissionStatusFormat.getMode(permission);
    }
    
    @Override
    public long getPermissionLong() {
      return permission;
    }

    @Override
    public AclFeature getAclFeature() {
      return aclFeature;
    }

    @Override
    public final long getModificationTime() {
      return modificationTime;
    }

    @Override
    public final long getAccessTime() {
      return accessTime;
    }
    
    @Override
    public final XAttrFeature getXAttrFeature() {
      return xAttrFeature;
    }
  }
}
