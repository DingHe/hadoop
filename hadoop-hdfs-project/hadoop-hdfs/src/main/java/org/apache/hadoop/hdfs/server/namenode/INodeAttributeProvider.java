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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;

import java.util.Arrays;
//主要用于自定义 INode（HDFS 文件或目录的元数据节点）的属性获取逻辑，
// 允许外部实现类覆盖默认的属性检查、访问控制逻辑。此类通过扩展，能够增强 NameNode 对文件系统的权限和属性管理
@InterfaceAudience.Public
@InterfaceStability.Unstable
public abstract class INodeAttributeProvider {

  public static class AuthorizationContext {
    private String fsOwner;
    private String supergroup;
    private UserGroupInformation callerUgi;
    private INodeAttributes[] inodeAttrs;
    private INode[] inodes;
    private byte[][] pathByNameArr;
    private int snapshotId;
    private String path;
    private int ancestorIndex;
    private boolean doCheckOwner;
    private FsAction ancestorAccess;
    private FsAction parentAccess;
    private FsAction access;
    private FsAction subAccess;
    private boolean ignoreEmptyDir;
    private String operationName;
    private CallerContext callerContext;

    public String getFsOwner() {
      return fsOwner;
    }

    public void setFsOwner(String fsOwner) {
      this.fsOwner = fsOwner;
    }

    public String getSupergroup() {
      return supergroup;
    }

    public void setSupergroup(String supergroup) {
      this.supergroup = supergroup;
    }

    public UserGroupInformation getCallerUgi() {
      return callerUgi;
    }

    public void setCallerUgi(UserGroupInformation callerUgi) {
      this.callerUgi = callerUgi;
    }

    public INodeAttributes[] getInodeAttrs() {
      return inodeAttrs;
    }

    public void setInodeAttrs(INodeAttributes[] inodeAttrs) {
      this.inodeAttrs = inodeAttrs;
    }

    public INode[] getInodes() {
      return inodes;
    }

    public void setInodes(INode[] inodes) {
      this.inodes = inodes;
    }

    public byte[][] getPathByNameArr() {
      return pathByNameArr;
    }

    public void setPathByNameArr(byte[][] pathByNameArr) {
      this.pathByNameArr = pathByNameArr;
    }

    public int getSnapshotId() {
      return snapshotId;
    }

    public void setSnapshotId(int snapshotId) {
      this.snapshotId = snapshotId;
    }

    public String getPath() {
      return path;
    }

    public void setPath(String path) {
      this.path = path;
    }

    public int getAncestorIndex() {
      return ancestorIndex;
    }

    public void setAncestorIndex(int ancestorIndex) {
      this.ancestorIndex = ancestorIndex;
    }

    public boolean isDoCheckOwner() {
      return doCheckOwner;
    }

    public void setDoCheckOwner(boolean doCheckOwner) {
      this.doCheckOwner = doCheckOwner;
    }

    public FsAction getAncestorAccess() {
      return ancestorAccess;
    }

    public void setAncestorAccess(FsAction ancestorAccess) {
      this.ancestorAccess = ancestorAccess;
    }

    public FsAction getParentAccess() {
      return parentAccess;
    }

    public void setParentAccess(FsAction parentAccess) {
      this.parentAccess = parentAccess;
    }

    public FsAction getAccess() {
      return access;
    }

    public void setAccess(FsAction access) {
      this.access = access;
    }

    public FsAction getSubAccess() {
      return subAccess;
    }

    public void setSubAccess(FsAction subAccess) {
      this.subAccess = subAccess;
    }

    public boolean isIgnoreEmptyDir() {
      return ignoreEmptyDir;
    }

    public void setIgnoreEmptyDir(boolean ignoreEmptyDir) {
      this.ignoreEmptyDir = ignoreEmptyDir;
    }

    public String getOperationName() {
      return operationName;
    }

    public void setOperationName(String operationName) {
      this.operationName = operationName;
    }

    public CallerContext getCallerContext() {
      return callerContext;
    }

    public void setCallerContext(CallerContext callerContext) {
      this.callerContext = callerContext;
    }

    public static class Builder {
      private String fsOwner;
      private String supergroup;
      private UserGroupInformation callerUgi;
      private INodeAttributes[] inodeAttrs;
      private INode[] inodes;
      private byte[][] pathByNameArr;
      private int snapshotId;
      private String path;
      private int ancestorIndex;
      private boolean doCheckOwner;
      private FsAction ancestorAccess;
      private FsAction parentAccess;
      private FsAction access;
      private FsAction subAccess;
      private boolean ignoreEmptyDir;
      private String operationName;
      private CallerContext callerContext;

      public AuthorizationContext build() {
        return new AuthorizationContext(this);
      }

      public Builder fsOwner(String val) {
        this.fsOwner = val;
        return this;
      }

      public Builder supergroup(String val) {
        this.supergroup = val;
        return this;
      }

      public Builder callerUgi(UserGroupInformation val) {
        this.callerUgi = val;
        return this;
      }

      public Builder inodeAttrs(INodeAttributes[] val) {
        this.inodeAttrs = val;
        return this;
      }

      public Builder inodes(INode[] val) {
        this.inodes = val;
        return this;
      }

      public Builder pathByNameArr(byte[][] val) {
        this.pathByNameArr = val;
        return this;
      }

      public Builder snapshotId(int val) {
        this.snapshotId = val;
        return this;
      }

      public Builder path(String val) {
        this.path = val;
        return this;
      }

      public Builder ancestorIndex(int val) {
        this.ancestorIndex = val;
        return this;
      }

      public Builder doCheckOwner(boolean val) {
        this.doCheckOwner = val;
        return this;
      }

      public Builder ancestorAccess(FsAction val) {
        this.ancestorAccess = val;
        return this;
      }

      public Builder parentAccess(FsAction val) {
        this.parentAccess = val;
        return this;
      }

      public Builder access(FsAction val) {
        this.access = val;
        return this;
      }

      public Builder subAccess(FsAction val) {
        this.subAccess = val;
        return this;
      }

      public Builder ignoreEmptyDir(boolean val) {
        this.ignoreEmptyDir = val;
        return this;
      }

      public Builder operationName(String val) {
        this.operationName = val;
        return this;
      }

      public Builder callerContext(CallerContext val) {
        this.callerContext = val;
        return this;
      }
    }

    public AuthorizationContext(Builder builder) {
      this.setFsOwner(builder.fsOwner);
      this.setSupergroup(builder.supergroup);
      this.setCallerUgi(builder.callerUgi);
      this.setInodeAttrs(builder.inodeAttrs);
      this.setInodes(builder.inodes);
      this.setPathByNameArr(builder.pathByNameArr);
      this.setSnapshotId(builder.snapshotId);
      this.setPath(builder.path);
      this.setAncestorIndex(builder.ancestorIndex);
      this.setDoCheckOwner(builder.doCheckOwner);
      this.setAncestorAccess(builder.ancestorAccess);
      this.setParentAccess(builder.parentAccess);
      this.setAccess(builder.access);
      this.setSubAccess(builder.subAccess);
      this.setIgnoreEmptyDir(builder.ignoreEmptyDir);
      this.setOperationName(builder.operationName);
      this.setCallerContext(builder.callerContext);
    }

    @VisibleForTesting
    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      } else if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      AuthorizationContext other = (AuthorizationContext)obj;
      return getFsOwner().equals(other.getFsOwner()) &&
          getSupergroup().equals(other.getSupergroup()) &&
          getCallerUgi().equals(other.getCallerUgi()) &&
          Arrays.deepEquals(getInodeAttrs(), other.getInodeAttrs()) &&
          Arrays.deepEquals(getInodes(), other.getInodes()) &&
          Arrays.deepEquals(getPathByNameArr(), other.getPathByNameArr()) &&
          getSnapshotId() == other.getSnapshotId() &&
          getPath().equals(other.getPath()) &&
          getAncestorIndex() == other.getAncestorIndex() &&
          isDoCheckOwner() == other.isDoCheckOwner() &&
          getAncestorAccess() == other.getAncestorAccess() &&
          getParentAccess() == other.getParentAccess() &&
          getAccess() == other.getAccess() &&
          getSubAccess() == other.getSubAccess() &&
          isIgnoreEmptyDir() == other.isIgnoreEmptyDir();
    }

    @Override
    public int hashCode() {
      assert false : "hashCode not designed";
      return 42; // any arbitrary constant will do
    }
  }

  /** 自定义权限校验逻辑的接口，允许开发者根据需求覆盖 HDFS 默认的文件系统权限检查
   * 适用场景：
   * 扩展权限控制：适配企业级需求，集成外部认证系统（如 LDAP、Kerberos）
   * 操作审计：记录被拒绝的访问请求，支持安全合规性要求
   * 复杂权限模型：实现比 HDFS 默认更精细的访问控制策略
   * The AccessControlEnforcer allows implementations to override the
   * default File System permission checking logic enforced on a file system
   * object
   */
  public interface AccessControlEnforcer {

    /**
     * Checks permission on a file system object. Has to throw an Exception
     * if the filesystem object is not accessible by the calling Ugi.
     * @param fsOwner Filesystem owner (The Namenode user)
     * @param supergroup super user group
     * @param callerUgi UserGroupInformation of the caller
     * @param inodeAttrs Array of INode attributes for each path element in the
     *                   the path
     * @param inodes Array of INodes for each path element in the path
     * @param pathByNameArr Array of byte arrays of the LocalName
     * @param snapshotId the snapshotId of the requested path
     * @param path Path String
     * @param ancestorIndex Index of ancestor
     * @param doCheckOwner perform ownership check
     * @param ancestorAccess The access required by the ancestor of the path.
     * @param parentAccess The access required by the parent of the path.
     * @param access The access required by the path.
     * @param subAccess If path is a directory, It is the access required of
     *                  the path and all the sub-directories. If path is not a
     *                  directory, there should ideally be no effect.
     * @param ignoreEmptyDir Ignore permission checking for empty directory?
     * @deprecated use{@link #checkPermissionWithContext(AuthorizationContext)}}
     * instead
     * @throws AccessControlException
     */
    //对文件系统对象执行权限检查，如果调用用户无权访问该对象，则抛出 AccessControlException 异常
    public abstract void checkPermission(String fsOwner, String supergroup,
        UserGroupInformation callerUgi, INodeAttributes[] inodeAttrs,
        INode[] inodes, byte[][] pathByNameArr, int snapshotId, String path,
        int ancestorIndex, boolean doCheckOwner, FsAction ancestorAccess,
        FsAction parentAccess, FsAction access, FsAction subAccess,
        boolean ignoreEmptyDir)
            throws AccessControlException;

    /**
     * Checks permission on a file system object. Has to throw an Exception
     * if the filesystem object is not accessible by the calling Ugi.
     * @param authzContext an {@link AuthorizationContext} object encapsulating
     *                     the various parameters required to authorize an
     *                     operation.
     * @throws AccessControlException
     */
    //使用 AuthorizationContext 对象执行权限检查
    //如果未实现此方法，默认抛出异常，提示未覆盖该方法
    default void checkPermissionWithContext(AuthorizationContext authzContext)
        throws AccessControlException {
      throw new AccessControlException("The authorization provider does not "
          + "implement the checkPermissionWithContext(AuthorizationContext) "
          + "API.");
    }

    /**
     * Checks if the user is a superuser or belongs to superuser group.
     * It throws an AccessControlException if user is not a superuser.
     *
     * @param authzContext an {@link AuthorizationContext} object encapsulating
     *                     the various parameters required to authorize an
     *                     operation.
     * @throws AccessControlException - if user is not a super user or part
     * of the super user group.
     */
    //检查调用者是否为超级用户或属于超级用户组，如果不满足条件则抛出异常
    default void checkSuperUserPermissionWithContext(
        AuthorizationContext authzContext)
        throws AccessControlException {
      UserGroupInformation callerUgi = authzContext.getCallerUgi();
      boolean isSuperUser =
          callerUgi.getShortUserName().equals(authzContext.getFsOwner()) ||
          callerUgi.getGroupsSet().contains(authzContext.getSupergroup());
      if (!isSuperUser) {
        throw new AccessControlException("Access denied for user " +
            callerUgi.getShortUserName() + ". Superuser privilege is " +
            "required for operation " + authzContext.getOperationName());
      }
    }

    /**
     * This method must be called when denying access to users to
     * notify the external enforcers.
     * This will help the external enforcers to audit the requests
     * by users that were denied access.
     * @param authzContext an {@link AuthorizationContext} object encapsulating
     *                     the various parameters required to authorize an
     *                     operation.
     * @throws AccessControlException
     */
    //用于在拒绝用户访问时通知外部权限系统（如审计系统），并抛出异常
    default void denyUserAccess(AuthorizationContext authzContext,
                                String errorMessage)
        throws AccessControlException {
      throw new AccessControlException(errorMessage);
    }
  }

  /**在 NameNode 启动时调用，初始化 INodeAttributeProvider，用于设置自定义的 INode 属性处理逻辑
   * Initialize the provider. This method is called at NameNode startup
   * time.
   */
  public abstract void start();

  /**在 NameNode 关闭时调用，进行资源清理，确保自定义属性提供器的安全退出
   * Shutdown the provider. This method is called at NameNode shutdown time.
   */
  public abstract void stop();
  //将传入的 HDFS 路径字符串解析为路径元素数组
  //返回值 String[]：路径元素的字符串数组，如 /a/b/c → ["a", "b", "c"]
  @Deprecated
  String[] getPathElements(String path) {
    path = path.trim();
    //如果路径不是以 / 开头，抛出 IllegalArgumentException 异常，路径必须是绝对路径
    if (path.charAt(0) != Path.SEPARATOR_CHAR) {
      throw new IllegalArgumentException("It must be an absolute path: " +
          path);
    }
    //统计路径中 / 出现的次数，确定路径元素数量
    int numOfElements = StringUtils.countMatches(path, Path.SEPARATOR);
    //如果路径以 / 结尾，减少一个元素数量（避免空元素）
    if (path.length() > 1 && path.endsWith(Path.SEPARATOR)) {
      numOfElements--;
    }
    String[] pathElements = new String[numOfElements];
    int elementIdx = 0;
    int idx = 0;
    int found = path.indexOf(Path.SEPARATOR_CHAR, idx);
    while (found > -1) {
      if (found > idx) {
        pathElements[elementIdx++] = path.substring(idx, found);
      }
      idx = found + 1;
      found = path.indexOf(Path.SEPARATOR_CHAR, idx);
    }
    if (idx < path.length()) {
      pathElements[elementIdx] = path.substring(idx);
    }
    return pathElements;
  }

  @Deprecated
  public INodeAttributes getAttributes(String fullPath, INodeAttributes inode) {
    return getAttributes(getPathElements(fullPath), inode);
  }
  //通过路径字符串获取指定 INode 的属性
  public abstract INodeAttributes getAttributes(String[] pathElements,
      INodeAttributes inode);
  //通过路径元素数组解析 INode 属性，提供自定义的属性获取逻辑
  public INodeAttributes getAttributes(byte[][] components,
      INodeAttributes inode) {
    String[] elements = new String[components.length];
    for (int i = 0; i < elements.length; i++) {
      elements[i] = DFSUtil.bytes2String(components[i]);
    }
    return getAttributes(elements, inode);
  }

  /**
   * Can be over-ridden by implementations to provide a custom Access Control
   * Enforcer that can provide an alternate implementation of the
   * default permission checking logic.
   * @param defaultEnforcer The Default AccessControlEnforcer
   * @return The AccessControlEnforcer to use
   */
  //允许提供自定义的权限检查器，替代 HDFS 默认的权限检查逻辑
  public AccessControlEnforcer getExternalAccessControlEnforcer(
      AccessControlEnforcer defaultEnforcer) {
    return defaultEnforcer;
  }
}
