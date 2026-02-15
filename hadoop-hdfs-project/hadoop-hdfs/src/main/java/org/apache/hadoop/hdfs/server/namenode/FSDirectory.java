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

import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.protobuf.InvalidProtocolBufferException;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.FSLimitException.MaxDirectoryItemsExceededException;
import org.apache.hadoop.hdfs.protocol.FSLimitException.PathComponentTooLongException;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ReencryptionInfoProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoStriped;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo.UpdatedReplicationInfo;
import org.apache.hadoop.hdfs.server.namenode.sps.StoragePolicySatisfyManager;
import org.apache.hadoop.hdfs.util.ByteArray;
import org.apache.hadoop.hdfs.util.EnumCounters;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

import static org.apache.hadoop.fs.CommonConfigurationKeys.FS_PROTECTED_DIRECTORIES;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ACCESS_CONTROL_ENFORCER_REPORTING_THRESHOLD_MS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ACCESS_CONTROL_ENFORCER_REPORTING_THRESHOLD_MS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_QUOTA_BY_STORAGETYPE_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_QUOTA_BY_STORAGETYPE_ENABLED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROTECTED_SUBDIRECTORIES_ENABLE;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROTECTED_SUBDIRECTORIES_ENABLE_DEFAULT;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.CRYPTO_XATTR_ENCRYPTION_ZONE;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.SECURITY_XATTR_UNREADABLE_BY_SUPERUSER;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.XATTR_SATISFY_STORAGE_POLICY;
import static org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot.CURRENT_STATE_ID;

/**
 * Both FSDirectory and FSNamesystem manage the state of the namespace.
 * FSDirectory is a pure in-memory data structure, all of whose operations
 * happen entirely in memory. In contrast, FSNamesystem persists the operations
 * to the disk.
 * @see org.apache.hadoop.hdfs.server.namenode.FSNamesystem
 **/
//主要用于管理HDFS的命名空间
//FSDirectory和FSNamesystem共同管理文件系统的状态，FSDirectory负责纯内存的数据结构操作，而FSNamesystem则负责将操作持久化到磁盘
@InterfaceAudience.Private
public class FSDirectory implements Closeable {
  static final Logger LOG = LoggerFactory.getLogger(FSDirectory.class);
  //创建 HDFS 文件系统中的 根目录 ("/")，并为其设置基本属性和功能
  //FSNamesystem namesystem：传入一个 FSNamesystem 对象，代表整个 HDFS 文件系统的命名空间管理类
  private static INodeDirectory createRoot(FSNamesystem namesystem) {
    final INodeDirectory r = new INodeDirectory(
        INodeId.ROOT_INODE_ID,
        INodeDirectory.ROOT_NAME,
        namesystem.createFsOwnerPermissions(new FsPermission((short) 0755)),
        0L);// 父目录ID，根目录没有父级，默认为0
    r.addDirectoryWithQuotaFeature(//为当前目录添加配额管理功能，限制命名空间和存储空间
        new DirectoryWithQuotaFeature.Builder().
            nameSpaceQuota(DirectoryWithQuotaFeature.DEFAULT_NAMESPACE_QUOTA). //设置目录下允许的最大文件和目录数，使用默认值
            storageSpaceQuota(DirectoryWithQuotaFeature.DEFAULT_STORAGE_SPACE_QUOTA).//设置目录下允许的最大存储空间，使用默认值
            build());
    r.addSnapshottableFeature();//为根目录添加 快照 功能，支持对文件系统进行快照操作，快照 (Snapshot) 是 HDFS 中的一项重要特性，允许用户捕获文件系统在某个时间点的状态，方便进行数据恢复或历史回溯
    r.setSnapshotQuota(0);//设置快照配额为 0，表示当前目录没有快照数量限制
    return r;
  }

  @VisibleForTesting
  static boolean CHECK_RESERVED_FILE_NAMES = true;//当该值为 true 时，HDFS 会对路径中使用的保留文件名（如 .reserved、.inodes）进行检查，防止非法访问
  public final static String DOT_RESERVED_STRING =
      HdfsConstants.DOT_RESERVED_STRING; //引用 HdfsConstants 类中定义的 .reserved 字符串，表示一个保留路径，通常用于系统内部的特殊访问
  public final static String DOT_RESERVED_PATH_PREFIX =
      HdfsConstants.DOT_RESERVED_PATH_PREFIX; //.reserved 路径的前缀，用于标识内部路径，访问这些路径时需要特殊权限
  public final static byte[] DOT_RESERVED = 
      DFSUtil.string2Bytes(DOT_RESERVED_STRING);//通过 DFSUtil.string2Bytes() 方法将 .reserved 字符串转换为 字节数组，用于路径匹配或序列化操作
  private final static String RAW_STRING = "raw";//表示 raw 字符串，用于定义原始路径访问，raw 通常用于直接访问原始数据而不经过透明加密层。例如，HDFS 加密区（Encryption Zone, EZ）允许通过路径 /user/.reserved/raw 直接访问未经解密的数据
  private final static byte[] RAW = DFSUtil.string2Bytes(RAW_STRING);
  public final static String DOT_INODES_STRING =
      HdfsConstants.DOT_INODES_STRING;//.inodes 字符串，HDFS 内部使用的保留路径
  public final static byte[] DOT_INODES = 
      DFSUtil.string2Bytes(DOT_INODES_STRING);
  private final static byte[] DOT_DOT =
      DFSUtil.string2Bytes("..");

  public final static HdfsFileStatus DOT_RESERVED_STATUS =
      new HdfsFileStatus.Builder()
        .isdir(true)
        .perm(new FsPermission((short) 01770))
        .build();

  public final static HdfsFileStatus DOT_SNAPSHOT_DIR_STATUS =
      new HdfsFileStatus.Builder()
        .isdir(true)
        .build();
  //表示文件系统的根目录。根目录在初始化时会被创建，并具有一定的默认特性，如配额、快照支持等
  INodeDirectory rootDir;
  //HDFS文件系统的核心，负责处理文件系统的各种操作，如文件的创建、删除、命名等
  private final FSNamesystem namesystem;
  //布尔类型，用于在消费编辑日志时跳过配额检查。通常在恢复操作中使用
  private volatile boolean skipQuotaCheck = false; //skip while consuming edits
  //表示路径组件（文件或目录名）的最大长度
  private final int maxComponentLength;
  //表示目录中最大允许的条目数
  private final int maxDirItems;
  //表示在列出目录内容时的最大限制
  private final int lsLimit;  // max list limit
  //表示每次操作中内容统计的最大计数
  private final int contentCountLimit; // max content summary counts per run
  //表示当列出内容时的延迟时间，单位是微秒
  private final long contentSleepMicroSec;
  //用于存储和管理所有的INode（索引节点）。通过它可以查找、操作文件系统中的文件和目录
  private final INodeMap inodeMap; // Synchronized by dirLock
  //表示尝试获取锁时的“yield”次数，通常用于锁竞争时的调度
  private long yieldCount = 0; // keep track of lock yield count.
  //表示初始化配额检查时的线程数
  private int quotaInitThreads;
  //表示每个inode（索引节点）允许的最大xattr（扩展属性）数目
  private final int inodeXAttrsLimit; //inode xattrs max limit

  // A set of directories that have been protected using the
  // dfs.namenode.protected.directories setting. These directories cannot
  // be deleted unless they are empty.
  //
  // Each entry in this set must be a normalized path.
  //存储被保护的目录，这些目录不能被删除，除非它们为空
  private volatile SortedSet<String> protectedDirectories;
  //布尔类型，表示是否启用受保护子目录的功能
  private final boolean isProtectedSubDirectoriesEnable;
  //布尔类型，表示是否启用权限检查
  private final boolean isPermissionEnabled;
  //表示是否启用权限内容摘要的子访问功能
  private final boolean isPermissionContentSummarySubAccess;
  /**
   * Support for ACLs is controlled by a configuration flag. If the
   * configuration flag is false, then the NameNode will reject all
   * ACL-related operations.
   */
  //表示是否启用ACL（访问控制列表）
  private final boolean aclsEnabled;
  /** Threshold to print a warning. */
  //表示访问控制强制执行器的报告阈值
  private final long accessControlEnforcerReportingThresholdMs;
  /**
   * Support for POSIX ACL inheritance. Not final for testing purpose.
   */
  //表示是否启用POSIX ACL继承
  private boolean posixAclInheritanceEnabled;
  //表示是否启用xattrs（扩展属性）
  private final boolean xattrsEnabled;
  //表示xattr的最大大小
  private final int xattrMaxSize;

  // precision of access times.
  //表示访问时间的精度
  private final long accessTimePrecision;
  // whether quota by storage type is allowed
  //表示是否启用按存储类型进行配额限制
  private final boolean quotaByStorageTypeEnabled;
  //表示文件系统所有者的简短用户名
  private final String fsOwnerShortUserName;
  //表示超级用户组的名称
  private final String supergroup;
  //用于唯一标识一个inode
  private final INodeId inodeId;
  //文件系统编辑日志，用于记录文件系统的变更
  private final FSEditLog editLog;
  //存储保留状态的文件信息
  private HdfsFileStatus[] reservedStatuses;
  //提供外部inode属性的接口
  private INodeAttributeProvider attributeProvider;

  // A HashSet of principals of users for whom the external attribute provider
  // will be bypassed
  //存储那些绕过外部属性提供程序的用户列表
  private HashSet<String> usersToBypassExtAttrProvider = null;

  // If external inode attribute provider is configured, use the new
  // authorizeWithContext() API or not.
  //表示是否使用带有上下文的权限检查API
  private boolean useAuthorizationWithContextAPI = false;
  //主要用于在 HDFS 中设置 INodeAttributeProvider（INode 属性提供者），并根据该提供者是否支持新的授权 API 进行动态切换
  //INodeAttributeProvider：用于提供对 HDFS 中 INode 属性的自定义处理（如访问控制、配额等）
  public void setINodeAttributeProvider(
      @Nullable INodeAttributeProvider provider) {
    attributeProvider = provider;

    if (attributeProvider == null) {
      // attributeProvider is set to null during NN shutdown.
      return;
    }

    // if the runtime external authorization provider doesn't support
    // checkPermissionWithContext(), fall back to the old API
    // checkPermission().
    // This check is done only once during NameNode initialization to reduce
    // runtime overhead.
    Class[] cArg = new Class[1];
    cArg[0] = INodeAttributeProvider.AuthorizationContext.class;

    INodeAttributeProvider.AccessControlEnforcer enforcer =
        attributeProvider.getExternalAccessControlEnforcer(null);

    // If external enforcer is null, we use the default enforcer, which
    // supports the new API.
    if (enforcer == null) {
      useAuthorizationWithContextAPI = true;
      return;
    }

    try {
      Class<?> clazz = enforcer.getClass();
      clazz.getDeclaredMethod("checkPermissionWithContext", cArg);
      useAuthorizationWithContextAPI = true;
      LOG.info("Use the new authorization provider API");
    } catch (NoSuchMethodException e) {
      useAuthorizationWithContextAPI = false;
      LOG.info("Fallback to the old authorization provider API because " +
          "the expected method is not found.");
    }
  }

  /**
   * The directory lock dirLock provided redundant locking.
   * It has been used whenever namesystem.fsLock was used.
   * dirLock is now removed and utility methods to acquire and release dirLock
   * remain as placeholders only
   */
  void readLock() {
    assert namesystem.hasReadLock() : "Should hold namesystem read lock";
  }

  void readUnlock() {
    assert namesystem.hasReadLock() : "Should hold namesystem read lock";
  }

  void writeLock() {
    assert namesystem.hasWriteLock() : "Should hold namesystem write lock";
  }

  void writeUnlock() {
    assert namesystem.hasWriteLock() : "Should hold namesystem write lock";
  }

  boolean hasWriteLock() {
    return namesystem.hasWriteLock();
  }

  boolean hasReadLock() {
    return namesystem.hasReadLock();
  }

  @Deprecated // dirLock is obsolete, use namesystem.fsLock instead
  public int getReadHoldCount() {
    return namesystem.getReadHoldCount();
  }

  @Deprecated // dirLock is obsolete, use namesystem.fsLock instead
  public int getWriteHoldCount() {
    return namesystem.getWriteHoldCount();
  }

  public int getListLimit() {
    return lsLimit;
  }

  @VisibleForTesting
  public final EncryptionZoneManager ezManager;//管理加密区域的功能

  /**
   * Caches frequently used file names used in {@link INode} to reuse 
   * byte[] objects and reduce heap usage.
   */
  private final NameCache<ByteArray> nameCache; //用于缓存频繁使用的文件名，以减少堆内存的使用

  // used to specify path resolution type. *_LINK will return symlinks instead
  // of throwing an unresolved exception
  public enum DirOp {
    READ, //表示对路径的读取。路径是正常解析的，直接读取文件或目录内容
    READ_LINK, //表示解析符号链接（symlink）。如果遇到符号链接，系统会返回该链接指向的目标，而不是抛出未解析的异常
    WRITE,  // disallows snapshot paths.//表示对路径进行写入操作。与 READ 操作不同，WRITE 操作还会有额外的限制，比如禁止对快照路径进行写入操作（即不允许在快照路径下进行写入）
    WRITE_LINK, //表示创建或修改符号链接。它允许在符号链接的路径上进行写入操作，即可以创建新的符号链接或修改现有的符号链接
    CREATE, // like write, but also blocks invalid path names.创建一个新的路径。与 WRITE 操作不同，CREATE 操作不仅会执行写操作，还会阻止创建无效的路径名
    CREATE_LINK; //表示创建符号链接。与 CREATE 操作相似，但它专门用于创建符号链接，而不是普通文件或目录
  };

  FSDirectory(FSNamesystem ns, Configuration conf) throws IOException {
    this.inodeId = new INodeId();
    rootDir = createRoot(ns);  //设置根目录
    inodeMap = INodeMap.newInstance(rootDir); //把根目录放入INodeMap缓存
    this.isPermissionEnabled = conf.getBoolean(
      DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY,
      DFSConfigKeys.DFS_PERMISSIONS_ENABLED_DEFAULT); //是否启用 HDFS 文件权限检查，取决于 dfs.permissions.enabled 配置
    this.isPermissionContentSummarySubAccess = conf.getBoolean(
        DFSConfigKeys.DFS_PERMISSIONS_CONTENT_SUMMARY_SUBACCESS_KEY,
        DFSConfigKeys.DFS_PERMISSIONS_CONTENT_SUMMARY_SUBACCESS_DEFAULT);//是否在计算内容摘要时进行子目录的访问权限检查，受 dfs.permissions.content-summary.subaccess 配置控制
    this.fsOwnerShortUserName =
      UserGroupInformation.getCurrentUser().getShortUserName();//获取当前文件系统所有者的短用户名（不含域名）
    this.supergroup = conf.get(
      DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_KEY,
      DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_DEFAULT);//HDFS 超级用户组名称，通常用于身份验证和授权
    this.aclsEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY,
        DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_DEFAULT);//是否启用访问控制列表（ACLs），由 dfs.namenode.acls.enabled 决定
    LOG.info("ACLs enabled? " + aclsEnabled);
    this.posixAclInheritanceEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_POSIX_ACL_INHERITANCE_ENABLED_KEY,
        DFSConfigKeys.DFS_NAMENODE_POSIX_ACL_INHERITANCE_ENABLED_DEFAULT);//是否支持 POSIX ACL 继承，决定子目录是否继承父目录的 POSIX ACL
    LOG.info("POSIX ACL inheritance enabled? " + posixAclInheritanceEnabled);
    this.xattrsEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_XATTRS_ENABLED_KEY,
        DFSConfigKeys.DFS_NAMENODE_XATTRS_ENABLED_DEFAULT);//是否支持扩展属性（XAttrs），如存储自定义的文件元数据
    LOG.info("XAttrs enabled? " + xattrsEnabled);
    this.xattrMaxSize = (int) conf.getLongBytes(
        DFSConfigKeys.DFS_NAMENODE_MAX_XATTR_SIZE_KEY,
        DFSConfigKeys.DFS_NAMENODE_MAX_XATTR_SIZE_DEFAULT);//扩展属性的最大允许大小，受 dfs.namenode.max.xattr.size 控制
    Preconditions.checkArgument(xattrMaxSize > 0,
        "The maximum size of an xattr should be > 0: (%s).",
        DFSConfigKeys.DFS_NAMENODE_MAX_XATTR_SIZE_KEY);
    Preconditions.checkArgument(xattrMaxSize <=
        DFSConfigKeys.DFS_NAMENODE_MAX_XATTR_SIZE_HARD_LIMIT,
        "The maximum size of an xattr should be <= maximum size"
        + " hard limit " + DFSConfigKeys.DFS_NAMENODE_MAX_XATTR_SIZE_HARD_LIMIT
        + ": (%s).", DFSConfigKeys.DFS_NAMENODE_MAX_XATTR_SIZE_KEY);

    this.accessTimePrecision = conf.getLong(
        DFS_NAMENODE_ACCESSTIME_PRECISION_KEY,
        DFS_NAMENODE_ACCESSTIME_PRECISION_DEFAULT);//设置访问时间精度，精确到毫秒级，减少访问时间更新频率

    this.quotaByStorageTypeEnabled =
        conf.getBoolean(DFS_QUOTA_BY_STORAGETYPE_ENABLED_KEY,
                        DFS_QUOTA_BY_STORAGETYPE_ENABLED_DEFAULT);//是否按存储类型（SSD、HDD）进行配额限制，受 dfs.quota.by.storagetype.enabled 控制

    int configuredLimit = conf.getInt(
        DFSConfigKeys.DFS_LIST_LIMIT, DFSConfigKeys.DFS_LIST_LIMIT_DEFAULT);
    this.lsLimit = configuredLimit>0 ?
        configuredLimit : DFSConfigKeys.DFS_LIST_LIMIT_DEFAULT;//限制每次列出目录项的最大数量，防止目录列表过大导致内存消耗
    this.contentCountLimit = conf.getInt(
        DFSConfigKeys.DFS_CONTENT_SUMMARY_LIMIT_KEY,
        DFSConfigKeys.DFS_CONTENT_SUMMARY_LIMIT_DEFAULT);//内容摘要操作的最大文件和目录数量，限制内容统计的粒度
    this.contentSleepMicroSec = conf.getLong(
        DFSConfigKeys.DFS_CONTENT_SUMMARY_SLEEP_MICROSEC_KEY,
        DFSConfigKeys.DFS_CONTENT_SUMMARY_SLEEP_MICROSEC_DEFAULT);//在内容摘要计算中每次处理后的休眠时间，防止 NameNode 负载过高
    
    // filesystem limits
    this.maxComponentLength = (int) conf.getLongBytes(
        DFSConfigKeys.DFS_NAMENODE_MAX_COMPONENT_LENGTH_KEY,
        DFSConfigKeys.DFS_NAMENODE_MAX_COMPONENT_LENGTH_DEFAULT);//路径组件（即目录或文件名）的最大长度限制，确保文件名合理性
    this.maxDirItems = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_KEY,
        DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_DEFAULT);//目录中允许的最大条目数，防止单个目录下文件或子目录过多
    this.inodeXAttrsLimit = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_KEY,
        DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_DEFAULT);//每个 INode 上的最大扩展属性数，控制扩展属性的数量

    this.protectedDirectories = parseProtectedDirectories(conf);//受保护目录的集合，防止特定系统关键路径被修改或删除
    this.isProtectedSubDirectoriesEnable = conf.getBoolean(
        DFS_PROTECTED_SUBDIRECTORIES_ENABLE,
        DFS_PROTECTED_SUBDIRECTORIES_ENABLE_DEFAULT);//是否启用对受保护子目录的保护，避免重要子目录被误操作

    this.accessControlEnforcerReportingThresholdMs = conf.getLong(
        DFS_NAMENODE_ACCESS_CONTROL_ENFORCER_REPORTING_THRESHOLD_MS_KEY,
        DFS_NAMENODE_ACCESS_CONTROL_ENFORCER_REPORTING_THRESHOLD_MS_DEFAULT);//访问控制执行器报告的阈值，超过此时间会记录警告日志

    Preconditions.checkArgument(this.inodeXAttrsLimit >= 0,
        "Cannot set a negative limit on the number of xattrs per inode (%s).",
        DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_KEY);
    // We need a maximum maximum because by default, PB limits message sizes
    // to 64MB. This means we can only store approximately 6.7 million entries
    // per directory, but let's use 6.4 million for some safety.
    final int MAX_DIR_ITEMS = 64 * 100 * 1000;
    Preconditions.checkArgument(
        maxDirItems > 0 && maxDirItems <= MAX_DIR_ITEMS, "Cannot set "
            + DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_KEY
            + " to a value less than 1 or greater than " + MAX_DIR_ITEMS);

    int threshold = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_NAME_CACHE_THRESHOLD_KEY,
        DFSConfigKeys.DFS_NAMENODE_NAME_CACHE_THRESHOLD_DEFAULT);
    NameNode.LOG.info("Caching file names occurring more than " + threshold
        + " times");
    nameCache = new NameCache<ByteArray>(threshold);//缓存高频访问的文件名，优化文件名查找性能
    namesystem = ns;
    this.editLog = ns.getEditLog();//操作日志记录器，跟踪文件系统的修改操作，支持 HDFS 崩溃恢复
    ezManager = new EncryptionZoneManager(this, conf);//管理加密区（Encryption Zone），支持 HDFS 数据加密

    this.quotaInitThreads = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_QUOTA_INIT_THREADS_KEY,
        DFSConfigKeys.DFS_NAMENODE_QUOTA_INIT_THREADS_DEFAULT);//用于初始化配额的线程数，优化大规模文件系统的配额计算

    initUsersToBypassExtProvider(conf);
  }
  //用于初始化一个用户列表，这些用户在访问文件时将 绕过外部属性提供程序（External Attribute Provider）
  private void initUsersToBypassExtProvider(Configuration conf) {
    //获取绕过外部属性提供程序的用户列表
    String[] bypassUsers = conf.getTrimmedStrings(
        DFSConfigKeys.DFS_NAMENODE_INODE_ATTRIBUTES_PROVIDER_BYPASS_USERS_KEY,
        DFSConfigKeys.DFS_NAMENODE_INODE_ATTRIBUTES_PROVIDER_BYPASS_USERS_DEFAULT);
    for(int i = 0; i < bypassUsers.length; i++) {
      String tmp = bypassUsers[i].trim();
      if (!tmp.isEmpty()) {
        if (usersToBypassExtAttrProvider == null) {
          usersToBypassExtAttrProvider = new HashSet<String>();
        }
        LOG.info("Add user " + tmp + " to the list that will bypass external"
            + " attribute provider.");
        usersToBypassExtAttrProvider.add(tmp);
      }
    }
  }

  /**如果用户配置为绕过外部属性提供程序，返回 true，否则返回 false
   * Check if a given user is configured to bypass external attribute provider.
   * @param user user principal
   * @return true if the user is to bypass external attribute provider
   */
  private boolean isUserBypassingExtAttrProvider(final String user) {
    return (usersToBypassExtAttrProvider != null) &&
          usersToBypassExtAttrProvider.contains(user);
  }

  /**
   * Return attributeProvider or null if ugi is to bypass attributeProvider.
   * @param ugi
   * @return configured attributeProvider or null
   */
  //根据当前用户的身份信息（UserGroupInformation 对象），返回适用的 INodeAttributeProvider 实例，
  // 如果用户被配置为绕过外部属性提供程序，则返回 null
  private INodeAttributeProvider getUserFilteredAttributeProvider(
      UserGroupInformation ugi) {
    if (attributeProvider == null ||
        (ugi != null && isUserBypassingExtAttrProvider(ugi.getUserName()))) {
      return null;
    }
    return attributeProvider;
  }

  /**
   * Get HdfsFileStatuses of the reserved paths: .inodes and raw.
   *获取 HDFS 文件系统的保留路径（如 .inodes 和 /raw）的状态信息
   * 保留路径：HDFS 中具有特殊含义的路径，通常由系统内部使用，普通用户无法直接访问
   * .inodes：HDFS 的内部 INode 视图路径，提供对底层元数据的访问
   * raw：用于加密区（Encryption Zone），提供未经加密的数据访问
   * @return Array of HdfsFileStatus
   */
  HdfsFileStatus[] getReservedStatuses() {
    Preconditions.checkNotNull(reservedStatuses, "reservedStatuses should "
        + " not be null. It is populated when FSNamesystem loads FS image."
        + " It has to be set at this time instead of initialization time"
        + " because CTime is loaded during FSNamesystem#loadFromDisk.");
    return reservedStatuses;
  }

  /**
   * Create HdfsFileStatuses of the reserved paths: .inodes and raw.
   * These statuses are solely for listing purpose. All other operations
   * on the reserved dirs are disallowed.
   * Operations on sub directories are resolved by
   * {@link FSDirectory#resolvePath(String, FSDirectory)}
   * and conducted directly, without the need to check the reserved dirs.
   *
   * This method should only be invoked once during namenode initialization.
   *
   * @param cTime CTime of the file system
   * @return Array of HdfsFileStatus
   */
  void createReservedStatuses(long cTime) {
    HdfsFileStatus inodes = new HdfsFileStatus.Builder()
        .isdir(true)
        .mtime(cTime)
        .atime(cTime)
        .perm(new FsPermission((short) 0770))
        .group(supergroup)
        .path(DOT_INODES)
        .build();
    HdfsFileStatus raw = new HdfsFileStatus.Builder()
        .isdir(true)
        .mtime(cTime)
        .atime(cTime)
        .perm(new FsPermission((short) 0770))
        .group(supergroup)
        .path(RAW)
        .build();
    reservedStatuses = new HdfsFileStatus[] {inodes, raw};
  }

  FSNamesystem getFSNamesystem() {
    return namesystem;
  }

  /**
   * Indicates whether the image loading is complete or not.
   * @return true if image loading is complete, false otherwise
   */
  public boolean isImageLoaded() {
    return namesystem.isImageLoaded();
  }

  /**
   * Parse configuration setting dfs.namenode.protected.directories to
   * retrieve the set of protected directories.
   * 解析 HDFS 配置项 dfs.namenode.protected.directories，将其中的 受保护目录 提取出来，返回一个排序后的集合
   * @param conf
   * @return a TreeSet
   */
  @VisibleForTesting
  static SortedSet<String> parseProtectedDirectories(Configuration conf) {
    return parseProtectedDirectories(conf
        .getTrimmedStringCollection(FS_PROTECTED_DIRECTORIES));
  }

  /**
   * Parse configuration setting dfs.namenode.protected.directories to retrieve
   * the set of protected directories.
   *
   * @param protectedDirsString
   *          a comma separated String representing a bunch of paths.
   * @return a TreeSet
   */
  @VisibleForTesting
  static SortedSet<String> parseProtectedDirectories(
      final String protectedDirsString) {
    return parseProtectedDirectories(StringUtils
        .getTrimmedStringCollection(protectedDirsString));
  }

  private static SortedSet<String> parseProtectedDirectories(
      final Collection<String> protectedDirs) {
    // Normalize each input path to guard against administrator error.
    return new TreeSet<>(
        normalizePaths(protectedDirs, FS_PROTECTED_DIRECTORIES));
  }

  public SortedSet<String> getProtectedDirectories() {
    return protectedDirectories;
  }

  public boolean isProtectedSubDirectoriesEnable() {
    return isProtectedSubDirectoriesEnable;
  }

  /**
   * Set directories that cannot be removed unless empty, even by an
   * administrator.
   * 设置受保护的目录
   * @param protectedDirsString
   *          comma separated list of protected directories
   */
  String setProtectedDirectories(String protectedDirsString) {
    if (protectedDirsString == null) {
      protectedDirectories = new TreeSet<>();
    } else {
      protectedDirectories = parseProtectedDirectories(protectedDirsString);
    }

    return Joiner.on(",").skipNulls().join(protectedDirectories);
  }
  //获取BlockManager
  BlockManager getBlockManager() {
    return getFSNamesystem().getBlockManager();
  }

  KeyProviderCryptoExtension getProvider() {
    return getFSNamesystem().getProvider();
  }

  /** @return the root directory inode. */
  //获取root目录
  public INodeDirectory getRoot() {
    return rootDir;
  }

  public BlockStoragePolicySuite getBlockStoragePolicySuite() {
    return getBlockManager().getStoragePolicySuite();
  }

  boolean isPermissionEnabled() {
    return isPermissionEnabled;
  }
  boolean isAclsEnabled() {
    return aclsEnabled;
  }
  boolean isPermissionContentSummarySubAccess() {
    return isPermissionContentSummarySubAccess;
  }

  @VisibleForTesting
  public boolean isPosixAclInheritanceEnabled() {
    return posixAclInheritanceEnabled;
  }

  @VisibleForTesting
  public void setPosixAclInheritanceEnabled(
      boolean posixAclInheritanceEnabled) {
    this.posixAclInheritanceEnabled = posixAclInheritanceEnabled;
  }

  boolean isXattrsEnabled() {
    return xattrsEnabled;
  }
  int getXattrMaxSize() { return xattrMaxSize; }

  boolean isAccessTimeSupported() {
    return accessTimePrecision > 0;
  }
  long getAccessTimePrecision() {
    return accessTimePrecision;
  }
  boolean isQuotaByStorageTypeEnabled() {
    return quotaByStorageTypeEnabled;
  }


  int getLsLimit() {
    return lsLimit;
  }

  int getContentCountLimit() {
    return contentCountLimit;
  }

  long getContentSleepMicroSec() {
    return contentSleepMicroSec;
  }

  int getInodeXAttrsLimit() {
    return inodeXAttrsLimit;
  }

  FSEditLog getEditLog() {
    return editLog;
  }

  /**
   * Shutdown the filestore
   */
  @Override
  public void close() throws IOException {}
  //初始化文件名字缓存
  void markNameCacheInitialized() {
    writeLock();
    try {
      nameCache.initialized();
    } finally {
      writeUnlock();
    }
  }

  boolean shouldSkipQuotaChecks() {
    return skipQuotaCheck;
  }

  /** Enable quota verification */
  void enableQuotaChecks() {
    skipQuotaCheck = false;
  }

  /** Disable quota verification */
  void disableQuotaChecks() {
    skipQuotaCheck = true;
  }

  /**
   * Resolves a given path into an INodesInPath.  All ancestor inodes that
   * exist are validated as traversable directories.  Symlinks in the ancestry
   * will generate an UnresolvedLinkException.  The returned IIP will be an
   * accessible path that also passed additional sanity checks based on how
   * the path will be used as specified by the DirOp.
   *   READ:   Expands reserved paths and performs permission checks
   *           during traversal.  Raw paths are only accessible by a superuser.
   *   WRITE:  In addition to READ checks, ensures the path is not a
   *           snapshot path.
   *   CREATE: In addition to WRITE checks, ensures path does not contain
   *           illegal character sequences.
   *
   * @param pc  A permission checker for traversal checks.  Pass null for
   *            no permission checks. pc：FSPermissionChecker 对象，用于执行权限检查
   * @param src The path to resolve. 待解析的路径（String 类型）
   * @param dirOp The {@link DirOp} that controls additional checks. 表示路径解析时需要执行的操作类型（例如 READ、WRITE 等）
   * @return if the path indicates an inode, return path after replacing up to
   *        {@code <inodeid>} with the corresponding path of the inode, else
   *        the path in {@code src} as is. If the path refers to a path in
   *        the "raw" directory, return the non-raw pathname.
   * @throws FileNotFoundException
   * @throws AccessControlException
   * @throws ParentNotDirectoryException
   * @throws UnresolvedLinkException
   */
  //解析路径并返回一个 INodesInPath 对象。它会检查路径的合法性，进行权限验证，以及确保路径中的所有父目录都是可遍历的目录。
  // 如果路径涉及符号链接，或者在创建、修改操作中使用，会进行适当的处理
  @VisibleForTesting
  public INodesInPath resolvePath(FSPermissionChecker pc, String src,
      DirOp dirOp) throws UnresolvedLinkException, FileNotFoundException,
      AccessControlException, ParentNotDirectoryException {
    //检查是否是创建操作（CREATE 或 CREATE_LINK），
    // 如果是，调用 DFSUtil.isValidName(src) 检查路径是否合法。如果路径不合法，抛出 InvalidPathException
    boolean isCreate = (dirOp == DirOp.CREATE || dirOp == DirOp.CREATE_LINK);
    // prevent creation of new invalid paths
    if (isCreate && !DFSUtil.isValidName(src)) {
      throw new InvalidPathException("Invalid file name: " + src);
    }
    //获取路径组件并解
    byte[][] components = INode.getPathComponents(src);
    //检查路径是否为 "raw" 路径
    boolean isRaw = isReservedRawName(components);
    components = resolveComponents(components, this);
    INodesInPath iip = INodesInPath.resolve(rootDir, components, isRaw);
    //如果启用了权限检查，并且路径是 raw 路径，则根据操作类型（READ、WRITE 等）进行权限检查。如果是写入操作，确保只有超级用户才能访问 raw 路径
    if (isPermissionEnabled && pc != null && isRaw) {
      switch(dirOp) {
      case READ_LINK:
      case READ:
        break;
      default:
        pc.checkSuperuserPrivilege(iip.getPath());
        break;
      }
    }
    // verify all ancestors are dirs and traversable.  note that only
    // methods that create new namespace items have the signature to throw
    // PNDE
    //验证路径的所有父目录是可遍历的目录
    try {
      checkTraverse(pc, iip, dirOp);
    } catch (ParentNotDirectoryException pnde) {
      if (!isCreate) {
        throw new AccessControlException(pnde.getMessage());
      }
      throw pnde;
    }
    return iip;
  }

  /**
   * This method should only be used from internal paths and not those provided
   * directly by a user. It resolves a given path into an INodesInPath in a
   * similar way to resolvePath(...), only traversal and permissions are not
   * checked.
   * @param src The path to resolve.
   * @return if the path indicates an inode, return path after replacing up to
   *        {@code <inodeid>} with the corresponding path of the inode, else
   *        the path in {@code src} as is. If the path refers to a path in
   *        the "raw" directory, return the non-raw pathname.
   * @throws FileNotFoundException
   */
  public INodesInPath unprotectedResolvePath(String src)
      throws FileNotFoundException {
    byte[][] components = INode.getPathComponents(src);
    boolean isRaw = isReservedRawName(components);
    components = resolveComponents(components, this);
    return INodesInPath.resolve(rootDir, components, isRaw);
  }
  //pc：FSPermissionChecker 对象，用于执行权限检查
  //src：待解析的路径（String 类型）
  //fileId：文件的 inode ID（long 类型）
  //解析路径并返回一个 INodesInPath 对象。它根据提供的文件 ID（fileId）解析路径，若文件 ID 为 GRANDFATHER_INODE_ID，则直接调用 resolvePath 方法处理路径。
  // 如果文件 ID 有效，则使用该 inode 解析路径
  INodesInPath resolvePath(FSPermissionChecker pc, String src, long fileId)
      throws UnresolvedLinkException, FileNotFoundException,
      AccessControlException, ParentNotDirectoryException {
    // Older clients may not have given us an inode ID to work with.
    // In this case, we have to try to resolve the path and hope it
    // hasn't changed or been deleted since the file was opened for write.
    INodesInPath iip;
    //如果 fileId 是常量 GRANDFATHER_INODE_ID，表示该文件来自较旧的客户端，并且没有提供 inode ID。
    // 在这种情况下，调用 resolvePath 方法并指定操作类型为 WRITE 来解析路径
    if (fileId == HdfsConstants.GRANDFATHER_INODE_ID) {
      iip = resolvePath(pc, src, DirOp.WRITE);
    } else {
      INode inode = getInode(fileId);
      if (inode == null) {
        iip = INodesInPath.fromComponents(INode.getPathComponents(src));
      } else {
        iip = INodesInPath.fromINode(inode);
      }
    }
    return iip;
  }

  // this method can be removed after IIP is used more extensively
  static String resolvePath(String src,
      FSDirectory fsd) throws FileNotFoundException {
    byte[][] pathComponents = INode.getPathComponents(src);
    pathComponents = resolveComponents(pathComponents, fsd);
    return DFSUtil.byteArray2PathString(pathComponents);
  }

  /**
   * @return true if the path is a non-empty directory; otherwise, return false.
   */
  public boolean isNonEmptyDirectory(INodesInPath inodesInPath) {
    readLock();
    try {
      final INode inode = inodesInPath.getLastINode();
      if (inode == null || !inode.isDirectory()) {
        //not found or not a directory
        return false;
      }
      final int s = inodesInPath.getPathSnapshotId();
      return !inode.asDirectory().getChildrenList(s).isEmpty();
    } finally {
      readUnlock();
    }
  }

  /**
   * Check whether the filepath could be created
   * @throws SnapshotAccessControlException if path is in RO snapshot
   */
  boolean isValidToCreate(String src, INodesInPath iip)
      throws SnapshotAccessControlException {
    String srcs = normalizePath(src);
    return srcs.startsWith("/") && !srcs.endsWith("/") &&
        iip.getLastINode() == null;
  }

  /**
   * Tell the block manager to update the replication factors when delete
   * happens. Deleting a file or a snapshot might decrease the replication
   * factor of the blocks as the blocks are always replicated to the highest
   * replication factor among all snapshots.
   */
  void updateReplicationFactor(Collection<UpdatedReplicationInfo> blocks) {
    BlockManager bm = getBlockManager();
    for (UpdatedReplicationInfo e : blocks) {
      BlockInfo b = e.block();
      bm.setReplication(b.getReplication(), e.targetReplication(), b);
    }
  }

  /**
   * Update the count of each directory with quota in the namespace.
   * A directory's count is defined as the total number inodes in the tree
   * rooted at the directory.
   *
   * This is an update of existing state of the filesystem and does not
   * throw QuotaExceededException.
   */
  //主要目的是更新文件系统中每个目录的配额计数。它计算每个目录下的 inode 总数，并在文件系统中更新相应的状态。
  // 此操作是对现有文件系统状态的更新，不会抛出 QuotaExceededException 异常
  void updateCountForQuota(int initThreads) {
    //在进行配额更新时，需要获取写锁，以确保操作的线程安全，防止其他线程并发修改文件系统
    writeLock();
    try {
      int threads = (initThreads < 1) ? 1 : initThreads;
      LOG.info("Initializing quota with " + threads + " thread(s)");
      long start = Time.monotonicNow();
      QuotaCounts counts = new QuotaCounts.Builder().build();
      ForkJoinPool p = new ForkJoinPool(threads);
      RecursiveAction task = new InitQuotaTask(getBlockStoragePolicySuite(),
          rootDir.getStoragePolicyID(), rootDir, counts);
      p.execute(task);
      task.join();
      p.shutdown();
      LOG.info("Quota initialization completed in " + (Time.monotonicNow() - start) +
          " milliseconds\n" + counts);
    } finally {
      writeUnlock();
    }
  }

  void updateCountForQuota() {
    updateCountForQuota(quotaInitThreads);
  }

  /**
   * parallel initialization using fork-join.
   */
  //RecursiveAction 是 Java 并行框架中的一种任务类型，用于拆分任务以进行并行处理
  //与 RecursiveTask 类似，但 RecursiveAction 不返回结果。它主要用于没有返回值的任务，通常用于执行副作用操作，例如排序、数组填充等
  //继承 RecursiveAction：创建一个类继承 RecursiveAction 并实现 compute() 方法，在该方法中定义任务的递归逻辑
  //分解任务：在 compute() 方法中，将大任务分解成小任务，并通过 fork() 方法启动子任务
  //合并任务：任务拆分后，使用 join() 来合并任务的结果（如果有的话，尽管 RecursiveAction 本身不返回结果）
  //使用 ForkJoinPool 执行任务：通过 ForkJoinPool 来执行并行任务

  //负责递归地计算并更新文件系统中目录的配额使用情况
  private static class InitQuotaTask extends RecursiveAction {
    private final INodeDirectory dir;//正在计算配额使用情况的目录
    private final QuotaCounts counts;//用于存储和更新配额使用情况（如已用空间、命名空间等）的对象
    private final BlockStoragePolicySuite bsps;//一组用于计算配额的存储策略
    private final byte blockStoragePolicyId;//用于计算配额的存储策略ID
    //构造函数
    public InitQuotaTask(BlockStoragePolicySuite bsps,
        byte blockStoragePolicyId, INodeDirectory dir, QuotaCounts counts) {
      this.dir = dir;
      this.counts = counts;
      this.bsps = bsps;
      this.blockStoragePolicyId = blockStoragePolicyId;
    }

    public void compute() {
      QuotaCounts myCounts =  new QuotaCounts.Builder().build();
      //计算当前目录的配额使用情况
      dir.computeQuotaUsage4CurrentDirectory(bsps, blockStoragePolicyId,
          myCounts);
      //获取当前目录的子节点列表
      ReadOnlyList<INode> children =
          dir.getChildrenList(CURRENT_STATE_ID);
      //如果该目录有子节点，递归地对每个子节点进行配额计算。如果是目录类型，则创建新的 InitQuotaTask 任务；如果是文件或符号链接，则直接更新配额
      if (children.size() > 0) {
        List<InitQuotaTask> subtasks = new ArrayList<InitQuotaTask>();
        for (INode child : children) {
          final byte childPolicyId =
              child.getStoragePolicyIDForQuota(blockStoragePolicyId);
          if (child.isDirectory()) {
            subtasks.add(new InitQuotaTask(bsps, childPolicyId,
                child.asDirectory(), myCounts));
          } else {
            // file or symlink. count using the local counts variable
            myCounts.add(child.computeQuotaUsage(bsps, childPolicyId, false,
                CURRENT_STATE_ID));
          }
        }
        // invoke and wait for completion
        invokeAll(subtasks);//// 执行所有子任务
      }
      //如果当前目录设置了配额，会检查计算出的使用情况是否超过了配额限制。如果超过，会记录警告日志
      if (dir.isQuotaSet()) {
        // check if quota is violated. It indicates a software bug.
        final QuotaCounts q = dir.getQuotaCounts();

        final long nsConsumed = myCounts.getNameSpace();
        final long nsQuota = q.getNameSpace();
        if (Quota.isViolated(nsQuota, nsConsumed)) {
          LOG.warn("Namespace quota violation in image for "
              + dir.getFullPathName()
              + " quota = " + nsQuota + " < consumed = " + nsConsumed);
        }

        final long ssConsumed = myCounts.getStorageSpace();
        final long ssQuota = q.getStorageSpace();
        if (Quota.isViolated(ssQuota, ssConsumed)) {
          LOG.warn("Storagespace quota violation in image for "
              + dir.getFullPathName()
              + " quota = " + ssQuota + " < consumed = " + ssConsumed);
        }

        final EnumCounters<StorageType> tsConsumed = myCounts.getTypeSpaces();
        for (StorageType t : StorageType.getTypesSupportingQuota()) {
          final long typeSpace = tsConsumed.get(t);
          final long typeQuota = q.getTypeSpaces().get(t);
          if (Quota.isViolated(typeQuota, typeSpace)) {
            LOG.warn("Storage type quota violation in image for "
                + dir.getFullPathName()
                + " type = " + t.toString() + " quota = "
                + typeQuota + " < consumed " + typeSpace);
          }
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Setting quota for " + dir + "\n" + myCounts);
        }
        //更新目录的配额信息： 最后，更新目录的配额信息
        dir.getDirectoryWithQuotaFeature().setSpaceConsumed(nsConsumed,
            ssConsumed, tsConsumed);
      }

      synchronized(counts) {
        counts.add(myCounts);
      }
    }
  }

  /** Updates namespace, storagespace and typespaces consumed for all
   * directories until the parent directory of file represented by path.
   *
   * @param iip the INodesInPath instance containing all the INodes for
   *            updating quota usage
   * @param nsDelta the delta change of namespace
   * @param ssDelta the delta change of storage space consumed without replication
   * @param replication the replication factor of the block consumption change
   * @throws QuotaExceededException if the new count violates any quota limit
   * @throws FileNotFoundException if path does not exist.
   */
  void updateSpaceConsumed(INodesInPath iip, long nsDelta, long ssDelta, short replication)
    throws QuotaExceededException, FileNotFoundException,
    UnresolvedLinkException, SnapshotAccessControlException {
    writeLock();
    try {
      if (iip.getLastINode() == null) {
        throw new FileNotFoundException("Path not found: " + iip.getPath());
      }
      updateCount(iip, nsDelta, ssDelta, replication, true);
    } finally {
      writeUnlock();
    }
  }

  public void updateCount(INodesInPath iip, INode.QuotaDelta quotaDelta,
      boolean check) throws QuotaExceededException {
    QuotaCounts counts = quotaDelta.getCountsCopy();
    updateCount(iip, iip.length() - 1, counts.negation(), check);
    Map<INode, QuotaCounts> deltaInOtherPaths = quotaDelta.getUpdateMap();
    for (Map.Entry<INode, QuotaCounts> entry : deltaInOtherPaths.entrySet()) {
      INodesInPath path = INodesInPath.fromINode(entry.getKey());
      updateCount(path, path.length() - 1, entry.getValue().negation(), check);
    }
    for (Map.Entry<INodeDirectory, QuotaCounts> entry :
        quotaDelta.getQuotaDirMap().entrySet()) {
      INodeDirectory quotaDir = entry.getKey();
      quotaDir.getDirectoryWithQuotaFeature().addSpaceConsumed2Cache(
          entry.getValue().negation());
    }
  }

  /**
   * Update the quota usage after deletion. The quota update is only necessary
   * when image/edits have been loaded and the file/dir to be deleted is not
   * contained in snapshots.
   */
  void updateCountForDelete(final INode inode, final INodesInPath iip) {
    if (getFSNamesystem().isImageLoaded() &&
        !inode.isInLatestSnapshot(iip.getLatestSnapshotId())) {
      QuotaCounts counts = inode.computeQuotaUsage(getBlockStoragePolicySuite());
      unprotectedUpdateCount(iip, iip.length() - 1, counts.negation());
    }
  }

  /**
   * Update usage count without replication factor change
   */
  void updateCount(INodesInPath iip, long nsDelta, long ssDelta, short replication,
      boolean checkQuota) throws QuotaExceededException {
    final INodeFile fileINode = iip.getLastINode().asFile();
    EnumCounters<StorageType> typeSpaceDeltas =
      getStorageTypeDeltas(fileINode.getStoragePolicyID(), ssDelta,
          replication, replication);
    updateCount(iip, iip.length() - 1,
      new QuotaCounts.Builder().nameSpace(nsDelta).storageSpace(ssDelta * replication).
          typeSpaces(typeSpaceDeltas).build(),
        checkQuota);
  }

  /**
   * Update usage count with replication factor change due to setReplication
   */
  void updateCount(INodesInPath iip, long nsDelta, long ssDelta, short oldRep,
      short newRep, boolean checkQuota) throws QuotaExceededException {
    final INodeFile fileINode = iip.getLastINode().asFile();
    EnumCounters<StorageType> typeSpaceDeltas =
        getStorageTypeDeltas(fileINode.getStoragePolicyID(), ssDelta, oldRep, newRep);
    updateCount(iip, iip.length() - 1,
        new QuotaCounts.Builder().nameSpace(nsDelta).
            storageSpace(ssDelta * (newRep - oldRep)).
            typeSpaces(typeSpaceDeltas).build(),
        checkQuota);
  }

  /** update count of each inode with quota
   * 
   * @param iip inodes in a path
   * @param numOfINodes the number of inodes to update starting from index 0
   * @param counts the count of space/namespace/type usage to be update
   * @param checkQuota if true then check if quota is exceeded
   * @throws QuotaExceededException if the new count violates any quota limit
   */
  void updateCount(INodesInPath iip, int numOfINodes,
                    QuotaCounts counts, boolean checkQuota)
                    throws QuotaExceededException {
    assert hasWriteLock();
    if (!namesystem.isImageLoaded()) {
      //still initializing. do not check or update quotas.
      return;
    }
    if (numOfINodes > iip.length()) {
      numOfINodes = iip.length();
    }
    if (checkQuota && !skipQuotaCheck) {
      verifyQuota(iip, numOfINodes, counts, null);
    }
    unprotectedUpdateCount(iip, numOfINodes, counts);
  }
  
  /** 
   * update quota of each inode and check to see if quota is exceeded. 
   * See {@link #updateCount(INodesInPath, int, QuotaCounts, boolean)}
   */ 
   void updateCountNoQuotaCheck(INodesInPath inodesInPath,
      int numOfINodes, QuotaCounts counts) {
    assert hasWriteLock();
    try {
      updateCount(inodesInPath, numOfINodes, counts, false);
    } catch (QuotaExceededException e) {
      NameNode.LOG.error("BUG: unexpected exception ", e);
    }
  }
  
  /**
   * updates quota without verification
   * callers responsibility is to make sure quota is not exceeded
   */
  static void unprotectedUpdateCount(INodesInPath inodesInPath,
      int numOfINodes, QuotaCounts counts) {
    for(int i=0; i < numOfINodes; i++) {
      if (inodesInPath.getINode(i).isQuotaSet()) { // a directory with quota
        inodesInPath.getINode(i).asDirectory().getDirectoryWithQuotaFeature()
            .addSpaceConsumed2Cache(counts);
      }
    }
  }

  /**
   * Update the cached quota space for a block that is being completed.
   * Must only be called once, as the block is being completed.
   * @param completeBlk - Completed block for which to update space
   * @param inodes - INodes in path to file containing completeBlk; if null
   *                 this will be resolved internally
   */
  public void updateSpaceForCompleteBlock(BlockInfo completeBlk,
      INodesInPath inodes) throws IOException {
    assert namesystem.hasWriteLock();
    INodesInPath iip = inodes != null ? inodes :
        INodesInPath.fromINode(namesystem.getBlockCollection(completeBlk));
    INodeFile fileINode = iip.getLastINode().asFile();
    // Adjust disk space consumption if required
    final long diff;
    final short replicationFactor;
    if (fileINode.isStriped()) {
      final ErasureCodingPolicy ecPolicy =
          FSDirErasureCodingOp
              .unprotectedGetErasureCodingPolicy(namesystem, iip);
      final short numDataUnits = (short) ecPolicy.getNumDataUnits();
      final short numParityUnits = (short) ecPolicy.getNumParityUnits();

      final long numBlocks = numDataUnits + numParityUnits;
      final long fullBlockGroupSize =
          fileINode.getPreferredBlockSize() * numBlocks;

      final BlockInfoStriped striped =
          new BlockInfoStriped(completeBlk, ecPolicy);
      final long actualBlockGroupSize = striped.spaceConsumed();

      diff = fullBlockGroupSize - actualBlockGroupSize;
      replicationFactor = (short) 1;
    } else {
      diff = fileINode.getPreferredBlockSize() - completeBlk.getNumBytes();
      replicationFactor = fileINode.getFileReplication();
    }
    if (diff > 0) {
      try {
        updateSpaceConsumed(iip, 0, -diff, replicationFactor);
      } catch (IOException e) {
        LOG.warn("Unexpected exception while updating disk space.", e);
      }
    }
  }

  public EnumCounters<StorageType> getStorageTypeDeltas(byte storagePolicyID,
      long dsDelta, short oldRep, short newRep) {
    EnumCounters<StorageType> typeSpaceDeltas =
        new EnumCounters<StorageType>(StorageType.class);
    // empty file
    if(dsDelta == 0){
      return typeSpaceDeltas;
    }
    // Storage type and its quota are only available when storage policy is set
    if (storagePolicyID != HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED) {
      BlockStoragePolicy storagePolicy = getBlockManager().getStoragePolicy(storagePolicyID);

      if (oldRep != newRep) {
        List<StorageType> oldChosenStorageTypes =
            storagePolicy.chooseStorageTypes(oldRep);

        for (StorageType t : oldChosenStorageTypes) {
          if (!t.supportTypeQuota()) {
            continue;
          }
          Preconditions.checkArgument(dsDelta > 0);
          typeSpaceDeltas.add(t, -dsDelta);
        }
      }

      List<StorageType> newChosenStorageTypes =
          storagePolicy.chooseStorageTypes(newRep);

      for (StorageType t : newChosenStorageTypes) {
        if (!t.supportTypeQuota()) {
          continue;
        }
        typeSpaceDeltas.add(t, dsDelta);
      }
    }
    return typeSpaceDeltas;
  }

  /**
   * Add the given child to the namespace.
   * @param existing the INodesInPath containing all the ancestral INodes
   * @param child the new INode to add
   * @param modes create modes
   * @return a new INodesInPath instance containing the new child INode. Null
   * if the adding fails.
   * @throws QuotaExceededException is thrown if it violates quota limit
   */
  INodesInPath addINode(INodesInPath existing, INode child,
                        FsPermission modes)
      throws QuotaExceededException, UnresolvedLinkException {
    cacheName(child);
    writeLock();
    try {
      return addLastINode(existing, child, modes, true);
    } finally {
      writeUnlock();
    }
  }

  /**
   * Verify quota for adding or moving a new INode with required 
   * namespace and storagespace to a given position.
   *  
   * @param iip INodes corresponding to a path
   * @param pos position where a new INode will be added
   * @param deltas needed namespace, storagespace and storage types
   * @param commonAncestor Last node in inodes array that is a common ancestor
   *          for a INode that is being moved from one location to the other.
   *          Pass null if a node is not being moved.
   * @throws QuotaExceededException if quota limit is exceeded.
   */
  static void verifyQuota(INodesInPath iip, int pos, QuotaCounts deltas,
                          INode commonAncestor) throws QuotaExceededException {
    if (deltas.getNameSpace() <= 0 && deltas.getStorageSpace() <= 0
        && deltas.getTypeSpaces().allLessOrEqual(0L)) {
      // if quota is being freed or not being consumed
      return;
    }

    // check existing components in the path
    for(int i = (pos > iip.length() ? iip.length(): pos) - 1; i >= 0; i--) {
      if (commonAncestor == iip.getINode(i)
          && !commonAncestor.isInLatestSnapshot(iip.getLatestSnapshotId())) {
        // Stop checking for quota when common ancestor is reached
        return;
      }
      final DirectoryWithQuotaFeature q
          = iip.getINode(i).asDirectory().getDirectoryWithQuotaFeature();
      if (q != null) { // a directory with quota
        try {
          q.verifyQuota(deltas);
        } catch (QuotaExceededException e) {
          e.setPathName(iip.getPath(i));
          throw e;
        }
      }
    }
  }

  /** Verify if the inode name is legal. */
  void verifyINodeName(byte[] childName) throws HadoopIllegalArgumentException {
    if (Arrays.equals(HdfsServerConstants.DOT_SNAPSHOT_DIR_BYTES, childName)) {
      String s = "\"" + HdfsConstants.DOT_SNAPSHOT_DIR + "\" is a reserved name.";
      if (!namesystem.isImageLoaded()) {
        s += "  Please rename it before upgrade.";
      }
      throw new HadoopIllegalArgumentException(s);
    }
  }

  /**
   * Verify child's name for fs limit.
   *
   * @param childName byte[] containing new child name
   * @param parentPath String containing parent path
   * @throws PathComponentTooLongException child's name is too long.
   */
  void verifyMaxComponentLength(byte[] childName, String parentPath)
      throws PathComponentTooLongException {
    if (maxComponentLength == 0) {
      return;
    }

    final int length = childName.length;
    if (length > maxComponentLength) {
      final PathComponentTooLongException e = new PathComponentTooLongException(
          maxComponentLength, length, parentPath,
          DFSUtil.bytes2String(childName));
      if (namesystem.isImageLoaded()) {
        throw e;
      } else {
        // Do not throw if edits log is still being processed
        NameNode.LOG.error("ERROR in FSDirectory.verifyINodeName", e);
      }
    }
  }

  /**
   * Verify children size for fs limit.
   *
   * @throws MaxDirectoryItemsExceededException too many children.
   */
  void verifyMaxDirItems(INodeDirectory parent, String parentPath)
      throws MaxDirectoryItemsExceededException {
    final int count = parent.getChildrenList(CURRENT_STATE_ID).size();
    if (count >= maxDirItems) {
      final MaxDirectoryItemsExceededException e
          = new MaxDirectoryItemsExceededException(parentPath, maxDirItems,
          count);
      if (namesystem.isImageLoaded()) {
        throw e;
      } else {
        // Do not throw if edits log is still being processed
        NameNode.LOG.error("FSDirectory.verifyMaxDirItems: "
            + e.getLocalizedMessage());
      }
    }
  }

  /**
   * Turn on HDFS-6962 POSIX ACL inheritance when the property
   * {@link DFSConfigKeys#DFS_NAMENODE_POSIX_ACL_INHERITANCE_ENABLED_KEY} is
   * true and a compatible client has sent both masked and unmasked create
   * modes.
   *
   * @param child INode newly created child
   * @param modes create modes
   */
  private void copyINodeDefaultAcl(INode child, FsPermission modes) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("child: {}, posixAclInheritanceEnabled: {}, modes: {}",
          child, posixAclInheritanceEnabled, modes);
    }

    if (posixAclInheritanceEnabled && modes != null &&
        modes.getUnmasked() != null) {
      //
      // HDFS-6962: POSIX ACL inheritance
      //
      child.setPermission(modes.getUnmasked());
      if (!AclStorage.copyINodeDefaultAcl(child)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("{}: no parent default ACL to inherit", child);
        }
        child.setPermission(modes.getMasked());
      }
    } else {
      //
      // Old behavior before HDFS-6962
      //
      AclStorage.copyINodeDefaultAcl(child);
    }
  }

  /**
   * Add a child to the end of the path specified by INodesInPath.
   * @param existing the INodesInPath containing all the ancestral INodes 包含所有祖先路径的 INodesInPath 实例。它提供了当前文件或目录的完整路径和每个路径部分的 INode
   * @param inode the new INode to add 即将添加到路径中的新 INode。它代表一个文件或目录
   * @param modes create modes 用于设置新 INode 的默认权限。当新文件或目录被添加时，它会继承一个权限模式
   * @param checkQuota whether to check quota 决定是否检查目录的配额。当为 true 时，方法会检查配额（如目录下的子项数量、文件数量等）
   * @return an INodesInPath instance containing the new INode
   */
  @VisibleForTesting
  public INodesInPath addLastINode(INodesInPath existing, INode inode,
      FsPermission modes, boolean checkQuota) throws QuotaExceededException {
    assert existing.getLastINode() != null &&
        existing.getLastINode().isDirectory();

    final int pos = existing.length();
    // Disallow creation of /.reserved. This may be created when loading
    // editlog/fsimage during upgrade since /.reserved was a valid name in older
    // release. This may also be called when a user tries to create a file
    // or directory /.reserved.
    //如果新 INode 的路径是根目录且文件名为保留文件名（如 /.reserved），则抛出异常，防止创建这些保留文件
    if (pos == 1 && existing.getINode(0) == rootDir && isReservedName(inode)) {
      throw new HadoopIllegalArgumentException(
          "File name \"" + inode.getLocalName() + "\" is reserved and cannot "
              + "be created. If this is during upgrade change the name of the "
              + "existing file or directory to another name before upgrading "
              + "to the new release.");
    }
    //获取父目录并检查配额
    final INodeDirectory parent = existing.getINode(pos - 1).asDirectory();
    // The filesystem limits are not really quotas, so this check may appear
    // odd. It's because a rename operation deletes the src, tries to add
    // to the dest, if that fails, re-adds the src from whence it came.
    // The rename code disables the quota when it's restoring to the
    // original location because a quota violation would cause the the item
    // to go "poof".  The fs limits must be bypassed for the same reason.
    if (checkQuota) {
      final String parentPath = existing.getPath();
      //验证文件名长度和目录项数量
      verifyMaxComponentLength(inode.getLocalNameBytes(), parentPath);
      verifyMaxDirItems(parent, parentPath);
    }
    // always verify inode name
    //验证新 INode 的名称是否合法
    verifyINodeName(inode.getLocalNameBytes());

    final boolean isSrcSetSp = inode.isSetStoragePolicy();
    final byte storagePolicyID = isSrcSetSp ?
        inode.getLocalStoragePolicyID() :
        parent.getStoragePolicyID();
    //计算新 INode 对配额的影响（如存储空间占用），并更新配额计数。computeQuotaUsage 方法会基于存储策略和快照 ID 计算新 INode 的配额使用情况
    final QuotaCounts counts = inode
        .computeQuotaUsage(getBlockStoragePolicySuite(),
            storagePolicyID, false, Snapshot.CURRENT_STATE_ID);
    updateCount(existing, pos, counts, checkQuota);

    boolean isRename = (inode.getParent() != null);
    //将新 INode 添加到父目录中。如果添加失败，则回滚配额更新，并返回 null
    final boolean added = parent.addChild(inode, true,
        existing.getLatestSnapshotId());
    if (!added) {
      updateCountNoQuotaCheck(existing, pos, counts.negation());
      return null;
    } else {
      //如果 INode 不是重命名操作，则复制默认的 ACL（访问控制列表）。然后将新 INode 添加到 INode 映射表中
      if (!isRename) {
        copyINodeDefaultAcl(inode, modes);
      }
      addToInodeMap(inode);
    }
    return INodesInPath.append(existing, inode, inode.getLocalNameBytes());
  }

  INodesInPath addLastINodeNoQuotaCheck(INodesInPath existing, INode i) {
    try {
      // All callers do not have create modes to pass.
      return addLastINode(existing, i, null, false);
    } catch (QuotaExceededException e) {
      NameNode.LOG.warn("FSDirectory.addChildNoQuotaCheck - unexpected", e);
    }
    return null;
  }

  /**
   * Remove the last inode in the path from the namespace.
   * Note: the caller needs to update the ancestors' quota count.
   *
   * @return -1 for failing to remove;
   *          0 for removing a reference whose referred inode has other 
   *            reference nodes;
   *          1 otherwise.
   */
  @VisibleForTesting
  public long removeLastINode(final INodesInPath iip) {
    final int latestSnapshot = iip.getLatestSnapshotId();
    final INode last = iip.getLastINode();
    final INodeDirectory parent = iip.getINode(-2).asDirectory();
    if (!parent.removeChild(last, latestSnapshot)) {
      return -1;
    }

    return (!last.isInLatestSnapshot(latestSnapshot)
        && INodeReference.tryRemoveReference(last) > 0) ? 0 : 1;
  }

  /**
   * Return a new collection of normalized paths from the given input
   * collection. The input collection is unmodified.
   *
   * Reserved paths, relative paths and paths with scheme are ignored.
   *
   * @param paths collection whose contents are to be normalized.
   * @return collection with all input paths normalized.
   */
  static Collection<String> normalizePaths(Collection<String> paths,
                                           String errorString) {
    if (paths.isEmpty()) {
      return paths;
    }
    final Collection<String> normalized = new ArrayList<>(paths.size());
    for (String dir : paths) {
      if (isReservedName(dir)) {
        LOG.error("{} ignoring reserved path {}", errorString, dir);
      } else {
        final Path path = new Path(dir);
        if (!path.isAbsolute()) {
          LOG.error("{} ignoring relative path {}", errorString, dir);
        } else if (path.toUri().getScheme() != null) {
          LOG.error("{} ignoring path {} with scheme", errorString, dir);
        } else {
          normalized.add(path.toString());
        }
      }
    }
    return normalized;
  }

  static String normalizePath(String src) {
    if (src.length() > 1 && src.endsWith("/")) {
      src = src.substring(0, src.length() - 1);
    }
    return src;
  }

  @VisibleForTesting
  public long getYieldCount() {
    return yieldCount;
  }

  void addYieldCount(long value) {
    yieldCount += value;
  }

  public INodeMap getINodeMap() {
    return inodeMap;
  }

  /**
   * This method is always called with writeLock of FSDirectory held.
   */
  public final void addToInodeMap(INode inode) {
    if (inode instanceof INodeWithAdditionalFields) {
      inodeMap.put(inode);
      if (!inode.isSymlink()) {
        final XAttrFeature xaf = inode.getXAttrFeature();
        addEncryptionZone((INodeWithAdditionalFields) inode, xaf);
        StoragePolicySatisfyManager spsManager =
            namesystem.getBlockManager().getSPSManager();
        if (spsManager != null && spsManager.isEnabled()) {
          addStoragePolicySatisfier((INodeWithAdditionalFields) inode, xaf);
        }
      }
    }
  }

  private void addStoragePolicySatisfier(INodeWithAdditionalFields inode,
      XAttrFeature xaf) {
    if (xaf == null) {
      return;
    }
    XAttr xattr = xaf.getXAttr(XATTR_SATISFY_STORAGE_POLICY);
    if (xattr == null) {
      return;
    }
    FSDirSatisfyStoragePolicyOp.unprotectedSatisfyStoragePolicy(inode, this);
  }

  private void addEncryptionZone(INodeWithAdditionalFields inode,
      XAttrFeature xaf) {
    if (xaf == null) {
      return;
    }
    XAttr xattr = xaf.getXAttr(CRYPTO_XATTR_ENCRYPTION_ZONE);
    if (xattr == null) {
      return;
    }
    try {
      final HdfsProtos.ZoneEncryptionInfoProto ezProto =
          HdfsProtos.ZoneEncryptionInfoProto.parseFrom(xattr.getValue());
      ezManager.unprotectedAddEncryptionZone(inode.getId(),
          PBHelperClient.convert(ezProto.getSuite()),
          PBHelperClient.convert(ezProto.getCryptoProtocolVersion()),
          ezProto.getKeyName());
      if (ezProto.hasReencryptionProto()) {
        final ReencryptionInfoProto reProto = ezProto.getReencryptionProto();
        // inodes parents may not be loaded if this is done during fsimage
        // loading so cannot set full path now. Pass in null to indicate that.
        ezManager.getReencryptionStatus()
            .updateZoneStatus(inode.getId(), null, reProto);
      }
    } catch (InvalidProtocolBufferException e) {
      NameNode.LOG.warn("Error parsing protocol buffer of " +
          "EZ XAttr " + xattr.getName() + " dir:" + inode.getFullPathName());
    }
  }
  
  /**
   * This is to handle encryption zone for rootDir when loading from
   * fsimage, and should only be called during NN restart.
   */
  public final void addRootDirToEncryptionZone(XAttrFeature xaf) {
    addEncryptionZone(rootDir, xaf);
  }

  /**
   * This method is always called with writeLock of FSDirectory held.
   */
  public final void removeFromInodeMap(List<? extends INode> inodes) {
    if (inodes != null) {
      for (INode inode : inodes) {
        if (inode != null && inode instanceof INodeWithAdditionalFields) {
          inodeMap.remove(inode);
          ezManager.removeEncryptionZone(inode.getId());
        }
      }
    }
  }
  
  /**
   * Get the inode from inodeMap based on its inode id.
   * @param id The given id
   * @return The inode associated with the given id
   */
  public INode getInode(long id) {
    return inodeMap.get(id);
  }
  
  @VisibleForTesting
  int getInodeMapSize() {
    return inodeMap.size();
  }

  long totalInodes() {
    return getInodeMapSize();
  }

  /**
   * Reset the entire namespace tree.
   */
  void reset() {
    writeLock();
    try {
      rootDir = createRoot(getFSNamesystem());
      inodeMap.clear();
      addToInodeMap(rootDir);
      nameCache.reset();
      inodeId.setCurrentValue(INodeId.LAST_RESERVED_ID);
    } finally {
      writeUnlock();
    }
  }

  static INode resolveLastINode(INodesInPath iip) throws FileNotFoundException {
    INode inode = iip.getLastINode();
    if (inode == null) {
      throw new FileNotFoundException("cannot find " + iip.getPath());
    }
    return inode;
  }

  /**
   * Caches frequently used file names to reuse file name objects and
   * reduce heap size.
   */
  void cacheName(INode inode) {
    // Name is cached only for files
    if (!inode.isFile()) {
      return;
    }
    ByteArray name = new ByteArray(inode.getLocalNameBytes());
    name = nameCache.put(name);
    if (name != null) {
      inode.setLocalName(name.getBytes());
    }
  }
  
  void shutdown() {
    nameCache.reset();
    inodeMap.clear();
  }
  
  /**
   * Given an INode get all the path complents leading to it from the root.
   * If an Inode corresponding to C is given in /A/B/C, the returned
   * patch components will be {root, A, B, C}.
   * Note that this method cannot handle scenarios where the inode is in a
   * snapshot.
   */
  public static byte[][] getPathComponents(INode inode) {
    List<byte[]> components = new ArrayList<byte[]>();
    components.add(0, inode.getLocalNameBytes());
    while(inode.getParent() != null) {
      components.add(0, inode.getParent().getLocalNameBytes());
      inode = inode.getParent();
    }
    return components.toArray(new byte[components.size()][]);
  }

  /** Check if a given inode name is reserved */
  public static boolean isReservedName(INode inode) {
    return CHECK_RESERVED_FILE_NAMES
            && Arrays.equals(inode.getLocalNameBytes(), DOT_RESERVED);
  }

  /** Check if a given path is reserved */
  public static boolean isReservedName(String src) {
    return src.startsWith(DOT_RESERVED_PATH_PREFIX + Path.SEPARATOR);
  }

  public static boolean isExactReservedName(String src) {
    return CHECK_RESERVED_FILE_NAMES && src.equals(DOT_RESERVED_PATH_PREFIX);
  }

  public static boolean isExactReservedName(byte[][] components) {
    return CHECK_RESERVED_FILE_NAMES &&
           (components.length == 2) &&
           isReservedName(components);
  }

  static boolean isReservedRawName(String src) {
    return src.startsWith(DOT_RESERVED_PATH_PREFIX +
        Path.SEPARATOR + RAW_STRING);
  }

  static boolean isReservedInodesName(String src) {
    return src.startsWith(DOT_RESERVED_PATH_PREFIX +
        Path.SEPARATOR + DOT_INODES_STRING);
  }

  static boolean isReservedName(byte[][] components) {
    return (components.length > 1) &&
            Arrays.equals(INodeDirectory.ROOT_NAME, components[0]) &&
            Arrays.equals(DOT_RESERVED, components[1]);
  }

  static boolean isReservedRawName(byte[][] components) {
    return (components.length > 2) &&
           isReservedName(components) &&
           Arrays.equals(RAW, components[2]);
  }

  /**
   * Resolve a /.reserved/... path to a non-reserved path.
   * <p/>
   * There are two special hierarchies under /.reserved/:
   * <p/>
   * /.reserved/.inodes/<inodeid> performs a path lookup by inodeid,
   * <p/>
   * /.reserved/raw/... returns the encrypted (raw) bytes of a file in an
   * encryption zone. For instance, if /ezone is an encryption zone, then
   * /ezone/a refers to the decrypted file and /.reserved/raw/ezone/a refers to
   * the encrypted (raw) bytes of /ezone/a.
   * <p/>
   * Pathnames in the /.reserved/raw directory that resolve to files not in an
   * encryption zone are equivalent to the corresponding non-raw path. Hence,
   * if /a/b/c refers to a file that is not in an encryption zone, then
   * /.reserved/raw/a/b/c is equivalent (they both refer to the same
   * unencrypted file).
   * 
   * @param pathComponents to be resolved
   * @param fsd FSDirectory
   * @return if the path indicates an inode, return path after replacing up to
   *         <inodeid> with the corresponding path of the inode, else the path
   *         in {@code pathComponents} as is. If the path refers to a path in
   *         the "raw" directory, return the non-raw pathname.
   * @throws FileNotFoundException if inodeid is invalid
   */
  //pathComponents：以字节数组的形式表示的路径组件，每个组件是路径的一个部分。例如路径 /a/b/c 对应的 pathComponents 是 {{'a'}, {'b'}, {'c'}}
  //byte[][]：解析后的路径组件数组，可能是原路径、替换后的实际路径，或者去除 /.reserved/raw 前缀的路径
  static byte[][] resolveComponents(byte[][] pathComponents,
      FSDirectory fsd) throws FileNotFoundException {
    final int nComponents = pathComponents.length;
    //如果路径不是 /.reserved/ 开头的，直接返回原路径
    if (nComponents < 3 || !isReservedName(pathComponents)) {
      /* This is not a /.reserved/ path so do nothing. */
    } else if (Arrays.equals(DOT_INODES, pathComponents[2])) {//解析路径中的 inodeid，将其转换为实际的路径
      /* It's a /.reserved/.inodes path. */
      if (nComponents > 3) {
        pathComponents = resolveDotInodesPath(pathComponents, fsd);
      }
    } else if (Arrays.equals(RAW, pathComponents[2])) {//如果路径指向加密区域，返回原始加密数据路径
      /* It's /.reserved/raw so strip off the /.reserved/raw prefix. */
      if (nComponents == 3) {
        pathComponents = new byte[][]{INodeDirectory.ROOT_NAME};
      } else {
        if (nComponents == 4
            && Arrays.equals(DOT_RESERVED, pathComponents[3])) {
          /* It's /.reserved/raw/.reserved so don't strip */
        } else {
          pathComponents = constructRemainingPath(
              new byte[][]{INodeDirectory.ROOT_NAME}, pathComponents, 3);
        }
      }
    }
    return pathComponents;
  }
  //pathComponents：路径的字节数组形式，表示解析的 /.reserved/.inodes 特殊路径
  //byte[][]：解析后的路径，仍以字节数组的形式返回，表示 HDFS 中的实际路径
  //法解析 /.reserved/.inodes/<inodeId> 路径，将 inodeId 转换为 HDFS 中的实际路径，并返回路径的字节数组形式
  private static byte[][] resolveDotInodesPath(
      byte[][] pathComponents, FSDirectory fsd)
      throws FileNotFoundException {
    //提取路径中的 inodeId 并解析为 long 类型
    final String inodeId = DFSUtil.bytes2String(pathComponents[3]);
    final long id;
    try {
      id = Long.parseLong(inodeId);
    } catch (NumberFormatException e) {
      throw new FileNotFoundException("Invalid inode path: " +
          DFSUtil.byteArray2PathString(pathComponents));
    }
    //如果路径是 /.reserved/.inodes/16384，并且该 inodeId 对应的是 HDFS 根目录，则直接返回根路径 /
    if (id == INodeId.ROOT_INODE_ID && pathComponents.length == 4) {
      return new byte[][]{INodeDirectory.ROOT_NAME};
    }
    //通过 inodeId 查找 inode
    INode inode = fsd.getInode(id);
    if (inode == null) {
      throw new FileNotFoundException(
          "File for given inode path does not exist: " +
              DFSUtil.byteArray2PathString(pathComponents));
    }

    // Handle single ".." for NFS lookup support.
    //处理 ".." 父目录解析
    if ((pathComponents.length > 4)
        && Arrays.equals(pathComponents[4], DOT_DOT)) {
      INode parent = inode.getParent();
      if (parent == null || parent.getId() == INodeId.ROOT_INODE_ID) {
        // inode is root, or its parent is root.
        return new byte[][]{INodeDirectory.ROOT_NAME};
      }
      return parent.getPathComponents();
    }
    //如果路径中有额外的子路径，保留 inode 解析的路径，并追加子路径
    return constructRemainingPath(
        inode.getPathComponents(), pathComponents, 4);
  }
  //用于将 已解析路径 (components) 与 剩余路径 (extraComponents 从 startAt 开始) 拼接，生成完整的路径
  private static byte[][] constructRemainingPath(byte[][] components,
      byte[][] extraComponents, int startAt) {
    int remainder = extraComponents.length - startAt;
    if (remainder > 0) {
      // grow the array and copy in the remaining components
      int pos = components.length;
      components = Arrays.copyOf(components, pos + remainder);
      System.arraycopy(extraComponents, startAt, components, pos, remainder);
    }
    if (NameNode.LOG.isDebugEnabled()) {
      NameNode.LOG.debug(
          "Resolved path is " + DFSUtil.byteArray2PathString(components));
    }
    return components;
  }

  INode getINode4DotSnapshot(INodesInPath iip) throws UnresolvedLinkException {
    Preconditions.checkArgument(
        iip.isDotSnapshotDir(), "%s does not end with %s",
        iip.getPath(), HdfsConstants.SEPARATOR_DOT_SNAPSHOT_DIR);

    final INode node = iip.getINode(-2);
    if (node != null && node.isDirectory()
        && node.asDirectory().isSnapshottable()) {
      return node;
    }
    return null;
  }

  /**
   * Resolves the given path into inodes.  Reserved paths are not handled and
   * permissions are not verified.  Client supplied paths should be
   * resolved via {@link #resolvePath(FSPermissionChecker, String, DirOp)}.
   * This method should only be used by internal methods.
   * @return the {@link INodesInPath} containing all inodes in the path.
   * @throws UnresolvedLinkException
   * @throws ParentNotDirectoryException
   * @throws AccessControlException
   */
  public INodesInPath getINodesInPath(String src, DirOp dirOp)
      throws UnresolvedLinkException, AccessControlException,
      ParentNotDirectoryException {
    return getINodesInPath(INode.getPathComponents(src), dirOp);
  }

  public INodesInPath getINodesInPath(byte[][] components, DirOp dirOp)
      throws UnresolvedLinkException, AccessControlException,
      ParentNotDirectoryException {
    INodesInPath iip = INodesInPath.resolve(rootDir, components);
    checkTraverse(null, iip, dirOp);
    return iip;
  }

  /**
   * Get {@link INode} associated with the file / directory.
   * See {@link #getINode(String, DirOp)}
   */
  @VisibleForTesting // should be removed after a lot of tests are updated
  public INode getINode(String src) throws UnresolvedLinkException,
      AccessControlException, ParentNotDirectoryException {
    return getINode(src, DirOp.READ);
  }

  /**
   * Get {@link INode} associated with the file / directory.
   * See {@link #getINode(String, DirOp)}
   */
  @VisibleForTesting // should be removed after a lot of tests are updated
  public INode getINode4Write(String src) throws UnresolvedLinkException,
      AccessControlException, FileNotFoundException,
      ParentNotDirectoryException {
    return getINode(src, DirOp.WRITE);
  }

  /**
   * Get {@link INode} associated with the file / directory.
   */
  public INode getINode(String src, DirOp dirOp) throws UnresolvedLinkException,
      AccessControlException, ParentNotDirectoryException {
    return getINodesInPath(src, dirOp).getLastINode();
  }

  FSPermissionChecker getPermissionChecker()
    throws AccessControlException {
    try {
      return getPermissionChecker(fsOwnerShortUserName, supergroup,
          NameNode.getRemoteUser());
    } catch (IOException e) {
      throw new AccessControlException(e);
    }
  }

  @VisibleForTesting
  FSPermissionChecker getPermissionChecker(String fsOwner, String superGroup,
      UserGroupInformation ugi) throws AccessControlException {
    return new FSPermissionChecker(
        fsOwner, superGroup, ugi, getUserFilteredAttributeProvider(ugi),
        useAuthorizationWithContextAPI,
        accessControlEnforcerReportingThresholdMs);
  }

  void checkOwner(FSPermissionChecker pc, INodesInPath iip)
      throws AccessControlException, FileNotFoundException {
    if (iip.getLastINode() == null) {
      throw new FileNotFoundException(
          "Directory/File does not exist " + iip.getPath());
    }
    checkPermission(pc, iip, true, null, null, null, null);
  }

  void checkPathAccess(FSPermissionChecker pc, INodesInPath iip,
      FsAction access) throws AccessControlException {
    checkPermission(pc, iip, false, null, null, access, null);
  }
  void checkParentAccess(FSPermissionChecker pc, INodesInPath iip,
      FsAction access) throws AccessControlException {
    checkPermission(pc, iip, false, null, access, null, null);
  }

  void checkAncestorAccess(FSPermissionChecker pc, INodesInPath iip,
      FsAction access) throws AccessControlException {
    checkPermission(pc, iip, false, access, null, null, null);
  }
  //检查路径的访问权限
  void checkTraverse(FSPermissionChecker pc, INodesInPath iip,
      boolean resolveLink) throws AccessControlException,
        UnresolvedPathException, ParentNotDirectoryException {
    FSPermissionChecker.checkTraverse(
        isPermissionEnabled ? pc : null, iip, resolveLink);
  }
  //pc：FSPermissionChecker 对象，负责检查权限
  //iip：INodesInPath 对象，表示路径中的 INode 列表
  //dirOp：DirOp 枚举，表示目录操作类型（如读取、写入、创建等）
  //检查用户是否有权限 访问或修改特定路径
  void checkTraverse(FSPermissionChecker pc, INodesInPath iip,
      DirOp dirOp) throws AccessControlException, UnresolvedPathException,
          ParentNotDirectoryException {
    final boolean resolveLink;
    //如果操作是以下三种，不解析符号链接 (resolveLink = false)
    switch (dirOp) {
      case READ_LINK://读取符号链接路径
      case WRITE_LINK://修改符号链接
      case CREATE_LINK://创建符号链接
        resolveLink = false;
        break;
      default:
        resolveLink = true;
        break;
    }
    checkTraverse(pc, iip, resolveLink);
    //只有 读取操作 (READ、READ_LINK) 允许访问快照路径
    boolean allowSnapshot = (dirOp == DirOp.READ || dirOp == DirOp.READ_LINK);
    if (!allowSnapshot && iip.isSnapshot()) {
      throw new SnapshotAccessControlException(
          "Modification on a read-only snapshot is disallowed");
    }
  }

  /**
   * Check whether current user have permissions to access the path. For more
   * details of the parameters, see
   * {@link FSPermissionChecker#checkPermission}.
   */
  void checkPermission(FSPermissionChecker pc, INodesInPath iip,
      boolean doCheckOwner, FsAction ancestorAccess, FsAction parentAccess,
      FsAction access, FsAction subAccess)
    throws AccessControlException {
    checkPermission(pc, iip, doCheckOwner, ancestorAccess,
        parentAccess, access, subAccess, false);
  }

  /**
   * Check whether current user have permissions to access the path. For more
   * details of the parameters, see
   * {@link FSPermissionChecker#checkPermission}.
   */
  void checkPermission(FSPermissionChecker pc, INodesInPath iip,
      boolean doCheckOwner, FsAction ancestorAccess, FsAction parentAccess,
      FsAction access, FsAction subAccess, boolean ignoreEmptyDir)
      throws AccessControlException {
    if (pc.isSuperUser()) {
      // call the external enforcer for audit
      pc.checkSuperuserPrivilege(iip.getPath());
    } else {
      readLock();
      try {
        pc.checkPermission(iip, doCheckOwner, ancestorAccess,
            parentAccess, access, subAccess, ignoreEmptyDir);
      } finally {
        readUnlock();
      }
    }
  }

  void checkUnreadableBySuperuser(FSPermissionChecker pc, INodesInPath iip)
      throws IOException {
    if (pc.isSuperUser()) {
      if (FSDirXAttrOp.getXAttrByPrefixedName(this, iip,
          SECURITY_XATTR_UNREADABLE_BY_SUPERUSER) != null) {
        String errorMessage = "Access is denied for " + pc.getUser()
            + " since the superuser is not allowed to perform this operation.";
        pc.denyUserAccess(iip.getPath(), errorMessage);
      } else {
        // call the external enforcer for audit.
        pc.checkSuperuserPrivilege(iip.getPath());
      }
    }
  }

  FileStatus getAuditFileInfo(INodesInPath iip)
      throws IOException {
    if (!namesystem.isAuditEnabled() || !namesystem.isExternalInvocation()) {
      return null;
    }

    final INode inode = iip.getLastINode();
    if (inode == null) {
      return null;
    }
    final int snapshot = iip.getPathSnapshotId();

    Path symlink = null;
    long size = 0;     // length is zero for directories
    short replication = 0;
    long blocksize = 0;

    if (inode.isFile()) {
      final INodeFile fileNode = inode.asFile();
      size = fileNode.computeFileSize(snapshot);
      replication = fileNode.getFileReplication(snapshot);
      blocksize = fileNode.getPreferredBlockSize();
    } else if (inode.isSymlink()) {
      symlink = new Path(
          DFSUtilClient.bytes2String(inode.asSymlink().getSymlink()));
    }

    return new FileStatus(
        size,
        inode.isDirectory(),
        replication,
        blocksize,
        inode.getModificationTime(snapshot),
        inode.getAccessTime(snapshot),
        inode.getFsPermission(snapshot),
        inode.getUserName(snapshot),
        inode.getGroupName(snapshot),
        symlink,
        new Path(iip.getPath()));
  }

  /**
   * Verify that parent directory of src exists.
   */
  void verifyParentDir(INodesInPath iip)
      throws FileNotFoundException, ParentNotDirectoryException {
    if (iip.length() > 2) {
      final INode parentNode = iip.getINode(-2);
      if (parentNode == null) {
        throw new FileNotFoundException("Parent directory doesn't exist: "
            + iip.getParentPath());
      } else if (!parentNode.isDirectory()) {
        throw new ParentNotDirectoryException("Parent path is not a directory: "
            + iip.getParentPath());
      }
    }
  }

  /** Allocate a new inode ID. */
  long allocateNewInodeId() {
    return inodeId.nextValue();
  }

  /** @return the last inode ID. */
  public long getLastInodeId() {
    return inodeId.getCurrentValue();
  }

  /**
   * Set the last allocated inode id when fsimage or editlog is loaded.
   */
  void resetLastInodeId(long newValue) throws IOException {
    try {
      inodeId.skipTo(newValue);
    } catch(IllegalStateException ise) {
      throw new IOException(ise);
    }
  }

  /** Should only be used for tests to reset to any value */
  void resetLastInodeIdWithoutChecking(long newValue) {
    inodeId.setCurrentValue(newValue);
  }

  INodeAttributes getAttributes(INodesInPath iip)
      throws IOException {
    INode node = FSDirectory.resolveLastINode(iip);
    int snapshot = iip.getPathSnapshotId();
    INodeAttributes nodeAttrs = node.getSnapshotINode(snapshot);
    UserGroupInformation ugi = NameNode.getRemoteUser();
    INodeAttributeProvider ap = this.getUserFilteredAttributeProvider(ugi);
    if (ap != null) {
      // permission checking sends the full components array including the
      // first empty component for the root.  however file status
      // related calls are expected to strip out the root component according
      // to TestINodeAttributeProvider.
      byte[][] components = iip.getPathComponents();
      components = Arrays.copyOfRange(components, 1, components.length);
      nodeAttrs = ap.getAttributes(components, nodeAttrs);
    }
    return nodeAttrs;
  }

}
