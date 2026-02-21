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
package org.apache.hadoop.hdfs.server.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.Validate;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.MetaRecoveryContext;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLayoutVersion;
import org.apache.hadoop.util.StringUtils;

/************************************
 * Some handy internal HDFS constants
 *
 ************************************/

@InterfaceAudience.Private
public interface HdfsServerConstants {
  // Will be set by
  // {@code DFSConfigKeys.DFS_NAMENODE_BLOCKPLACEMENTPOLICY_MIN_BLOCKS_FOR_WRITE_KEY}.
  int MIN_BLOCKS_FOR_WRITE = 1;

  long LEASE_RECOVER_PERIOD = 10 * 1000; // in ms
  // We need to limit the length and depth of a path in the filesystem.
  // HADOOP-438
  // Currently we set the maximum length to 8k characters and the maximum depth
  // to 1k.
  int MAX_PATH_LENGTH = 8000;
  int MAX_PATH_DEPTH = 1000;
  // An invalid transaction ID that will never be seen in a real namesystem.
  long INVALID_TXID = -12345;
  // Number of generation stamps reserved for legacy blocks.
  long RESERVED_LEGACY_GENERATION_STAMPS = 1024L * 1024 * 1024 * 1024;
  /**
   * Current layout version for NameNode.
   * Please see {@link NameNodeLayoutVersion.Feature} on adding new layout version.
   */
  int NAMENODE_LAYOUT_VERSION
      = NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION;
  /**
  * Current minimum compatible version for NameNode
  * Please see {@link NameNodeLayoutVersion.Feature} on adding new layout version.
  */
  int MINIMUM_COMPATIBLE_NAMENODE_LAYOUT_VERSION
      = NameNodeLayoutVersion.MINIMUM_COMPATIBLE_LAYOUT_VERSION;
  /**
   * Path components that are reserved in HDFS.
   * <p>
   * .reserved is only reserved under root ("/").
   */
  String[] RESERVED_PATH_COMPONENTS = new String[] {
      HdfsConstants.DOT_SNAPSHOT_DIR,
      FSDirectory.DOT_RESERVED_STRING
  };
  byte[] DOT_SNAPSHOT_DIR_BYTES
              = DFSUtil.string2Bytes(HdfsConstants.DOT_SNAPSHOT_DIR);

  /**
   * Type of the node
   */
  enum NodeType {
    NAME_NODE,  //主节点，负责管理文件系统的元数据。
    DATA_NODE,  //数据节点，负责存储实际的数据块。
    JOURNAL_NODE //日志节点，用于实现高可用性（HA）。
  }

  /** Startup options for rolling upgrade. */
  enum RollingUpgradeStartupOption{
    ROLLBACK, //回滚模式，用于将 HDFS 从升级中回退到之前的版本。
    STARTED; //启动滚动升级模式，允许集群在运行时进行升级。

    public String getOptionString() {
      return StartupOption.ROLLINGUPGRADE.getName() + " "
          + StringUtils.toLowerCase(name());
    }

    public boolean matches(StartupOption option) {
      return option == StartupOption.ROLLINGUPGRADE
          && option.getRollingUpgradeStartupOption() == this;
    }

    private static final RollingUpgradeStartupOption[] VALUES = values();

    static RollingUpgradeStartupOption fromString(String s) {
      if ("downgrade".equalsIgnoreCase(s)) {
        throw new IllegalArgumentException(
            "The \"downgrade\" option is no longer supported"
                + " since it may incorrectly finalize an ongoing rolling upgrade."
                + " For downgrade instruction, please see the documentation"
                + " (http://hadoop.apache.org/docs/current/hadoop-project-dist/"
                + "hadoop-hdfs/HdfsRollingUpgrade.html#Downgrade).");
      }
      for(RollingUpgradeStartupOption opt : VALUES) {
        if (opt.name().equalsIgnoreCase(s)) {
          return opt;
        }
      }
      throw new IllegalArgumentException("Failed to convert \"" + s
          + "\" to " + RollingUpgradeStartupOption.class.getSimpleName());
    }

    public static String getAllOptionString() {
      final StringBuilder b = new StringBuilder("<");
      for(RollingUpgradeStartupOption opt : VALUES) {
        b.append(StringUtils.toLowerCase(opt.name())).append("|");
      }
      b.setCharAt(b.length() - 1, '>');
      return b.toString();
    }
  }

  /** Startup options */
  enum StartupOption{
    FORMAT  ("-format"), //对 NameNode 进行格式化，删除已有的元数据并重新初始化
    CLUSTERID ("-clusterid"), //指定新的 clusterId，与 -format 一起使用，设置 HDFS 集群的唯一标识符
    GENCLUSTERID ("-genclusterid"), //自动生成新的 clusterId，多用于自动化部署场景
    REGULAR ("-regular"), //默认正常启动 NameNode，执行常规操作。
    BACKUP  ("-backup"), //启动为 BackupNode，用于保存主节点的镜像和编辑日志的备份。
    CHECKPOINT("-checkpoint"), //启动为 CheckpointNode，定期将编辑日志合并为新的镜像，减小日志体积。
    UPGRADE ("-upgrade"), //进行 HDFS 版本升级，执行升级过程中的必要操作
    ROLLBACK("-rollback"), //回滚到上一个 HDFS 版本，撤销上次升级。
    ROLLINGUPGRADE("-rollingUpgrade"), //进行滚动升级，确保升级过程不中断服务，支持零停机升级。
    IMPORT  ("-importCheckpoint"), //从 CheckpointNode 导入检查点，恢复或更新元数据。
    BOOTSTRAPSTANDBY("-bootstrapStandby"),//为 Standby NameNode 引导同步元数据。
    INITIALIZESHAREDEDITS("-initializeSharedEdits"), //初始化共享编辑日志目录，支持 HA 架构。
    RECOVER  ("-recover"), //执行故障恢复模式，尝试修复损坏的元数据。
    FORCE("-force"),//强制执行某些操作，通常与 -format、-recover 结合使用，跳过用户确认。
    NONINTERACTIVE("-nonInteractive"), //以非交互模式执行，适用于自动化脚本，避免用户输入干预。
    SKIPSHAREDEDITSCHECK("-skipSharedEditsCheck"), //跳过共享编辑日志的检查，减少检查时间。
    RENAMERESERVED("-renameReserved"), //处理保留名称的重命名操作，解决兼容性问题。
    METADATAVERSION("-metadataVersion"), //显示当前 HDFS 的元数据版本，供调试和升级检查使用。
    UPGRADEONLY("-upgradeOnly"), //只执行升级，不启动 HDFS 服务，通常用于手动升级场景
    // The -hotswap constant should not be used as a startup option, it is
    // only used for StorageDirectory.analyzeStorage() in hot swap drive scenario.
    // TODO refactor StorageDirectory.analyzeStorage() so that we can do away with
    // this in StartupOption.
    HOTSWAP("-hotswap"),
    // Startup the namenode in observer mode.
    OBSERVER("-observer"); //	以 Observer 模式启动，提供元数据的只读视图，减轻主节点负载。

    private static final Pattern ENUM_WITH_ROLLING_UPGRADE_OPTION = Pattern.compile(
        "(\\w+)\\((\\w+)\\)");

    private final String name;
    
    // Used only with format and upgrade options
    private String clusterId = null;
    
    // Used only by rolling upgrade
    private RollingUpgradeStartupOption rollingUpgradeStartupOption;

    // Used only with format option
    private boolean isForceFormat = false;
    private boolean isInteractiveFormat = true;
    
    // Used only with recovery option
    private int force = 0;

    StartupOption(String arg) {this.name = arg;} //构造函数，初始化 name 属性，表示该枚举对应的启动参数
    public String getName() {return name;}  //获取枚举对应的启动参数名称（如 -format、-upgrade 等）。
    public NamenodeRole toNodeRole() {    //根据启动选项，返回相应的 NameNode 角色：
      switch(this) {
      case BACKUP: 
        return NamenodeRole.BACKUP;
      case CHECKPOINT: 
        return NamenodeRole.CHECKPOINT;
      default:
        return NamenodeRole.NAMENODE;
      }
    }
    //设置和获取 HDFS 集群的唯一标识符（clusterId）
    public void setClusterId(String cid) {
      clusterId = cid;
    }

    public String getClusterId() {
      return clusterId;
    }
    //仅在 ROLLINGUPGRADE 模式下，设置滚动升级的具体子选项
    public void setRollingUpgradeStartupOption(String opt) {
      Preconditions.checkState(this == ROLLINGUPGRADE);
      rollingUpgradeStartupOption = RollingUpgradeStartupOption.fromString(opt);
    }
    
    public RollingUpgradeStartupOption getRollingUpgradeStartupOption() {
      Preconditions.checkState(this == ROLLINGUPGRADE);
      return rollingUpgradeStartupOption;
    }
    //仅在 RECOVER 模式下，创建 MetaRecoveryContext 对象，执行元数据恢复操作。
    public MetaRecoveryContext createRecoveryContext() {
      if (!name.equals(RECOVER.name))
        return null;
      return new MetaRecoveryContext(force);
    }
    //控制是否强制执行某些操作（如强制格式化或恢复）
    public void setForce(int force) {
      this.force = force;
    }
    
    public int getForce() {
      return this.force;
    }
    //标识是否强制格式化 HDFS，true 强制执行，false 正常执行
    public boolean getForceFormat() {
      return isForceFormat;
    }
    
    public void setForceFormat(boolean force) {
      isForceFormat = force;
    }
    //控制格式化是否需要用户确认
    public boolean getInteractiveFormat() {
      return isInteractiveFormat;
    }
    
    public void setInteractiveFormat(boolean interactive) {
      isInteractiveFormat = interactive;
    }
    
    @Override
    public String toString() {
      if (this == ROLLINGUPGRADE) {
        return new StringBuilder(super.toString())
            .append("(").append(getRollingUpgradeStartupOption()).append(")")
            .toString();
      }
      return super.toString();
    }
    //根据传入的字符串解析对应的 StartupOption，支持解析带 rollingUpgrade 子选项的复杂字符串。
    static public StartupOption getEnum(String value) {
      Matcher matcher = ENUM_WITH_ROLLING_UPGRADE_OPTION.matcher(value);
      if (matcher.matches()) {
        StartupOption option = StartupOption.valueOf(matcher.group(1));
        option.setRollingUpgradeStartupOption(matcher.group(2));
        return option;
      } else {
        return StartupOption.valueOf(value);
      }
    }
  }

  /**
   * Defines the NameNode role.
   */
  enum NamenodeRole {
    NAMENODE  ("NameNode"),  //主节点，负责管理 HDFS 文件系统的命名空间，处理客户端请求，维护元数据。
    BACKUP    ("Backup Node"), //备份节点，保存主节点的元数据镜像和编辑日志的实时备份。
    CHECKPOINT("Checkpoint Node"); //检查点节点，定期将 fsimage 和 edits 合并，减小编辑日志体积。

    private String description = null;
    NamenodeRole(String arg) {this.description = arg;}
  
    @Override
    public String toString() {
      return description;
    }
  }

  /**
   * Block replica states, which it can go through while being constructed.
   */
  // HDFS 数据块副本（Replica） 在构建过程中可能经历的不同状态。
  // 这些状态用于跟踪 HDFS 中每个数据块副本的生命周期，帮助 NameNode 和 DataNode 进行副本的管理、恢复和复制操作
  enum ReplicaState {
    /** Replica is finalized. The state when replica is not modified. */
    FINALIZED(0), //副本已完成（最终状态，不能再被修改）
    /** Replica is being written to. */
    RBW(1), // 副本正在写入（Replica Being Written），数据块正在写入过程中，尚未完成。DataNode 在此状态下持续接收数据。
    /** Replica is waiting to be recovered. */
    RWR(2), // 副本等待恢复（Replica Waiting for Recovery），数据块需要恢复，例如 DataNode 宕机或写入中断，等待恢复过程开始
    /** Replica is under recovery. */
    RUR(3), // 副本正在恢复（Replica Under Recovery），数据块正在恢复过程中，通常涉及多个 DataNode 进行数据的同步与修复。
    /** Temporary replica: created for replication and relocation only. */
    TEMPORARY(4);// 临时副本（仅用于复制和迁移），通常是为了副本复制、数据迁移等目的而创建的临时状态

    // Since ReplicaState (de)serialization depends on ordinal, either adding
    // new value should be avoided to this enum or newly appended value should
    // be handled by NameNodeLayoutVersion#Feature.

    private static final ReplicaState[] cachedValues = ReplicaState.values();

    private final int value; //为每个枚举常量分配一个整型值，便于序列化和反序列化操作

    ReplicaState(int v) {
      value = v;
    }

    public int getValue() {
      return value;
    }

    /**
     * Retrieve ReplicaState corresponding to given index.
     *
     * @param v Index to retrieve {@link ReplicaState}.
     * @return {@link ReplicaState} object.
     * @throws IndexOutOfBoundsException if the index is invalid.
     */
    public static ReplicaState getState(int v) { //v：索引值，表示副本状态的枚举值。
      Validate.validIndex(cachedValues, v, "Index Expected range: [0, "
          + (cachedValues.length - 1) + "]. Actual value: " + v);
      return cachedValues[v];
    }

    /**
     * Retrieve ReplicaState corresponding to index provided in binary stream.
     *
     * @param in Index value provided as bytes in given binary stream.
     * @return {@link ReplicaState} object.
     * @throws IOException if an I/O error occurs while reading bytes.
     * @throws IndexOutOfBoundsException if the index is invalid.
     *///in：输入流，提供副本状态的索引值（字节形式）。
    public static ReplicaState read(DataInput in) throws IOException {
      byte idx = in.readByte();
      Validate.validIndex(cachedValues, idx, "Index Expected range: [0, "
          + (cachedValues.length - 1) + "]. Actual value: " + idx);
      return cachedValues[idx];
    }

    /** Write to out */
    public void write(DataOutput out) throws IOException {
      out.writeByte(ordinal());
    }
  }

  /** 定义了 HDFS 中 处于构建过程中的数据块（Block） 所经历的不同状态。该类用于跟踪文件写入、追加或恢复过程中数据块的生命周期
   * States, which a block can go through while it is under construction.
   */
  enum BlockUCState {
    /**
     * Block construction completed.<br>
     * The block has at least the configured minimal replication number
     * of {@link ReplicaState#FINALIZED} replica(s), and is not going to be
     * modified.
     * NOTE, in some special cases, a block may be forced to COMPLETE state,
     * even if it doesn't have required minimal replications.
     */
    COMPLETE, //数据块已构建完成，且满足最小副本数要求，不能再被修改。
    /**
     * The block is under construction.<br>
     * It has been recently allocated for write or append.
     */
    UNDER_CONSTRUCTION, //数据块正在构建中，意味着数据块被新分配用于写入或追加操作。
    /**
     * The block is under recovery.<br>
     * When a file lease expires its last block may not be {@link #COMPLETE}
     * and needs to go through a recovery procedure, 
     * which synchronizes the existing replicas contents.
     */
    UNDER_RECOVERY, //数据块正在恢复中，通常发生在租约（lease）过期时，需要对数据副本进行同步和修复。
    /**
     * The block is committed.<br>
     * The client reported that all bytes are written to data-nodes
     * with the given generation stamp and block length, but no 
     * {@link ReplicaState#FINALIZED} 
     * replicas has yet been reported by data-nodes themselves.
     */
    COMMITTED //数据块已提交，客户端确认数据写入完成，但尚未收到 DataNode 报告的任何最终副本。
  }
  
  String NAMENODE_LEASE_HOLDER = "HDFS_NameNode";

  String CRYPTO_XATTR_ENCRYPTION_ZONE =
      "raw.hdfs.crypto.encryption.zone";
  String CRYPTO_XATTR_FILE_ENCRYPTION_INFO =
      "raw.hdfs.crypto.file.encryption.info";
  String SECURITY_XATTR_UNREADABLE_BY_SUPERUSER =
      "security.hdfs.unreadable.by.superuser";
  String XATTR_ERASURECODING_POLICY =
      "system.hdfs.erasurecoding.policy";
  String XATTR_SNAPSHOT_DELETED = "system.hdfs.snapshot.deleted";

  String XATTR_SATISFY_STORAGE_POLICY = "user.hdfs.sps";

  Path MOVER_ID_PATH = new Path("/system/mover.id");

  long BLOCK_GROUP_INDEX_MASK = 15;
  byte MAX_BLOCKS_IN_GROUP = 16;
  // maximum bandwidth per datanode 1TB/sec.
  long MAX_BANDWIDTH_PER_DATANODE = 1099511627776L;
}
