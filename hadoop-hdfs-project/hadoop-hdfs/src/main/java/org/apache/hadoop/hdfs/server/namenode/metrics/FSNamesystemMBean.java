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
package org.apache.hadoop.hdfs.server.namenode.metrics;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * 
 * This Interface defines the methods to get the status of a the FSNamesystem of
 * a name node.
 * It is also used for publishing via JMX (hence we follow the JMX naming
 * convention.)
 * 
 * Note we have not used the MetricsDynamicMBeanBase to implement this
 * because the interface for the NameNodeStateMBean is stable and should
 * be published as an interface.
 * 
 * <p>
 * Name Node runtime activity statistic  info is reported in
 * @see org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics
 *
 */
// 提供 HDFS 的 FSNamesystem 的监控信息，尤其是 NameNode 的状态。接口中的每个方法都提供了与 HDFS 系统运行时性能和健康相关的指标
@InterfaceAudience.Private
public interface FSNamesystemMBean {

  /** 获取文件系统的当前状态，返回的值是文件系统的状态（如 safemode 或 operational）。当 NameNode 处于 safemode 时，文件系统处于一种保护模式，不允许进行任何写操作
   * The state of the file system: Safemode or Operational
   * @return the state
   */
  public String getFSState();
  
  
  /** 返回系统中已分配的块的总数。一个块是 HDFS 存储数据的基本单位
   * Number of allocated blocks in the system
   * @return -  number of allocated blocks
   */
  public long getBlocksTotal();

  /** 返回文件系统的总存储容量，以字节为单位
   * Total storage capacity
   * @return -  total capacity in bytes
   */
  public long getCapacityTotal();


  /**返回系统中剩余的可用存储容量，以字节为单位
   * Free (unused) storage capacity
   * @return -  free capacity in bytes
   */
  public long getCapacityRemaining();
 
  /**返回系统中已用的存储容量，以字节为单位
   * Used storage capacity
   * @return -  used capacity in bytes
   */
  public long getCapacityUsed();

  /** 返回系统中提供的存储容量的总量（可能是某些特定配置或设备的存储容量），以字节为单位
   * Total PROVIDED storage capacity.
   * @return -  total PROVIDED storage capacity in bytes
   */
  public long getProvidedCapacityTotal();

  /** 返回系统中总的文件和目录数目
   * Total number of files and directories
   * @return -  num of files and directories
   */
  public long getFilesTotal();
 
  /** 返回所有等待复制的块的总数
   * Get aggregated count of all blocks pending to be reconstructed.
   * @deprecated Use {@link #getPendingReconstructionBlocks()} instead.
   */
  @Deprecated
  public long getPendingReplicationBlocks();

  /** 返回系统中所有等待重建的块的总数，这些块因为某些原因而失去冗余，需要重建
   * Get aggregated count of all blocks pending to be reconstructed.
   * @return Number of blocks to be replicated.
   */
  public long getPendingReconstructionBlocks();

  /**
   * Get aggregated count of all blocks with low redundancy.
   * @deprecated Use {@link #getLowRedundancyBlocks()} instead.
   */
  @Deprecated
  public long getUnderReplicatedBlocks();

  /** 返回所有冗余度低的块的数量，冗余度低的块意味着它们的副本数低于设定的阈值
   * Get aggregated count of all blocks with low redundancy.
   * @return Number of blocks with low redundancy.
   */
  public long getLowRedundancyBlocks();

  /** 返回已安排进行复制的块的数量
   * Blocks scheduled for replication
   * @return -  num of blocks scheduled for replication
   */
  public long getScheduledReplicationBlocks();

  /** 返回 FSNamesystem 的总负载。这个负载可以反映当前系统的压力或负载情况
   * Total Load on the FSNamesystem
   * @return -  total load of FSNamesystem
   */
  public int getTotalLoad();

  /** 返回系统中处于活跃状态的 DataNode 数量
   * Number of Live data nodes
   * @return number of live data nodes
   */
  public int getNumLiveDataNodes();
  
  /**返回系统中死亡（不可用）DataNode 的数量
   * Number of dead data nodes
   * @return number of dead data nodes
   */
  public int getNumDeadDataNodes();
  
  /** 返回系统中陈旧（不再更新）DataNode 的数量
   * Number of stale data nodes
   * @return number of stale data nodes
   */
  public int getNumStaleDataNodes();

  /**返回处于退役状态的活跃 DataNode 数量。DataNode 被标记为退役后不再提供服务
   * Number of decommissioned Live data nodes
   * @return number of decommissioned live data nodes
   */
  public int getNumDecomLiveDataNodes();

  /**返回处于退役状态的死亡 DataNode 数量
   * Number of decommissioned dead data nodes
   * @return number of decommissioned dead data nodes
   */
  public int getNumDecomDeadDataNodes();

  /**返回当前在服务中的活跃 DataNode 数量
   * @return Number of in-service data nodes, where NumInServiceDataNodes =
   * NumLiveDataNodes - NumDecomLiveDataNodes - NumInMaintenanceLiveDataNodes
   */
  int getNumInServiceLiveDataNodes();

  /**返回所有活跃 DataNode 中失败的存储卷的总数量
   * Number of failed data volumes across all live data nodes.
   * @return number of failed data volumes across all live data nodes
   */
  int getVolumeFailuresTotal();

  /**
   * Returns an estimate of total capacity lost due to volume failures in bytes
   * across all live data nodes.
   * @return estimate of total capacity lost in bytes
   */
  long getEstimatedCapacityLostTotal();

  /**返回处于退役过程中的 DataNode 数量
   * Number of data nodes that are in the decommissioning state
   */
  public int getNumDecommissioningDataNodes();

  /**返回系统中快照的统计信息
   * The statistics of snapshots
   */
  public String getSnapshotStats();

  /**返回文件系统中支持的最大 inode 数量。inode 是文件系统用来存储文件元数据的结构
   * Return the maximum number of inodes in the file system
   */
  public long getMaxObjects();

  /**返回等待删除的块的数量
   * Number of blocks pending deletion
   * @return number of blocks pending deletion
   */
  long getPendingDeletionBlocks();

  /**返回块删除操作开始的时间
   * Time when block deletions will begin
   * @return time when block deletions will begin
   */
  long getBlockDeletionStartTime();

  /**回系统中陈旧存储的数量，通常指那些不再使用但仍然存在的存储
   * Number of content stale storages.
   * @return number of content stale storages
   */
  public int getNumStaleStorages();

  /**
   * Returns a nested JSON object listing the top users for different RPC 
   * operations over tracked time windows.
   * 列出不同 RPC 操作在指定时间窗口内的顶级用户的计数
   * @return JSON string
   */
  public String getTopUserOpCounts();

  /**返回系统中加密区域的数量
   * Return the number of encryption zones in the system.
   */
  int getNumEncryptionZones();

  /**
   * Returns the length of the wait Queue for the FSNameSystemLock.
   *返回等待获取 FSNameSystemLock 的线程数。一个较大的数值表示许多线程在等待该锁
   * A larger number here indicates lots of threads are waiting for
   * FSNameSystemLock.
   * @return int - Number of Threads waiting to acquire FSNameSystemLock
   */
  int getFsLockQueueLength();

  /**返回 FSEditLog 上的同步操作总数
   * Return total number of Sync Operations on FSEditLog.
   */
  long getTotalSyncCount();

  /**
   * Return total time spent doing sync operations on FSEditLog.
   */
  String getTotalSyncTimes();

  /**
   * @return Number of IN_MAINTENANCE live data nodes
   */
  int getNumInMaintenanceLiveDataNodes();

  /**
   * @return Number of IN_MAINTENANCE dead data nodes
   */
  int getNumInMaintenanceDeadDataNodes();

  /**
   * @return Number of ENTERING_MAINTENANCE data nodes
   */
  int getNumEnteringMaintenanceDataNodes();

  /**
   * Get the current number of delegation tokens in memory.
   * @return number of DTs
   */
  long getCurrentTokensCount();

  /**
   * Returns the number of paths to be processed by storage policy satisfier.
   *
   * @return The number of paths to be processed by sps.
   */
  int getPendingSPSPaths();

  /**
   * Get the progress of the reconstruction queues initialisation.
   *
   * @return Returns values between 0 and 1 for the progress.
   */
  float getReconstructionQueuesInitProgress();
}
