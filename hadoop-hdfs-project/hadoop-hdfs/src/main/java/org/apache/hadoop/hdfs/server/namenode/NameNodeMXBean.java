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

import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;

/**用于提供 Hadoop HDFS 中 NameNode 相关的信息。这个接口允许通过 JMX 获取集群的各种运行时信息和统计数据，供监控工具或者管理员使用
 * This is the JMX management interface for namenode information.
 * End users shouldn't be implementing these interfaces, and instead
 * access this information through the JMX APIs.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public interface NameNodeMXBean {

  /**
   * Gets the version of Hadoop.
   *  返回 Hadoop 的版本信息
   * @return the version.
   */
  String getVersion();

  /**
   * Get the version of software running on the Namenode.
   * 返回正在运行在 NameNode 上的软件版本
   * @return a string representing the version.
   */
  String getSoftwareVersion();

  /**
   * Gets the used space by data nodes.
   * 返回数据节点已使用的空间
   * @return the used space by data nodes.
   */
  long getUsed();
  
  /**
   * Gets total non-used raw bytes.
   *  返回未使用的原始字节数
   * @return total non-used raw bytes.
   */
  long getFree();
  
  /**
   * Gets total raw bytes including non-dfs used space.
   * 返回包括非 DFS 使用空间的总原始字节数
   * @return the total raw bytes including non-dfs used space.
   */
  long getTotal();

  /**
   * Gets capacity of the provided storage mounted, in bytes.
   * 返回提供的存储设备的总容量（以字节为单位）
   * @return the total raw bytes present in the provided storage.
   */
  long getProvidedCapacity();

  /**
   * Gets the safemode status.
   *  返回 NameNode 的安全模式状态
   * @return the safemode status.
   */
  String getSafemode();
  
  /**
   * Checks if upgrade is finalized.
   * 检查升级是否已完成
   * @return true, if upgrade is finalized.
   */
  boolean isUpgradeFinalized();

  /**
   * Gets the RollingUpgrade information.
   * 获取滚动升级的状态信息。如果正在进行升级，返回相关信息，否则返回 null
   * @return Rolling upgrade information if an upgrade is in progress. Else
   * (e.g. if there is no upgrade or the upgrade is finalized), returns null.
   */
  RollingUpgradeInfo.Bean getRollingUpgradeStatus();

  /**
   * Gets total used space by data nodes for non DFS purposes such as storing
   * temporary files on the local file system.
   *  获取数据节点用于非 DFS 目的（如存储临时文件）的已使用空间
   * @return the non dfs space of the cluster.
   */
  long getNonDfsUsedSpace();
  
  /**
   * Gets the total used space by data nodes as percentage of total capacity.
   *  返回数据节点已使用空间占总容量的百分比
   * @return the percentage of used space on the cluster.
   */
  float getPercentUsed();
  
  /**
   * Gets the total remaining space by data nodes as percentage of total 
   * capacity.
   *  返回数据节点剩余空间占总容量的百分比
   * @return the percentage of the remaining space on the cluster.
   */
  float getPercentRemaining();

  /**
   * Gets the amount of cache used by the datanode (in bytes).
   * 返回数据节点使用的缓存空间（以字节为单位）
   * @return the amount of cache used by the datanode (in bytes).
   */
  long getCacheUsed();

  /**
   * Gets the total cache capacity of the datanode (in bytes).
   *返回数据节点缓存的总容量（以字节为单位）
   * @return the total cache capacity of the datanode (in bytes).
   */
  long getCacheCapacity();
  
  /**
   * Get the total space used by the block pools of this namenode.
   * 获取 NameNode 的块池已使用的空间
   * @return the total space used by the block pools of this namenode.
   */
  long getBlockPoolUsedSpace();
  
  /**
   * Get the total space used by the block pool as percentage of total capacity.
   * 获取块池已使用的空间占总容量的百分比
   * @return the total space used by the block pool as percentage of total
   * capacity.
   */
  float getPercentBlockPoolUsed();
    
  /**
   * Gets the total numbers of blocks on the cluster.
   *  获取集群中的总块数
   * @return the total number of blocks of the cluster.
   */
  long getTotalBlocks();
  
  /**
   * Gets the total number of missing blocks on the cluster.
   *  获取集群中丢失的块的总数
   * @return the total number of missing blocks on the cluster.
   */
  long getNumberOfMissingBlocks();
  
  /**
   * Gets the total number of missing blocks on the cluster with
   * replication factor 1.
   * 获取复制因子为 1 丢失的块的数量
   * @return the total number of missing blocks on the cluster with
   * replication factor 1.
   */
  long getNumberOfMissingBlocksWithReplicationFactorOne();

  /**
   * Gets the total number of replicated low redundancy blocks on the cluster
   * with the highest risk of loss.
   * 获取复制冗余度低的块的总数，这些块有最高的丢失风险
   * @return the total number of low redundancy blocks on the cluster
   * with the highest risk of loss.
   */
  long getHighestPriorityLowRedundancyReplicatedBlocks();

  /**
   * Gets the total number of erasure coded low redundancy blocks on the cluster
   * with the highest risk of loss.
   * 获取复制冗余度低的 EC 块的总数，这些块有最高的丢失风险
   * @return the total number of low redundancy blocks on the cluster
   * with the highest risk of loss.
   */
  long getHighestPriorityLowRedundancyECBlocks();

  /**
   * Gets the total number of snapshottable dirs in the system.
   * 获取系统中可快照的目录的总数
   * @return the total number of snapshottable dirs in the system.
   */
  long getNumberOfSnapshottableDirs();

  /**
   * Gets the number of threads.
   * 获取 NameNode 线程的数量
   * @return the number of threads.
   */
  int getThreads();

  /**
   * Gets the live node information of the cluster.
   *  获取集群中存活节点的信息
   * @return the live node information.
   */
  String getLiveNodes();
  
  /**
   * Gets the dead node information of the cluster.
   * 获取集群中死节点的信息
   * @return the dead node information.
   */
  String getDeadNodes();
  
  /**
   * Gets the decommissioning node information of the cluster.
   *  获取正在退役的节点信息
   * @return the decommissioning node information.
   */
  String getDecomNodes();

  /**
   * Gets the information on nodes entering maintenance.
   *获取正在进入维护状态的节点信息
   * @return the information on nodes entering maintenance.
   */
  String getEnteringMaintenanceNodes();

  /**
   * Gets the cluster id.
   * 获取集群 ID
   * @return the cluster id.
   */
  String getClusterId();
  
  /**
   * Gets the block pool id.
   * 获取块池 ID
   * @return the block pool id.
   */
  String getBlockPoolId();

  /**
   * Get status information about the directories storing image and edits logs
   * of the NN.
   * 获取 NameNode 存储镜像和编辑日志的目录状态信息
   * @return the name dir status information, as a JSON string.
   */
  String getNameDirStatuses();

  /**
   * Get Max, Median, Min and Standard Deviation of DataNodes usage.
   *获取数据节点的使用情况（包括最大值、中位数、最小值和标准差）
   * @return the DataNode usage information, as a JSON string.
   */
  String getNodeUsage();

  /**
   * Get status information about the journals of the NN.
   *获取 NameNode 日志的状态信息
   * @return the name journal status information, as a JSON string.
   */
  String getNameJournalStatus();
  
  /**
   * Get information about the transaction ID, including the last applied 
   * transaction ID and the most recent checkpoint's transaction ID.
   * 获取事务 ID 的信息，包括最后应用的事务 ID 和最近检查点的事务 ID
   * @return information about the transaction ID.
   */
  String getJournalTransactionInfo();

  /**
   * Gets the NN start time in milliseconds.
   * 获取 NameNode 启动时间的毫秒数
   * @return the NN start time in msec.
   */
  long getNNStartedTimeInMillis();

  /**
   * Get the compilation information which contains date, user and branch.
   *取编译信息，包括日期、用户和分支
   * @return the compilation information, as a JSON string.
   */
  String getCompileInfo();

  /**
   * Get the list of corrupt files.
   * 获取损坏文件的列表
   * @return the list of corrupt files, as a JSON string.
   */
  String getCorruptFiles();

  /**
   * Get the length of the list of corrupt files.
   *获取损坏文件的数量
   * @return the length of the list of corrupt files.
   */
  int getCorruptFilesCount();

  /**
   * Get the number of distinct versions of live datanodes.
   * 获取活跃数据节点版本的不同数量
   * @return the number of distinct versions of live datanodes.
   */
  int getDistinctVersionCount();

  /**
   * Get the number of live datanodes for each distinct versions.
   * 获取每个版本的活跃数据节点数量
   * @return the number of live datanodes for each distinct versions.
   */
  Map<String, Integer> getDistinctVersions();
  
  /**
   * Get namenode directory size.
   *获取 NameNode 目录的大小
   * @return namenode directory size.
   */
  String getNameDirSize();

  /**
   * Verifies whether the cluster setup can support all enabled EC policies.
   *验证集群设置是否支持所有启用的 EC 策略
   * @return the result of the verification.
   */
  String getVerifyECWithTopologyResult();

}
