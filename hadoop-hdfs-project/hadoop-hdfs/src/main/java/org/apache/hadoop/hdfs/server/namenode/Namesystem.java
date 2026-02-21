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

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockCollection;
import org.apache.hadoop.hdfs.server.namenode.ha.HAContext;
import org.apache.hadoop.hdfs.util.RwLock;
//定义了与 NameNode 文件系统相关的一些操作，主要涵盖 NameNode 的运行状态、快照、缓存管理、HA（高可用性）上下文等
/** Namesystem operations. */
@InterfaceAudience.Private
public interface Namesystem extends RwLock, SafeMode {
  /**
   * Is this name system running?
   */
  // 检查 NameNode 是否正在运行。
  // 返回 true 表示 NameNode 正在运行，false 表示 NameNode 没有在运行或已经停止
  boolean isRunning();
  // 根据块集合的 ID 获取对应的块集合。
  // 返回一个 BlockCollection 对象，该对象包含了特定 ID 下的块信息
  BlockCollection getBlockCollection(long id);
  // 获取文件系统的目录对象 (FSDirectory)。
  // 文件系统的目录对象用于管理文件和目录的元数据，包含了文件系统的结构信息
  FSDirectory getFSDirectory();
  // 如果需要，启动 SecretManager。
  // SecretManager 用于管理和保护敏感数据，如权限信息和加密密钥。该方法保证在必要时启动这个管理器
  void startSecretManagerIfNecessary();
  // 检查指定的块集合是否属于某个快照。
  // 快照是文件系统在某一时间点的只读副本，可以用于恢复文件的历史状态
  boolean isInSnapshot(long blockCollectionID);
  // 获取缓存管理器 (CacheManager) 对象。
  // CacheManager 用于管理 NameNode 中的缓存数据，例如文件系统的元数据缓存
  CacheManager getCacheManager();
  // 获取高可用性上下文 (HAContext) 对象。
  // 高可用性上下文用于管理 Hadoop 集群中的高可用性配置和状态，确保在发生故障时，NameNode 可以顺利切换
  HAContext getHAContext();

  /**
   * @return Whether the namenode is transitioning to active state and is in the
   *         middle of the starting active services.
   */
  // 检查 NameNode 是否正在进行从备份到激活状态的过渡。
  // 这个状态通常发生在 HDFS 的高可用性模式下，当一个 NameNode 从备用模式切换到活动模式时
  boolean inTransitionToActive();

  /** 从 inode 中移除指定的扩展属性 (xAttr)。
   * xAttr 是文件系统中与文件或目录相关联的元数据属性，通常用于表示某些额外的特性或标记
   * Remove xAttr from the inode.
   * @param id
   * @param xattrName
   * @throws IOException
   */
  void removeXattr(long id, String xattrName) throws IOException;

  /**
   * Check if snapshot roots are created for all existing snapshottable
   * directories. Create them if not.
   */
  // 检查并为所有现有的快照目录创建快照垃圾回收根目录。
  // 如果某些目录尚未设置快照垃圾回收根目录，该方法将进行创建。
  // 快照垃圾回收根目录用于存储被删除文件的快照副本
  void checkAndProvisionSnapshotTrashRoots();
}
