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
 * This interface defines the methods to get status pertaining to blocks of type
 * {@link org.apache.hadoop.hdfs.protocol.BlockType#STRIPED} in FSNamesystem
 * of a NameNode. It is also used for publishing via JMX.
 * <p>
 * Aggregated status of all blocks is reported in
 * @see FSNamesystemMBean
 * Name Node runtime activity statistic info is reported in
 * @see org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics
 * 提供了一些方法用于获取 HDFS 系统中擦除编码块组的运行状态，包括冗余、损坏、丢失、待删除等情况。通过这些方法，管理员可以监控 NameNode 上的擦除编码块组状态，并根据需要采取相应的维护措施
 *///MBean（管理 Bean）是一种用于管理和监控 Java 应用程序、系统资源或设备的组件。
  // 它是 Java 管理扩展（JMX，Java Management Extensions）框架的一部分，JMX 提供了一个标准的 API 来实现应用程序的管理和监控
@InterfaceAudience.Private
public interface ECBlockGroupsMBean {
  /**该方法检查并返回当前文件系统中冗余度较低的擦除编码块组的数量。低冗余的块组通常意味着在某些块丢失或损坏的情况下，数据的恢复可能会受到影响
   * Return count of erasure coded block groups with low redundancy.
   */
  long getLowRedundancyECBlockGroups();

  /** 该方法返回当前文件系统中损坏的擦除编码块组的数量。损坏的块组可能无法正常工作，需要修复或重建
   * Return count of erasure coded block groups that are corrupt.
   */
  long getCorruptECBlockGroups();

  /**该方法返回文件系统中丢失的擦除编码块组的数量。丢失的块组意味着数据无法访问，可能需要重新恢复
   * Return count of erasure coded block groups that are missing.
   */
  long getMissingECBlockGroups();

  /**该方法返回文件系统中所有尚未完全形成的擦除编码块组的总字节数。未来块组通常是正在进行的操作中，尚未完全写入磁盘的部分
   * Return total bytes of erasure coded future block groups.
   */
  long getBytesInFutureECBlockGroups();

  /**该方法返回当前待删除的擦除编码块数量。这些块可能已经被标记为删除，正在等待实际删除操作
   * Return count of erasure coded blocks that are pending deletion.
   */
  long getPendingDeletionECBlocks();

  /**该方法返回当前系统中所有擦除编码块组的总数，包括正常、损坏、低冗余等所有类型的块组
   * Return total number of erasure coded block groups.
   */
  long getTotalECBlockGroups();

  /**该方法返回当前系统中启用的擦除编码策略的名称，多个策略之间用逗号分隔。这些策略决定了如何将数据分散到多个块组中进行冗余存储
   * @return the enabled erasure coding policies separated with comma.
   */
  String getEnabledEcPolicies();
}
