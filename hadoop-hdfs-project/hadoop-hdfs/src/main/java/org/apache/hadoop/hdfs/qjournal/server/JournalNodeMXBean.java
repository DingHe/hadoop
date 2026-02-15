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
package org.apache.hadoop.hdfs.qjournal.server;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.util.List;

/** 是 HDFS QJM（Quorum Journal Manager） 机制中的核心组件，主要用于 高可用性（HA） 配置中管理和持久化 EditLog（编辑日志）。
 * JournalNodeMXBean 接口使 Hadoop 运维人员或监控系统能够通过 JMX 方式获取 JournalNode 的信息，方便对 HDFS 集群进行监控和管理
 * This is the JMX management interface for JournalNode information
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface JournalNodeMXBean {
  
  /**
   * Get status information (e.g., whether formatted) of JournalNode's journals.
   *  返回一个 字符串，描述 JournalNode 中每个 Journal（即日志存储）状态的信息
   * @return A string presenting status for each journal
   */
  String getJournalsStatus();

  /**
   * Get host and port of JournalNode.
   * 返回一个字符串，格式为 host:port，表示 JournalNode 服务所在的 主机名 和 端口号
   * @return colon separated host and port.
   */
  String getHostAndPort();

  /**
   * Get list of the clusters of JournalNode's journals
   * as one JournalNode may support multiple clusters.
   * 返回一个 字符串列表，包含 JournalNode 支持的 HDFS 集群 ID
   * @return list of clusters.
   */
  List<String> getClusterIds();

  /**
   * Gets the version of Hadoop.
   * 返回 JournalNode 所属的 Hadoop 版本
   * @return the version of Hadoop.
   */
  String getVersion();

  /**
   * Get the start time of the JournalNode.
   * 返回 JournalNode 启动时间，以 毫秒为单位（自 UNIX 纪元时间 1970-01-01 00:00:00 UTC 起的毫秒数）
   * @return the start time of the JournalNode.
   */
  long getJNStartedTimeInMillis();

  /**
   * Get the list of the storage infos of JournalNode's journals. Storage infos
   * include layout version, namespace id, cluster id and creation time of the
   * File system state.
   * 返回 JournalNode 管理的所有 Journal 存储信息，以字符串列表形式呈现
   * @return the list of storage infos associated with journals.
   */
  List<String> getStorageInfos();
}
