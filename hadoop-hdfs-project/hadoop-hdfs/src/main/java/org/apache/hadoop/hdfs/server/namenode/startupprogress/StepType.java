/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.server.namenode.startupprogress;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Indicates a particular type of {@link Step}.
 */
// 主要作用是定义 NameNode 启动过程中各个具体步骤的“分类标签”。
// 当 NameNode 启动时，它需要经历加载镜像（fsimage）、回放编辑日志（editlog）以及等待块汇报等多个阶段。
// 为了让管理员能够通过 HTTP 界面或 JMX 查看到底启动到了哪一步，以及每一步消耗了多少时间，HDFS 设计了 StartupProgress 机制。
@InterfaceAudience.Private
public enum StepType {
  /**
   * The namenode has entered safemode and is awaiting block reports from
   * datanodes.
   */
  // 等待块汇报。
  // NameNode 进入安全模式，正在等待 DataNodes 上报它们持有的数据块信息，以达到安全副本水位线。
  AWAITING_REPORTED_BLOCKS("AwaitingReportedBlocks", "awaiting reported blocks"),

  /**
   * The namenode is performing an operation related to delegation keys.
   */
  // 委托令牌密钥。
  // NameNode 正在加载或处理用于安全认证的委托令牌根密钥。
  DELEGATION_KEYS("DelegationKeys", "delegation keys"),

  /**
   * The namenode is performing an operation related to delegation tokens.
   */
  // 委托令牌。
  // NameNode 正在从元数据中恢复已发放的委托令牌。
  DELEGATION_TOKENS("DelegationTokens", "delegation tokens"),

  /**
   * The namenode is performing an operation related to inodes.
   */
  // iNodes（索引节点）。
  // 启动过程中最耗时的部分之一，NameNode 正在加载文件系统的目录树结构。
  INODES("Inodes", "inodes"),

  /**
   * The namenode is performing an operation related to cache pools.
   */
  // 加载 HDFS 集中式缓存（Centralized Cache Management）的存储池定义。
  CACHE_POOLS("CachePools", "cache pools"),

  /**
   * The namenode is performing an operation related to cache entries.
   */
  // 加载具体的缓存指令，确定哪些路径需要被缓存在 DataNode 内存中。
  CACHE_ENTRIES("CacheEntries", "cache entries"),

  /**
   * The namenode is performing an operation related to erasure coding policies.
   */
  // 加载集群定义的纠删码方案（如我们之前讨论的 RS-6-3、XOR 等）。
  ERASURE_CODING_POLICIES("ErasureCodingPolicies", "erasure coding policies");

  private final String name, description;

  /**
   * Private constructor of enum.
   * 
   * @param name String step type name
   * @param description String step type description
   */
  private StepType(String name, String description) {
    this.name = name;
    this.description = description;
  }

  /**
   * Returns step type description.
   * 
   * @return String step type description
   */
  public String getDescription() {
    return description;
  }

  /**
   * Returns step type name.
   * 
   * @return String step type name
   */
  public String getName() {
    return name;
  }
}
