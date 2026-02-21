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
package org.apache.hadoop.io.erasurecode;

/**
 * Constants related to the erasure code feature.
 */
// 集中管理纠删码相关的硬编码字符串、数值限定以及预定义的算法模式（Schema）。
// 其核心作用包括：
// 标准化命名：确保在整个 Hadoop 项目中，编解码器（Codec）的名字（如 "rs"）是统一的，避免因拼写错误导致的系统崩溃。
// 预定义常用模式：提前定义好工业界常用的 EC 方案（如 RS-6-3），方便其他类（如 ErasureCodingPolicyManager）直接引用。
// 划定权限边界：通过 ID 范围划分，区分系统内置策略和用户自定义策略，防止 ID 冲突。
public final class ErasureCodeConstants {

  private ErasureCodeConstants() {
  }
  // 哑编解码器。通常用于测试，不进行实际的数学计算。
  public static final String DUMMY_CODEC_NAME = "dummy";
  // 标准的里德-所罗门（Reed-Solomon）算法。这是 HDFS 中最常用的 EC 算法。
  public static final String RS_CODEC_NAME = "rs";
  // 旧版 RS 算法实现。用于兼容早期版本或特定硬件加速库。
  public static final String RS_LEGACY_CODEC_NAME = "rs-legacy";
  // 异或（XOR）算法。计算最简单，但容错能力有限（通常只能容忍一个单元损坏）。
  public static final String XOR_CODEC_NAME = "xor";
  // 分层异或（Hitchhiker XOR）算法。一种优化的异或方案，旨在减少恢复数据时的网络流出。
  public static final String HHXOR_CODEC_NAME = "hhxor";
  // 副本模式。为了逻辑统一，将传统的副本机制也视为一种特殊的“编码”方式。
  public static final String REPLICATION_CODEC_NAME = "replication";
  // 6 个数据单元 + 3 个校验单元。最平衡的方案，可容忍同时损坏 3 个块，存储效率为 1.5x。
  public static final ECSchema RS_6_3_SCHEMA = new ECSchema(
      RS_CODEC_NAME, 6, 3);
  // 3 数据 + 2 校验。适用于较小规模的集群。
  public static final ECSchema RS_3_2_SCHEMA = new ECSchema(
      RS_CODEC_NAME, 3, 2);

  public static final ECSchema RS_6_3_LEGACY_SCHEMA = new ECSchema(
      RS_LEGACY_CODEC_NAME, 6, 3);

  public static final ECSchema XOR_2_1_SCHEMA = new ECSchema(
      XOR_CODEC_NAME, 2, 1);
  // 10 数据 + 4 校验。存储效率更高（1.4x），但恢复数据时消耗更多 CPU 和带宽，适用于大规模冷数据。
  public static final ECSchema RS_10_4_SCHEMA = new ECSchema(
      RS_CODEC_NAME, 10, 4);
  // 内部特殊映射，模拟副本模式。
  public static final ECSchema REPLICATION_1_2_SCHEMA = new ECSchema(
      REPLICATION_CODEC_NAME, 1, 2);

  public static final byte MAX_POLICY_ID = Byte.MAX_VALUE;
  // 分界线：ID 在 0-63 之间的属于 HDFS 系统内置策略（不可修改）；ID 在 64-127 之间的允许用户自定义。
  public static final byte USER_DEFINED_POLICY_START_ID = (byte) 64;
  // 规定 ID 为 0 的策略永远代表传统的副本模式。
  public static final byte REPLICATION_POLICY_ID = (byte) 0;
  public static final String REPLICATION_POLICY_NAME = REPLICATION_CODEC_NAME;
}
