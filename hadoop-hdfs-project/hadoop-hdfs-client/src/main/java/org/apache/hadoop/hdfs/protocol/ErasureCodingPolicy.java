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
package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.util.Preconditions;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.apache.hadoop.io.erasurecode.ErasureCodeConstants;

import java.io.Serializable;

/**
 * A policy about how to write/read/code an erasure coding file.
 * <p>
 * Note this class should be lightweight and immutable, because it's cached
 * by {@link SystemErasureCodingPolicies}, to be returned as a part of
 * {@link HdfsFileStatus}.
 */
// ErasureCodingPolicy（纠删码策略）定义了一个文件在 HDFS 中如何进行编码、分块和存储。
// 传统的 HDFS 使用副本机制（Replication），
// 而 EC 机制通过数学算法（如 Reed-Solomon）将文件切分为若干个数据单元（Data Units）并计算出校验单元（Parity Units）。
// 主要作用包括：
// 定义布局：确定数据块和校验块的数量（例如 RS-6-3 表示 6 个数据块，3 个校验块）。
// 确定切片大小：定义“单元格（Cell Size）”的大小，这是数据条带化（Striping）的最小物理单位。
// 身份标识：为每种策略提供唯一的名称和 ID，方便 NameNode 在处理文件状态（HdfsFileStatus）时快速识别。
@InterfaceAudience.Private
public final class ErasureCodingPolicy implements Serializable {

  private static final long serialVersionUID = 0x0079fe4e;
  // 策略的唯一名称。
  // 例如 RS-6-3-1024k。它是策略的可读标识，通常由算法名称、参数和单元格大小组合而成。
  private final String name;
  // 纠删码模式定义。
  // 包含了具体的编码器名称（如 rs）、数据单元数量、校验单元数量以及其他算法相关的特定参数。
  private final ECSchema schema;
  // 单元格大小（字节）。
  // 定义了在将数据分布到不同 DataNode 之前，连续数据块切片的大小。HDFS 强制要求该值必须是 1024 的倍数（1k 对齐）。
  private final int cellSize;
  // 策略的唯一数字 ID。
  // 用于在存储和网络传输中高效识别策略。系统预定义策略 ID 较小，用户自定义策略 ID 较大。
  private final byte id;

  public ErasureCodingPolicy(String name, ECSchema schema,
      int cellSize, byte id) {
    Preconditions.checkNotNull(name);
    Preconditions.checkNotNull(schema);
    Preconditions.checkArgument(cellSize > 0, "cellSize must be positive");
    Preconditions.checkArgument(cellSize % 1024 == 0,
        "cellSize must be 1024 aligned");
    this.name = name;
    this.schema = schema;
    this.cellSize = cellSize;
    this.id = id;
  }

  public ErasureCodingPolicy(ECSchema schema, int cellSize, byte id) {
    this(composePolicyName(schema, cellSize), schema, cellSize, id);
  }

  public ErasureCodingPolicy(ECSchema schema, int cellSize) {
    this(composePolicyName(schema, cellSize), schema, cellSize, (byte) -1);
  }

  public static String composePolicyName(ECSchema schema, int cellSize) {
    Preconditions.checkNotNull(schema);
    Preconditions.checkArgument(cellSize > 0, "cellSize must be positive");
    Preconditions.checkArgument(cellSize % 1024 == 0,
        "cellSize must be 1024 aligned");
    return schema.getCodecName().toUpperCase() + "-" +
        schema.getNumDataUnits() + "-" + schema.getNumParityUnits() +
        "-" + cellSize / 1024 + "k";
  }

  public String getName() {
    return name;
  }

  public ECSchema getSchema() {
    return schema;
  }

  public int getCellSize() {
    return cellSize;
  }

  public int getNumDataUnits() {
    return schema.getNumDataUnits();
  }

  public int getNumParityUnits() {
    return schema.getNumParityUnits();
  }

  public String getCodecName() {
    return schema.getCodecName();
  }

  public byte getId() {
    return id;
  }

  public boolean isReplicationPolicy() {
    return (id == ErasureCodeConstants.REPLICATION_POLICY_ID);
  }

  public boolean isSystemPolicy() {
    return (this.id < ErasureCodeConstants.USER_DEFINED_POLICY_START_ID);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (o == this) {
      return true;
    }
    if (o.getClass() != getClass()) {
      return false;
    }
    ErasureCodingPolicy rhs = (ErasureCodingPolicy) o;
    return new EqualsBuilder()
        .append(name, rhs.name)
        .append(schema, rhs.schema)
        .append(cellSize, rhs.cellSize)
        .append(id, rhs.id)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(303855623, 582626729)
        .append(name)
        .append(schema)
        .append(cellSize)
        .append(id)
        .toHashCode();
  }

  @Override
  public String toString() {
    return "ErasureCodingPolicy=[" + "Name=" + name + ", "
        + "Schema=[" + schema.toString() + "], "
        + "CellSize=" + cellSize + ", "
        + "Id=" + id
        + "]";
  }
}
