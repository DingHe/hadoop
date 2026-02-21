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

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Identifies a Block uniquely across the block pools
 */
// 在 HDFS 1.x 版本中，整个集群只有一个命名空间（Namespace）。
// 但在 HDFS 2.x 引入 Federation（联邦） 之后，一个集群可以有多个独立的 NameNode，每个 NameNode 管理自己的 Block Pool（块池）。
// ExtendedBlock 的核心作用是：在全集群范围内唯一标识一个数据块。
// 普通 Block 类：只包含块 ID、长度和时间戳。在 Federation 环境下，不同的块池可能会出现相同的块 ID。
// ExtendedBlock 类：在 Block 的基础上增加了 poolId（块池 ID）。
// 唯一性公式：ExtendedBlock = Block Pool ID + Block ID。
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ExtendedBlock {
  // 所属块池的唯一标识符。
  // 它通常对应一个特定的 NameNode 命名空间。
  // 代码中使用了 poolId.intern()，这是为了通过 JVM 的字符串常量池节省内存，因为成千上万个块可能属于同一个 poolId。
  private String poolId;
  // 实际的数据块元数据。
  private Block block;

  public ExtendedBlock() {
    this(null, 0, 0, 0);
  }

  public ExtendedBlock(final ExtendedBlock b) {
    this(b.poolId, new Block(b.block));
  }

  public ExtendedBlock(final String poolId, final long blockId) {
    this(poolId, blockId, 0, 0);
  }

  public ExtendedBlock(String poolId, Block b) {
    this.poolId = poolId != null ? poolId.intern() : null;
    this.block = b;
  }

  public ExtendedBlock(final String poolId, final long blkid, final long len,
      final long genstamp) {
    this.poolId = poolId != null ? poolId.intern() : null;
    block = new Block(blkid, len, genstamp);
  }

  public String getBlockPoolId() {
    return poolId;
  }

  /** Returns the block file name for the block */
  public String getBlockName() {
    return block.getBlockName();
  }

  public long getNumBytes() {
    return block.getNumBytes();
  }

  public long getBlockId() {
    return block.getBlockId();
  }

  public long getGenerationStamp() {
    return block.getGenerationStamp();
  }

  public void setBlockId(final long bid) {
    block.setBlockId(bid);
  }

  public void setGenerationStamp(final long genStamp) {
    block.setGenerationStamp(genStamp);
  }

  public void setNumBytes(final long len) {
    block.setNumBytes(len);
  }

  public void set(String poolId, Block blk) {
    this.poolId = poolId != null ? poolId.intern() : null;
    this.block = blk;
  }

  public static Block getLocalBlock(final ExtendedBlock b) {
    return b == null ? null : b.getLocalBlock();
  }

  public Block getLocalBlock() {
    return block;
  }

  @Override // Object
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ExtendedBlock)) {
      return false;
    }
    ExtendedBlock b = (ExtendedBlock)o;
    return b.block.equals(block) &&
        (b.poolId != null ? b.poolId.equals(poolId) : poolId == null);
  }

  @Override // Object
  public int hashCode() {
    return new HashCodeBuilder(31, 17).
        append(poolId).
        append(block).
        toHashCode();
  }

  @Override // Object
  public String toString() {
    return poolId + ":" + block;
  }
}
