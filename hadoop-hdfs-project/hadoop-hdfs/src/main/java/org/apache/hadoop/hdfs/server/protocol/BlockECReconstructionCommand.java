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
package org.apache.hadoop.hdfs.server.protocol;

import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;

import java.util.Arrays;
import java.util.Collection;

/**
 * A BlockECReconstructionCommand is an instruction to a DataNode to
 * reconstruct a striped block group with missing blocks.
 *
 * Upon receiving this command, the DataNode pulls data from other DataNodes
 * hosting blocks in this group and reconstructs the lost blocks through codec
 * calculation.
 *
 * After the reconstruction, the DataNode pushes the reconstructed blocks to
 * their final destinations if necessary (e.g., the destination is different
 * from the reconstruction node, or multiple blocks in a group are to be
 * reconstructed).
 */
// 在 HDFS 的纠删码（Erasure Coding, EC）机制中，BlockECReconstructionCommand 是 NameNode 向 DataNode 下达的核心指令之一。
// 它负责指挥 DataNode 如何恢复丢失的条带数据块。
// 当 NameNode 发现某个纠删码块组（Block Group）出现了副本缺失（例如某个 DataNode 掉线导致部分数据块或校验块丢失）时，它不会简单地进行“复制”，而是需要进行“重构”。
// BlockECReconstructionCommand 的作用就是封装重构任务详情并发送给选定的执行节点（通常是一个 DataNode）。执行节点收到此命令后会：
// 拉取数据：从其他存活的 DataNode（Source）下载该块组中剩余的数据块和校验块。
// 解码计算：利用指定的纠删码算法（如 Reed-Solomon）计算出丢失的块。
// 推送数据：将重构好的块发送到最终的目标存储（Target）。
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BlockECReconstructionCommand extends DatanodeCommand {
  // 该命令包含的任务集合。
  // 一次心跳响应可以包含多个重构任务，每个任务对应一个特定的块组恢复。
  private final Collection<BlockECReconstructionInfo> ecTasks;

  /**
   * Create BlockECReconstructionCommand from a collection of
   * {@link BlockECReconstructionInfo}, each representing a reconstruction
   * task
   */
  public BlockECReconstructionCommand(int action,
      Collection<BlockECReconstructionInfo> blockECReconstructionInfoList) {
    super(action);
    this.ecTasks = blockECReconstructionInfoList;
  }
  // 格式化输出，方便在 NameNode 日志中查看当前下发了哪些重构任务。
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("BlockECReconstructionCommand(\n  ");
    Joiner.on("\n  ").appendTo(sb, ecTasks);
    sb.append("\n)");
    return sb.toString();
  }

  /** Block and targets pair */
  // 真正的“任务说明书”，包含了恢复一个块组所需的所有细节。
  @InterfaceAudience.Private
  @InterfaceStability.Evolving
  public static class BlockECReconstructionInfo {
    // 待恢复的块组（Block Group）对象，包含 Pool ID 和 Block ID。
    private final ExtendedBlock block;
    // 数据来源节点。
    // DataNode 需要从这些节点下载现存的块。
    private final DatanodeInfo[] sources;
    // 重构目标节点。
    // 重构出来的块最终应该存储到哪些 DataNode 上。
    private DatanodeInfo[] targets;
    // 指定目标节点上的具体存储 ID 和存储类型（如 SSD/DISK），确保数据落在正确的磁盘上。
    private String[] targetStorageIDs;
    private StorageType[] targetStorageTypes;
    // 存活块的索引。
    // 对应 EC 策略中的位置（例如 0-5 是数据块，6-8 是校验块），告知 DataNode 当前从 Source 读到的是哪些位置的块。
    private final byte[] liveBlockIndices;
    // 排除重构的索引。在某些复杂场景下，告知 DataNode 哪些位置虽然丢了但不需要在本次任务中恢复。
    private final byte[] excludeReconstructedIndices;
    // 纠删码策略。告知 DataNode 应该用什么算法（如 RS-6-3）来计算数据。
    private final ErasureCodingPolicy ecPolicy;

    public BlockECReconstructionInfo(ExtendedBlock block,
        DatanodeInfo[] sources, DatanodeStorageInfo[] targetDnStorageInfo,
        byte[] liveBlockIndices, byte[] excludeReconstructedIndices, ErasureCodingPolicy ecPolicy) {
      this(block, sources, DatanodeStorageInfo
          .toDatanodeInfos(targetDnStorageInfo), DatanodeStorageInfo
          .toStorageIDs(targetDnStorageInfo), DatanodeStorageInfo
          .toStorageTypes(targetDnStorageInfo), liveBlockIndices,
          excludeReconstructedIndices, ecPolicy);
    }

    public BlockECReconstructionInfo(ExtendedBlock block,
        DatanodeInfo[] sources, DatanodeInfo[] targets,
        String[] targetStorageIDs, StorageType[] targetStorageTypes,
        byte[] liveBlockIndices, byte[] excludeReconstructedIndices, ErasureCodingPolicy ecPolicy) {
      this.block = block;
      this.sources = sources;
      this.targets = targets;
      this.targetStorageIDs = targetStorageIDs;
      this.targetStorageTypes = targetStorageTypes;
      this.liveBlockIndices = liveBlockIndices == null ?
          new byte[]{} : liveBlockIndices;
      this.excludeReconstructedIndices = excludeReconstructedIndices;
      this.ecPolicy = ecPolicy;
    }

    public ExtendedBlock getExtendedBlock() {
      return block;
    }

    public DatanodeInfo[] getSourceDnInfos() {
      return sources;
    }

    public DatanodeInfo[] getTargetDnInfos() {
      return targets;
    }

    public String[] getTargetStorageIDs() {
      return targetStorageIDs;
    }

    public StorageType[] getTargetStorageTypes() {
      return targetStorageTypes;
    }

    public byte[] getLiveBlockIndices() {
      return liveBlockIndices;
    }

    public byte[] getExcludeReconstructedIndices() {
      return excludeReconstructedIndices;
    }

    public ErasureCodingPolicy getErasureCodingPolicy() {
      return ecPolicy;
    }

    @Override
    public String toString() {
      return new StringBuilder().append("BlockECReconstructionInfo(\n  ")
          .append("Recovering ").append(block).append(" From: ")
          .append(Arrays.asList(sources)).append(" To: [")
          .append(Arrays.asList(targets)).append(")\n")
          .append(" Block Indices: ").append(Arrays.toString(liveBlockIndices))
          .toString();
    }
  }

  public Collection<BlockECReconstructionInfo> getECTasks() {
    return this.ecTasks;
  }
}
