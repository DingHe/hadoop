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
package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Subclass of {@link BlockInfo}, presenting a block group in erasure coding.
 *
 * We still use triplets to store DatanodeStorageInfo for each block in the
 * block group, as well as the previous/next block in the corresponding
 * DatanodeStorageInfo. For a (m+k) block group, the first (m+k) triplet units
 * are sorted and strictly mapped to the corresponding block.
 *
 * Normally each block belonging to group is stored in only one DataNode.
 * However, it is possible that some block is over-replicated. Thus the triplet
 * array's size can be larger than (m+k). Thus currently we use an extra byte
 * array to record the block index for each triplet.
 */
// 专门用于表示和管理 纠删码块组（Block Group）
// 传统的 HDFS 使用“连续存储”模式（Contiguous），一个物理块对应多个完全相同的副本。而纠删码（EC）将一个逻辑块组切分为多个**条带化（Striping）**的内部块。
// 作用在于：
// 块组元数据管理：在 NameNode 内存中代表整个块组，而不是单个物理块。
// 映射物理分布：管理该块组内的各个数据块（Data Units）和校验块（Parity Units）分别存储在哪些 DataNode 的哪个存储单元（Storage）上。
// 处理超量副本：在 EC 模式下，虽然通常一个索引只对应一个副本，但由于网络震荡或重构，可能出现同一个索引对应多个 DataNode 的情况（Over-replicated），该类通过 indices 数组来处理这种复杂的对应关系。
@InterfaceAudience.Private
public class BlockInfoStriped extends BlockInfo {
  // 关联该块组使用的纠删码策略。
  private final ErasureCodingPolicy ecPolicy;
  /**
   * Always the same size with triplets. Record the block index for each triplet
   * TODO: actually this is only necessary for over-replicated block. Thus can
   * be further optimized to save memory usage.
   */
  // 存储每个存储槽（Triplet）对应的内部块索引。
  // 与父类的 triplets 数组一一对应。triplets 记录了节点信息，而 indices 记录了该节点上存的是这个块组里的第几个块（例如 0-5 是数据块，6-8 是校验块）
  private byte[] indices;

  public BlockInfoStriped(Block blk, ErasureCodingPolicy ecPolicy) {
    super(blk, (short) (ecPolicy.getNumDataUnits() + ecPolicy.getNumParityUnits()));
    indices = new byte[ecPolicy.getNumDataUnits() + ecPolicy.getNumParityUnits()];
    initIndices();
    this.ecPolicy = ecPolicy;
  }
  // 分别返回该策略下的总块数、数据块数和校验块数。
  public short getTotalBlockNum() {
    return (short) (ecPolicy.getNumDataUnits() + ecPolicy.getNumParityUnits());
  }

  public short getDataBlockNum() {
    return (short) ecPolicy.getNumDataUnits();
  }

  public short getParityBlockNum() {
    return (short) ecPolicy.getNumParityUnits();
  }
  // 返回纠删码的单元格大小。
  public int getCellSize() {
    return ecPolicy.getCellSize();
  }

  /**
   * If the block is committed/completed and its length is less than a full
   * stripe, it returns the the number of actual data blocks.
   * Otherwise it returns the number of data units specified by erasure coding policy.
   */
  // 如果是正在写的块，返回策略定义的数量；如果已完成，则根据实际字节数计算真实的内部数据块数量（因为小文件可能填不满所有条带）。
  public short getRealDataBlockNum() {
    if (isComplete() || getBlockUCState() == BlockUCState.COMMITTED) {
      return (short) Math.min(getDataBlockNum(),
          (getNumBytes() - 1) / ecPolicy.getCellSize() + 1);
    } else {
      return getDataBlockNum();
    }
  }

  public short getRealTotalBlockNum() {
    return (short) (getRealDataBlockNum() + getParityBlockNum());
  }

  public ErasureCodingPolicy getErasureCodingPolicy() {
    return ecPolicy;
  }

  private void initIndices() {
    for (int i = 0; i < indices.length; i++) {
      indices[i] = -1;
    }
  }
  // 当发生超量副本（Over-replication）时，寻找 triplets 数组中多出来的空位。如果没有空位，则调用 ensureCapacity 进行扩容。
  private int findSlot() {
    int i = getTotalBlockNum();
    int capacity = getCapacity();
    for (; i < capacity; i++) {
      if (getStorageInfo(i) == null) {
        return i;
      }
    }
    // need to expand the triplet size
    ensureCapacity(i + 1, true);
    return i;
  }
  // 负责处理 DataNode 上报的纠删码块，并将其元数据记录到 NameNode 内存中。
  // 纠删码（EC）的一个显著特点是：一个“块组”包含多个不同索引的“内部块”。这个方法的核心逻辑就是判断上报的是哪一个分片，并决定存放在数组的哪个位置。
  @Override
  boolean addStorage(DatanodeStorageInfo storage, Block reportedBlock) {
    // 确保上报的块 ID 确实属于纠删码类型
    Preconditions.checkArgument(BlockIdManager.isStripedBlockID(
        reportedBlock.getBlockId()), "reportedBlock is not striped");
    // 将上报的物理子块 ID 转换为所属块组的主 ID（Base ID），检查它是否真的属于当前的 BlockInfoStriped 对象。防止 DataNode 乱报或报错。
    Preconditions.checkArgument(BlockIdManager.convertToStripedID(
        reportedBlock.getBlockId()) == this.getBlockId(),
        "reported blk_%s does not belong to the group of stored blk_%s",
        reportedBlock.getBlockId(), this.getBlockId());
    // 确定该子块在块组中的具体位置（例如：是第 1 个数据块还是第 3 个校验块）。
    int blockIndex = BlockIdManager.getBlockIndex(reportedBlock);
    int index = blockIndex;
    // 检查预定位置（index）是否已经被其他 DataNode 占用了。
    DatanodeStorageInfo old = getStorageInfo(index);
    if (old != null && !old.equals(storage)) { // over replicated
      // check if the storage has been stored
      // 为超量副本寻找“备用座位”。
      int i = findStorageInfo(storage);
      if (i == -1) {
        index = findSlot();
      } else {
        return true;
      }
    }
    addStorage(storage, index, blockIndex);
    return true;
  }
  // 用于将特定的 DataNode 存储信息和它所承载的内部块索引（Internal Block Index）正式写入到 NameNode 的内存数组中。
  private void addStorage(DatanodeStorageInfo storage, int index,
      int blockIndex) {
    // 将 DataNode 的存储单元（DatanodeStorageInfo）保存到 triplets 数组的指定槽位中。
    // index 是数组的下标。这一步让 NameNode 知道：“这个块组的某一部分存放在这个 DataNode 上”。
    setStorageInfo(index, storage);
    setNext(index, null);
    setPrevious(index, null);
    indices[index] = (byte) blockIndex;
  }

  private int findStorageInfoFromEnd(DatanodeStorageInfo storage) {
    final int len = getCapacity();
    for(int idx = len - 1; idx >= 0; idx--) {
      DatanodeStorageInfo cur = getStorageInfo(idx);
      if (storage.equals(cur)) {
        return idx;
      }
    }
    return -1;
  }

  @VisibleForTesting
  public byte getStorageBlockIndex(DatanodeStorageInfo storage) {
    int i = this.findStorageInfo(storage);
    return i == -1 ? -1 : indices[i];
  }

  /**
   * Identify the block stored in the given datanode storage. Note that
   * the returned block has the same block Id with the one seen/reported by the
   * DataNode.
   */
  Block getBlockOnStorage(DatanodeStorageInfo storage) {
    int index = getStorageBlockIndex(storage);
    if (index < 0) {
      return null;
    } else {
      Block block = new Block(this);
      block.setBlockId(this.getBlockId() + index);
      return block;
    }
  }

  @Override
  boolean removeStorage(DatanodeStorageInfo storage) {
    int dnIndex = findStorageInfoFromEnd(storage);
    if (dnIndex < 0) { // the node is not found
      return false;
    }
    assert getPrevious(dnIndex) == null && getNext(dnIndex) == null :
        "Block is still in the list and must be removed first.";
    // set the triplet to null
    setStorageInfo(dnIndex, null);
    setNext(dnIndex, null);
    setPrevious(dnIndex, null);
    indices[dnIndex] = -1;
    return true;
  }
  // 用于动态扩容
  // 在纠删码（EC）模式下，虽然一个块组的理想副本数是固定的（如 RS-6-3 为 9 个），
  // 但由于数据迁移、节点恢复或错误的超量副本，NameNode 可能需要记录超过预期的存储位置。这时就需要调用此方法来扩大内存中的数组容量。
  private void ensureCapacity(int totalSize, boolean keepOld) {
    // 检查当前已有的槽位容量（Capacity）是否小于请求的目标容量 totalSize
    if (getCapacity() < totalSize) {
      // 备份旧数据指针
      Object[] old = triplets;
      byte[] oldIndices = indices;
      // 按新的 totalSize 分配更大的数组空间。
      triplets = new Object[totalSize * 3];
      indices = new byte[totalSize];
      initIndices();
      // 如果 keepOld 为 true，则将原有的元数据迁移到新数组中。
      if (keepOld) {
        System.arraycopy(old, 0, triplets, 0, old.length);
        System.arraycopy(oldIndices, 0, indices, 0, oldIndices.length);
      }
    }
  }

  public long spaceConsumed() {
    // In case striped blocks, total usage by this striped blocks should
    // be the total of data blocks and parity blocks because
    // `getNumBytes` is the total of actual data block size.
    return StripedBlockUtil.spaceConsumedByStripedBlock(getNumBytes(),
        ecPolicy.getNumDataUnits(), ecPolicy.getNumParityUnits(),
        ecPolicy.getCellSize());
    }

  @Override
  public final boolean isStriped() {
    return true;
  }

  @Override
  public BlockType getBlockType() {
    return BlockType.STRIPED;
  }

  @Override
  public int numNodes() {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert triplets.length % 3 == 0 : "Malformed BlockInfo";
    int num = 0;
    for (int idx = getCapacity()-1; idx >= 0; idx--) {
      if (getStorageInfo(idx) != null) {
        num++;
      }
    }
    return num;
  }

  @Override
  final boolean hasNoStorage() {
    final int len = getCapacity();
    for(int idx = 0; idx < len; idx++) {
      if (getStorageInfo(idx) != null) {
        return false;
      }
    }
    return true;
  }

  /**
   * Striped blocks on Provided Storage is not supported. All blocks on
   * Provided storage are assumed to be "contiguous".
   */
  @Override
  boolean isProvided() {
    return false;
  }

  /**
   * This class contains datanode storage information and block index in the
   * block group.
   */
  public static class StorageAndBlockIndex {
    private final DatanodeStorageInfo storage;
    private final byte blockIndex;

    StorageAndBlockIndex(DatanodeStorageInfo storage, byte blockIndex) {
      this.storage = storage;
      this.blockIndex = blockIndex;
    }

    /**
     * @return storage in the datanode.
     */
    public DatanodeStorageInfo getStorage() {
      return storage;
    }

    /**
     * @return block index in the block group.
     */
    public byte getBlockIndex() {
      return blockIndex;
    }
  }

  public Iterable<StorageAndBlockIndex> getStorageAndIndexInfos() {
    return new Iterable<StorageAndBlockIndex>() {
      @Override
      public Iterator<StorageAndBlockIndex> iterator() {
        return new Iterator<StorageAndBlockIndex>() {
          private int index = 0;

          @Override
          public boolean hasNext() {
            while (index < getCapacity() && getStorageInfo(index) == null) {
              index++;
            }
            return index < getCapacity();
          }

          @Override
          public StorageAndBlockIndex next() {
            if (!hasNext()) {
              throw new NoSuchElementException();
            }
            int i = index++;
            return new StorageAndBlockIndex(
                (DatanodeStorageInfo) triplets[i * 3], indices[i]);
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException("Remove is not supported");
          }
        };
      }
    };
  }
}
