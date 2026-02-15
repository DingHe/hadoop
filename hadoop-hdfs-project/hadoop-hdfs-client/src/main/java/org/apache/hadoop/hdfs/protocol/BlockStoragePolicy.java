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

import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.BlockStoragePolicySpi;
import org.apache.hadoop.fs.StorageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 描述了如何为块的副本选择存储类型
 * A block storage policy describes how to select the storage types
 * for the replicas of a block.
 */
@InterfaceAudience.Private
public class BlockStoragePolicy implements BlockStoragePolicySpi {
  public static final Logger LOG = LoggerFactory.getLogger(BlockStoragePolicy
      .class);

  /** A 4-bit policy ID 存储策略的唯一标识符。它是一个 4 位的策略 ID，用于区分不同的存储策略*/
  private final byte id;
  /** Policy name 存储策略的名称，用于描述该策略的标识*/
  private final String name;
  //存储副本的首选存储类型。是一个 StorageType 类型的数组，指定了为新创建的块副本选择的存储类型
  /** The storage types to store the replicas of a new block. */
  private final StorageType[] storageTypes;
  /** The fallback storage type for block creation. */
  //块创建时的回退存储类型。如果在创建副本时，首选存储类型不可用，则会使用这些回退类型
  private final StorageType[] creationFallbacks;
  /** The fallback storage type for replication. */
  //块复制时的回退存储类型。与 creationFallbacks 类似，如果复制副本时首选存储类型不可用，则使用这些回退类型
  private final StorageType[] replicationFallbacks;
  /**
   * Whether the policy is inherited during file creation.
   * If set then the policy cannot be changed after file creation.
   */
  private boolean copyOnCreateFile;

  @VisibleForTesting
  public BlockStoragePolicy(byte id, String name, StorageType[] storageTypes,
      StorageType[] creationFallbacks, StorageType[] replicationFallbacks) {
    this(id, name, storageTypes, creationFallbacks, replicationFallbacks,
         false);
  }

  @VisibleForTesting
  public BlockStoragePolicy(byte id, String name, StorageType[] storageTypes,
      StorageType[] creationFallbacks, StorageType[] replicationFallbacks,
      boolean copyOnCreateFile) {
    this.id = id;
    this.name = name;
    this.storageTypes = storageTypes;
    this.creationFallbacks = creationFallbacks;
    this.replicationFallbacks = replicationFallbacks;
    this.copyOnCreateFile = copyOnCreateFile;
  }

  /** 用于根据副本数量 (replication) 选择存储类型，并避免选择临时存储类型
   * 返回一个 List<StorageType> 类型的列表，表示为块副本选择的存储类型
   * @return a list of {@link StorageType}s for storing the replicas of a block.
   */
  //replication：副本数量
  public List<StorageType> chooseStorageTypes(final short replication) {
    final List<StorageType> types = new LinkedList<>();//用于存储最终选择的存储类型
    int i = 0, j = 0;

    // Do not return transient storage types. We will not have accurate
    // usage information for transient types.
    for (;i < replication && j < storageTypes.length; ++j) {
      if (!storageTypes[j].isTransient()) {//如果当前存储类型不是临时存储类型，便将其添加到 types 列表中，并增加已选择的存储类型计数 i
        types.add(storageTypes[j]);
        ++i;
      }
    }
    //使用 storageTypes 数组的最后一个存储类型填充剩余副本
    final StorageType last = storageTypes[storageTypes.length - 1];
    if (!last.isTransient()) {
      for (; i < replication; i++) { //没有选择足够的存储类型（即 i < replication）
        types.add(last);
      }
    }
    return types;
  }

  /**
   * Choose the storage types for storing the remaining replicas, given the
   * replication number and the storage types of the chosen replicas.
   *
   * @param replication the replication number.
   * @param chosen the storage types of the chosen replicas.
   * @return a list of {@link StorageType}s for storing the replicas of a block.
   */
  //根据给定的副本数 (replication) 和已选择的存储类型 (chosen)，来选择合适的存储类型
  //chosen：已选择的存储类型，表示已用于存储某些副本的存储类型
  public List<StorageType> chooseStorageTypes(final short replication,
      final Iterable<StorageType> chosen) {
    return chooseStorageTypes(replication, chosen, null);
  }
  //excess：一个 List<StorageType>，用于存储差集的元素，即在 types 中存在但在 chosen 中没有的存储类型。如果不需要存储差集，传入 null
  private List<StorageType> chooseStorageTypes(final short replication,
      final Iterable<StorageType> chosen, final List<StorageType> excess) {
    final List<StorageType> types = chooseStorageTypes(replication);
    diff(types, chosen, excess);
    return types;
  }

  /**
   * Choose the storage types for storing the remaining replicas, given the
   * replication number, the storage types of the chosen replicas and
   * the unavailable storage types. It uses fallback storage in case that
   * the desired storage type is unavailable.
   *
   * @param replication the replication number.
   * @param chosen the storage types of the chosen replicas. 已选择的存储类型，用于指示哪些存储类型已经被用于当前副本
   * @param unavailables the unavailable storage types. 不可用的存储类型，表示当前不可用的存储类型，如果需要使用某个存储类型，而它在 unavailables 中，则需要被替换成备用存储类型
   * @param isNewBlock Is it for new block creation? 表示是否是为新块创建副本。如果是 true，表示正在为新块创建副本；如果为 false，则表示为现有块复制副本
   * @return a list of {@link StorageType}s for storing the replicas of a block.
   */
  //主要用于选择存储块副本的存储类型，考虑副本数、已选择的存储类型、不可用存储类型以及创建新块的标识
  public List<StorageType> chooseStorageTypes(final short replication,
      final Iterable<StorageType> chosen,
      final EnumSet<StorageType> unavailables,
      final boolean isNewBlock) {
    final List<StorageType> excess = new LinkedList<>();//用于存储 types 中的多余存储类型
    //选择了初步的存储类型
    final List<StorageType> storageTypes = chooseStorageTypes(
        replication, chosen, excess);
    //期望的存储类型数量
    final int expectedSize = storageTypes.size() - excess.size();
    final List<StorageType> removed = new LinkedList<>();
    for(int i = storageTypes.size() - 1; i >= 0; i--) {
      // replace/remove unavailable storage types.
      final StorageType t = storageTypes.get(i);
      if (unavailables.contains(t)) {
        final StorageType fallback = isNewBlock?
            getCreationFallback(unavailables)
            : getReplicationFallback(unavailables);
        if (fallback == null) {
          removed.add(storageTypes.remove(i));
        } else {
          storageTypes.set(i, fallback);
        }
      }
    }
    // remove excess storage types after fallback replacement.
    diff(storageTypes, excess, null);
    if (storageTypes.size() < expectedSize) {
      LOG.warn("Failed to place enough replicas: expected size is {}"
          + " but only {} storage types can be selected (replication={},"
          + " selected={}, unavailable={}" + ", removed={}" + ", policy={}"
          + ")", expectedSize, storageTypes.size(), replication, storageTypes,
          unavailables, removed, this);
    }
    return storageTypes;
  }

  /**
   * Compute the difference between two lists t and c so that after the diff
   * computation we have: t = t - c;
   * Further, if e is not null, set e = e + c - t;
   */
  //用于计算两个列表之间的差异，并在参数列表 t 上进行操作
  //1、从列表 t 中移除在列表 c 中也存在的元素，使得 t = t - c（即 t 中剩余的元素是 t 和 c 的差集）
  //2、如果 e 不为 null，将 c 中的元素添加到 e 列表中，前提是这些元素在 t 中不存在，即 e = e + c - t
  private static void diff(List<StorageType> t, Iterable<StorageType> c,
      List<StorageType> e) {
    for(StorageType storagetype : c) {
      final int i = t.indexOf(storagetype);//// 查找 c 中当前元素在 t 中的索引
      if (i >= 0) {// 如果 t 中存在该元素
        t.remove(i);// 从 t 中移除该元素
      } else if (e != null) { //// 如果 t 中没有该元素，并且 e 不为 null
        e.add(storagetype); // 将该元素添加到 e 列表中
      }
    }
  }

  /**
   * Choose excess storage types for deletion, given the
   * replication number and the storage types of the chosen replicas.
   *
   * @param replication the replication number.
   * @param chosen the storage types of the chosen replicas.
   * @return a list of {@link StorageType}s for deletion.
   */
  public List<StorageType> chooseExcess(final short replication,
      final Iterable<StorageType> chosen) {
    final List<StorageType> types = chooseStorageTypes(replication);
    final List<StorageType> excess = new LinkedList<>();
    diff(types, chosen, excess);
    return excess;
  }

  /** @return the fallback {@link StorageType} for creation. */
  public StorageType getCreationFallback(EnumSet<StorageType> unavailables) {
    return getFallback(unavailables, creationFallbacks);
  }

  /** @return the fallback {@link StorageType} for replication. */
  public StorageType getReplicationFallback(EnumSet<StorageType> unavailables) {
    return getFallback(unavailables, replicationFallbacks);
  }

  @Override
  public int hashCode() {
    return Byte.valueOf(id).hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    } else if (!(obj instanceof BlockStoragePolicy)) {
      return false;
    }
    final BlockStoragePolicy that = (BlockStoragePolicy)obj;
    return this.id == that.id;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "{" + name + ":" + id
        + ", storageTypes=" + Arrays.asList(storageTypes)
        + ", creationFallbacks=" + Arrays.asList(creationFallbacks)
        + ", replicationFallbacks=" + Arrays.asList(replicationFallbacks) + "}";
  }

  public byte getId() {
    return id;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public StorageType[] getStorageTypes() {
    return this.storageTypes;
  }

  @Override
  public StorageType[] getCreationFallbacks() {
    return this.creationFallbacks;
  }

  @Override
  public StorageType[] getReplicationFallbacks() {
    return this.replicationFallbacks;
  }

  private static StorageType getFallback(EnumSet<StorageType> unavailables,
      StorageType[] fallbacks) {
    for(StorageType fb : fallbacks) {
      if (!unavailables.contains(fb)) {
        return fb;
      }
    }
    return null;
  }

  @Override
  public boolean isCopyOnCreateFile() {
    return copyOnCreateFile;
  }
}
