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

import org.apache.hadoop.fs.StorageType;

import java.util.UUID;

/**
 * Class captures information of a storage in Datanode.
 */
// 在 HDFS 的架构设计中，一个 DataNode 并不只是一个单一的硬盘，它可能管理着多个磁盘、SSD 甚至是内存。DatanodeStorage 的作用就是抽象化这些存储单元。
// 唯一标识：为 DataNode 上的每一个存储目录分配唯一的 ID（StorageID）。
// 状态管理：监控该存储介质是正常的、只读的还是已经损坏（Failed）。
// 介质区分：标识存储类型（如机械硬盘 HDD、固态硬盘 SSD、内存 RAM_DISK 等）。
// 协议通信：它是 DataNode 向 NameNode 进行心跳汇报（Heartbeat）和块汇报（Block Report）时的重要组成部分，让 NameNode 知道具体的 Block 存放在哪个存储介质上。
public class DatanodeStorage {
  /** The state of the storage. */
  public enum State {
    // 正常状态，可读可写。
    NORMAL,

    /**
     * A storage that represents a read-only path to replicas stored on a shared
     * storage device. Replicas on {@link #READ_ONLY_SHARED} storage are not
     * counted towards live replicas.
     *
     * <p>
     * In certain implementations, a {@link #READ_ONLY_SHARED} storage may be
     * correlated to its {@link #NORMAL} counterpart using the
     * {@link DatanodeStorage#storageID}.  This property should be used for
     * debugging purposes only.
     * </p>
     */
    // 只读共享状态。
    // 通常用于特殊的共享存储设备。存放在此类存储上的副本不计入 HDFS 的副本总数（因为它是共享的，不能提供独立的冗余）。
    READ_ONLY_SHARED,
    // 故障状态。
    // 表示该磁盘或目录发生 IO 错误，已不可用。
    FAILED
  }
  // 存储介质的唯一标识符。通常以 DS- 开头。
  // 它是持久化的，即使 DataNode 重启，该 ID 通常也会保持不变，以便 NameNode 识别。
  private final String storageID;
  // 当前存储的状态
  private final State state;
  // 存储介质的类型（由 org.apache.hadoop.fs.StorageType 定义，如 DISK, SSD, ARCHIVE, RAM_DISK）。这对于 HDFS 的分层存储策略（Storage Policy）至关重要。
  private final StorageType storageType;
  // 定义了所有 StorageID 的统一前缀。
  private static final String STORAGE_ID_PREFIX = "DS-";

  /**
   * Create a storage with {@link State#NORMAL} and {@link StorageType#DEFAULT}.
   */
  public DatanodeStorage(String storageID) {
    this(storageID, State.NORMAL, StorageType.DEFAULT);
  }

  public DatanodeStorage(String sid, State s, StorageType sm) {
    this.storageID = sid;
    this.state = s;
    this.storageType = sm;
  }

  public String getStorageID() {
    return storageID;
  }

  public State getState() {
    return state;
  }

  public StorageType getStorageType() {
    return storageType;
  }

  /**
   * Generate new storage ID. The format of this string can be changed
   * in the future without requiring that old storage IDs be updated.
   *
   * @return unique storage ID
   */
  //生成新的存储ID
  public static String generateUuid() {
    return STORAGE_ID_PREFIX + UUID.randomUUID();
  }

  /**
   * Verify that a given string is a storage ID in the "DS-..uuid.." format.
   */
  public static boolean isValidStorageId(final String storageID) {
    try {
      // Attempt to parse the UUID.
      if (storageID != null && storageID.indexOf(STORAGE_ID_PREFIX) == 0) {
        UUID.fromString(storageID.substring(STORAGE_ID_PREFIX.length()));
        return true;
      }
    } catch (IllegalArgumentException ignored) {
    }

    return false;
  }

  @Override
  public String toString() {
    return "DatanodeStorage["+ storageID + "," + storageType + "," + state +"]";
  }

  @Override
  public boolean equals(Object other){
    if (other == this) {
      return true;
    }

    if ((other == null) ||
        !(other instanceof DatanodeStorage)) {
      return false;
    }
    DatanodeStorage otherStorage = (DatanodeStorage) other;
    return otherStorage.getStorageID().compareTo(getStorageID()) == 0;
  }

  @Override
  public int hashCode() {
    return getStorageID().hashCode();
  }
}
