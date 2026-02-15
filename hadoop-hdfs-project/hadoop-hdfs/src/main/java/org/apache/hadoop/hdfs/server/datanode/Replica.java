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
package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;

/** 表示存储在 DataNode 上的块副本
 * This represents block replicas which are stored in DataNode.
 */
@InterfaceAudience.Private
public interface Replica {
  /** Get the block ID  该副本对应的块的 ID（long 类型）*/
  public long getBlockId();

  /** Get the generation stamp 该副本的生成戳（long 类型）*/
  public long getGenerationStamp();

  /**返回副本的状态（ReplicaState 枚举类型）
   * Get the replica state
   * @return the replica state
   */
  public ReplicaState getState();

  /**返回已接收到的字节数（long 类型）
   * Get the number of bytes received
   * @return the number of bytes that have been received
   */
  public long getNumBytes();
  
  /**返回已写入磁盘的字节数（long 类型）
   * Get the number of bytes that have written to disk
   * @return the number of bytes that have written to disk
   */
  public long getBytesOnDisk();

  /**返回对读者可见的字节数（long 类型）
   * Get the number of bytes that are visible to readers
   * @return the number of bytes that are visible to readers
   */
  public long getVisibleLength();

  /**返回存储副本的卷的 UUID（String 类型）
   * Return the storageUuid of the volume that stores this replica.
   */
  public String getStorageUuid();

  /**返回副本是否存储在临时存储（boolean 类型）
   * Return true if the target volume is backed by RAM.
   */
  public boolean isOnTransientStorage();

  /**返回存储副本的卷（FsVolumeSpi 类型）
   * Get the volume of replica.
   * @return the volume of replica
   */
  public FsVolumeSpi getVolume();
}
