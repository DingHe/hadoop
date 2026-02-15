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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;

/**
 * Interface responsible for inspecting a set of storage directories and devising
 * a plan to load the namespace from them.
 */
//主要用于检查 Apache HDFS 中 NameNode 的存储目录，并制定从这些目录中加载命名空间（Namespace）的计划。
// 它负责解析和验证存储目录中的 fsimage 文件，确保系统可以从这些文件中恢复文件系统的元数据
@InterfaceAudience.Private
@InterfaceStability.Unstable
abstract class FSImageStorageInspector {
  /**
   * Inspect the contents of the given storage directory.
   */
  //主要用于解析指定存储目录中的 fsimage 文件，提取其路径、事务 ID、是否有效等信息
  abstract void inspectDirectory(StorageDirectory sd) throws IOException;

  /**
   * @return false if any of the storage directories have an unfinalized upgrade 
   */
  //检查所有被解析的存储目录，确认是否有未完成的升级
  abstract boolean isUpgradeFinalized();
  
  /**
   * Get the image files which should be loaded into the filesystem.
   * @throws IOException if not enough files are available (eg no image found in any directory)
   */
  //遍历检查的存储目录，找到最新的 fsimage 文件（通常按事务 ID 排序取最大值）
  abstract List<FSImageFile> getLatestImages() throws IOException;

  /** 
   * Get the minimum tx id which should be loaded with this set of images.
   */
  //统计已解析的 fsimage 文件中的最大事务 ID，确保加载的是最新的元数据
  abstract long getMaxSeenTxId();

  /**
   * @return true if the directories are in such a state that the image should be re-saved
   * following the load
   */
  //存储目录状态不一致、某些 fsimage 文件缺失或损坏、需要将多个事务合并为一个新的 fsimage 文件
  abstract boolean needToSave();

  /**
   * Record of an image that has been located and had its filename parsed.
   */
  static class FSImageFile {
    //该 fsimage 文件所在的存储目录
    final StorageDirectory sd;
    //文件对应的事务 ID（Checkpoint TxId）。用于区分不同快照的唯一标识
    final long txId;
    //具体的 fsimage 文件路径。
    private final File file;
    
    FSImageFile(StorageDirectory sd, File file, long txId) {
      assert txId >= 0 || txId == HdfsServerConstants.INVALID_TXID
        : "Invalid txid on " + file +": " + txId;
      
      this.sd = sd;
      this.txId = txId;
      this.file = file;
    } 
    
    File getFile() {
      return file;
    }

    public long getCheckpointTxId() {
      return txId;
    }
    
    @Override
    public String toString() {
      return String.format("FSImageFile(file=%s, cpktTxId=%019d)", 
                           file.toString(), txId);
    }
  }

}
