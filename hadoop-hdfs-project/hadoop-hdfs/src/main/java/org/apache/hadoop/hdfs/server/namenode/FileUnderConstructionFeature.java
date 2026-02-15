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

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;

/** 用于处理 HDFS 中正在建造（under-construction）的文件的相关功能。
 * 特别地，它涉及到文件的最后一个块（block）在客户端操作时的状态更新和清理
 * Feature for under-construction file.
 */
@InterfaceAudience.Private
public class FileUnderConstructionFeature implements INode.Feature {
  //这个属性表示当前持有文件租约（lease）的客户端的名字。
  // 客户端持有租约时，文件处于"under-construction"（正在建造）状态，客户端会进行写入操作
  private String clientName; // lease holder
  //这个属性表示持有文件租约的客户端所在的机器名称。它用于标识哪个机器正在写入文件
  private final String clientMachine;

  public FileUnderConstructionFeature(final String clientName, final String clientMachine) {
    this.clientName = clientName;
    this.clientMachine = clientMachine;
  }

  public String getClientName() {
    return clientName;
  }

  void setClientName(String clientName) {
    this.clientName = clientName;
  }

  public String getClientMachine() {
    return clientMachine;
  }

  /**
   * Update the length for the last block
   *
   * @param lastBlockLength
   *          The length of the last block reported from client
   * @throws IOException
   */
  //f：INodeFile 类型，表示文件节点
  //astBlockLength：long，表示客户端报告的最后一个块的长度
  //更新文件最后一个正在写的数据块的长度
  void updateLengthOfLastBlock(INodeFile f, long lastBlockLength)
      throws IOException {
    BlockInfo lastBlock = f.getLastBlock();//获取文件的最后一个块
    assert (lastBlock != null) : "The last block for path "
        + f.getFullPathName() + " is null when updating its length";
    assert !lastBlock.isComplete()
        : "The last block for path " + f.getFullPathName()
            + " is not under-construction when updating its length";
    lastBlock.setNumBytes(lastBlockLength); //更新块的大小
  }

  /**
   * When deleting a file in the current fs directory, and the file is contained
   * in a snapshot, we should delete the last block if it's under construction
   * and its size is 0.
   */
  //f：INodeFile 类型，表示文件节点。
  //collectedBlocks：BlocksMapUpdateInfo 类型，用于收集需要删除的块信息
  //删除快照中文件的最后一个长度为0的数据块
  void cleanZeroSizeBlock(final INodeFile f,
      final BlocksMapUpdateInfo collectedBlocks) {
    final BlockInfo[] blocks = f.getBlocks();
    if (blocks != null && blocks.length > 0
        && !blocks[blocks.length - 1].isComplete()) {
      BlockInfo lastUC = blocks[blocks.length - 1];
      if (lastUC.getNumBytes() == 0) {
        // this is a 0-sized block. do not need check its UC state here
        collectedBlocks.addDeleteBlock(lastUC);
        f.removeLastBlock(lastUC);
      }
    }
  }
}
