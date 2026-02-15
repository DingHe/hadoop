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

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;

/**
 * An enumeration of logs available on a remote NameNode.
 */
//主要用于表示远程 NameNode 上可用的 RemoteEditLog 日志的清单。这个类提供了一个日志列表，并记录了最后提交的事务 ID (committedTxnId)。
// 它的主要作用是在 HDFS 的 NameNode 之间同步编辑日志（Edit Log）时，提供一个清晰的日志清单，确保日志数据的一致性和正确性
public class RemoteEditLogManifest {
  //存储 RemoteEditLog 对象的列表，每个 RemoteEditLog 代表一段事务日志（事务 ID 范围）
  private List<RemoteEditLog> logs;
  //记录已提交的最大事务 ID，表示 NameNode 已经确保持久化到磁盘的事务 ID
  private long committedTxnId = HdfsServerConstants.INVALID_TXID;

  public RemoteEditLogManifest() {
  }

  public RemoteEditLogManifest(List<RemoteEditLog> logs) {
    this(logs, HdfsServerConstants.INVALID_TXID);
  }

  public RemoteEditLogManifest(List<RemoteEditLog> logs, long committedTxnId) {
    this.logs = logs;
    this.committedTxnId = committedTxnId;
    checkState();
  }
  
  
  /**
   * Check that the logs are non-overlapping sequences of transactions,
   * in sorted order. They do not need to be contiguous.
   * @throws IllegalStateException if incorrect
   */
  //确保 logs 按照事务 ID 递增排列，并且日志之间不能有重叠
  private void checkState()  {
    Preconditions.checkNotNull(logs);

    RemoteEditLog prev = null;
    for (RemoteEditLog log : logs) {
      if (prev != null) {
        if (log.getStartTxId() <= prev.getEndTxId()) {
          throw new IllegalStateException(
              "Invalid log manifest (log " + log + " overlaps " + prev + ")\n"
              + this);
        }
      }
      prev = log;
    }
  }
  
  public List<RemoteEditLog> getLogs() {
    return Collections.unmodifiableList(logs);
  }

  public long getCommittedTxnId() {
    return committedTxnId;
  }

  @Override
  public String toString() {
    return "[" + Joiner.on(", ").join(logs) + "]" + " CommittedTxId: "
        + committedTxnId;
  }
}
