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

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.Storage.FormatConfirmable;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;

/**
 * A JournalManager is responsible for managing a single place of storing
 * edit logs. It may correspond to multiple files, a backup node, etc.
 * Even when the actual underlying storage is rolled, or failed and restored,
 * each conceptual place of storage corresponds to exactly one instance of
 * this class, which is created when the EditLog is first opened.
 */
//主要通过方法来管理编辑日志
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface JournalManager extends Closeable, FormatConfirmable,
    LogsPurgeable {

  /**
   * Format the underlying storage, removing any previously
   * stored data.
   */
  //NamespaceInfo ns：包含名称空间信息，通常包括集群的标识符、版本号等信息
  //boolean force：强制格式化标志。如果为 true，即使存储中已有数据，也会强制进行格式化
  //格式化底层存储，删除之前存储的任何数据。
  // 它通常在 NameNode 格式化过程中调用，以确保存储在格式化操作之前被清空。
  // force 参数用于强制执行格式化操作，忽略存储中可能存在的任何数据
  void format(NamespaceInfo ns, boolean force) throws IOException;

  /**
   * Begin writing to a new segment of the log stream, which starts at
   * the given transaction ID.
   */
  //long txId：开始的事务 ID，用于标识日志段的起始位置
  //int layoutVersion：布局版本，用于指示日志的格式版本
  //开始写入一个新的日志段，日志段的起始事务 ID 为 txId。它返回一个 EditLogOutputStream 对象，表示新的日志段输出流，用于写入日志数据
  EditLogOutputStream startLogSegment(long txId, int layoutVersion)
      throws IOException;

  /**
   * Mark the log segment that spans from firstTxId to lastTxId
   * as finalized and complete.
   */
  //long firstTxId：日志段的第一个事务 ID
  //long lastTxId：日志段的最后一个事务 ID
  //标记一个日志段为已完成，即日志段的事务 ID 从 firstTxId 到 lastTxId 之间的所有事务已经写入并确认。它通常用于完成日志的提交
  void finalizeLogSegment(long firstTxId, long lastTxId) throws IOException;

  /**
   * Set the amount of memory that this stream should use to buffer edits
   */
  //int size：设置日志输出缓冲区的大小
  //该方法设置日志输出流的缓冲区大小。调整缓冲区大小有助于优化写入性能，特别是在高事务量的情况下
  void setOutputBufferCapacity(int size);

  /**
   * Recover segments which have not been finalized.
   */
  //用于恢复未完成的日志段。如果有日志段因故障等原因未被最终化，调用该方法将尝试恢复这些日志段
  void recoverUnfinalizedSegments() throws IOException;
  
  /**
   * Perform any steps that must succeed across all JournalManagers involved in
   * an upgrade before proceeding onto the actual upgrade stage. If a call to
   * any JM's doPreUpgrade method fails, then doUpgrade will not be called for
   * any JM.
   */
  //在升级过程中，所有涉及的 JournalManager 必须先执行这个步骤。
  // 它用于执行在实际升级之前的所有操作。如果某个 JournalManager 执行失败，升级操作会中止
  void doPreUpgrade() throws IOException;
  
  /**
   * Perform the actual upgrade of the JM. After this is completed, the NN can
   * begin to use the new upgraded metadata. This metadata may later be either
   * finalized or rolled back to the previous state.
   * 
   * @param storage info about the new upgraded versions.
   */
  //Storage storage：包含新的升级版本的存储信息
  //执行实际的升级操作，将 JournalManager 升级到新版本。升级完成后，NameNode 可以开始使用新的元数据
  void doUpgrade(Storage storage) throws IOException;
  
  /**
   * Finalize the upgrade. JMs should purge any state that they had been keeping
   * around during the upgrade process. After this is completed, rollback is no
   * longer allowed.
   */
  //完成升级过程，并清除在升级过程中保留的任何状态。升级完成后，系统将不再允许回滚操作
  void doFinalize() throws IOException;
  
  /**
   * Return true if this JM can roll back to the previous storage state, false
   * otherwise. The NN will refuse to run the rollback operation unless at least
   * one JM or fsimage storage directory can roll back.
   * 
   * @param storage the storage info for the current state 当前存储的状态信息
   * @param prevStorage the storage info for the previous (unupgraded) state  上一个存储的状态信息（即未升级的状态）
   * @param targetLayoutVersion the layout version we intend to roll back to  目标布局版本
   * @return true if this JM can roll back, false otherwise. 如果 JournalManager 可以回滚到之前的存储状态，返回 true，否则返回 false
   */
  //法检查 JournalManager 是否可以回滚到上一个存储状态。它会根据目标布局版本、当前存储状态和上一个存储状态进行验证
  boolean canRollBack(StorageInfo storage, StorageInfo prevStorage,
      int targetLayoutVersion) throws IOException;
  
  /**
   * Perform the rollback to the previous FS state. JMs which do not need to
   * roll back their state should just return without error.
   */
  //执行回滚操作，将 JournalManager 回滚到上一个 FS 状态。某些 JournalManager 可能不需要回滚，因此可以为空实现
  void doRollback() throws IOException;

  /**
   * Discard the segments whose first txid is {@literal >=} the given txid.
   * @param startTxId The given txid should be right at the segment boundary, 
   * i.e., it should be the first txid of some segment, if segment corresponding
   * to the txid exists.
   */
  //long startTxId：开始的事务 ID
  //丢弃所有事务 ID 大于或等于 startTxId 的日志段。通常用于清理过时或无用的日志数据
  void discardSegments(long startTxId) throws IOException;

  /**
   * @return the CTime of the journal manager.
   */
  //返回日志管理器的创建时间。它通常用于了解日志的历史时间信息
  long getJournalCTime() throws IOException;

  /**
   * Close the journal manager, freeing any resources it may hold.
   */
  @Override
  void close() throws IOException;
  
  /** 
   * Indicate that a journal is cannot be used to load a certain range of 
   * edits.
   * This exception occurs in the case of a gap in the transactions, or a
   * corrupt edit file.
   */
  public static class CorruptionException extends IOException {
    static final long serialVersionUID = -4687802717006172702L;
    
    public CorruptionException(String reason) {
      super(reason);
    }
  }

}
