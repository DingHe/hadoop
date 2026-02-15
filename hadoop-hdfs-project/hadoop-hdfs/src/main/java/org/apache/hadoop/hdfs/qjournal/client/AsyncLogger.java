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
package org.apache.hadoop.hdfs.qjournal.client;

import java.net.InetSocketAddress;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocol;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournaledEditsResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournalStateResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PrepareRecoveryResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.SegmentStateProto;
import org.apache.hadoop.hdfs.qjournal.protocol.RequestInfo;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListenableFuture;

/**
 * Interface for a remote log which is only communicated with asynchronously.
 * This is essentially a wrapper around {@link QJournalProtocol} with the key
 * differences being:
 * 
 * <ul>
 * <li>All methods return {@link ListenableFuture}s instead of synchronous
 * objects.</li>
 * <li>The {@link RequestInfo} objects are created by the underlying
 * implementation.</li>
 * </ul>
 */
//用于异步与远程JournalNode进行交互的接口，主要围绕 JournalNode 的日志管理进行操作，封装了对 QJournalProtocol 接口的异步调用。该接口的主要作用包括：
//异步写入事务日志：将 HDFS 的编辑日志（edits）发送到远程节点。
//日志段管理：支持日志段的开始、完成和清理操作。
//恢复与故障处理：提供日志恢复、数据回滚等功能，保证 HDFS 在崩溃时的数据完整性和一致性。
//日志元数据查询：获取远程节点的日志状态、已提交事务 ID、可用日志段等信息。
//升级和回滚：支持 HDFS 升级和回滚操作，确保数据兼容性和系统平稳升级
interface AsyncLogger {
  
  interface Factory {
    AsyncLogger createLogger(Configuration conf, NamespaceInfo nsInfo,
        String journalId, String nameServiceId, InetSocketAddress addr);
  }

  /**
   * Send a batch of edits to the logger.
   * @param segmentTxId the first txid in the current segment
   * @param firstTxnId the first txid of the edits.
   * @param numTxns the number of transactions in the batch
   * @param data the actual data to be sent
   */
  //向远程 JournalNode 发送一批编辑日志，保证事务的持久化
  //segmentTxId：当前日志段的起始事务 ID，用于标识一个日志文件
  //firstTxnId：本次批量事务的第一个事务 ID，确保事务的有序性和一致性
  //numTxns：事务数量，表示本次批量操作中包含多少个事务
  //data：编辑日志的二进制数据，通常是 HDFS 操作的序列化结果
  public ListenableFuture<Void> sendEdits(
      final long segmentTxId, final long firstTxnId,
      final int numTxns, final byte[] data);

  /**
   * Begin writing a new log segment.
   * 
   * @param txid the first txid to be written to the new log
   * @param layoutVersion the LayoutVersion of the log
   */
  //在 JournalNode 上启动一个新的日志段，通常在 HDFS 开始新的写入周期时调用
  //txid：新日志段的第一个事务 ID，表示新日志的起点
  //layoutVersion：HDFS 的布局版本，确保数据格式与 JournalNode 兼容
  public ListenableFuture<Void> startLogSegment(long txid, int layoutVersion);

  /**
   * Finalize a log segment.
   * 
   * @param startTxId the first txid that was written to the segment
   * @param endTxId the last txid that was written to the segment
   */
  //在 JournalNode 上完成一个日志段，确保事务已持久化并标记为不可修改
  //startTxId：该日志段的起始事务 ID
  //endTxId：该日志段的结束事务 ID
  public ListenableFuture<Void> finalizeLogSegment(
      long startTxId, long endTxId);

  /**
   * Allow the remote node to purge edit logs earlier than this.
   * @param minTxIdToKeep the min txid which must be retained
   */
  //删除 JournalNode 上比 minTxIdToKeep 旧的日志段，节省存储空间
  public ListenableFuture<Void> purgeLogsOlderThan(long minTxIdToKeep);

  /**
   * Format the log directory.
   * @param nsInfo the namespace info to format with
   * @param force the force option to format
   */
  //格式化远程 JournalNode，清空所有日志数据
  public ListenableFuture<Void> format(NamespaceInfo nsInfo, boolean force);

  /**
   * @return whether or not the remote node has any valid data.
   */
  //检查 JournalNode 是否已经格式化，是否具备有效数据
  public ListenableFuture<Boolean> isFormatted();
  
  /**
   * @return the state of the last epoch on the target node.
   */
  public ListenableFuture<GetJournalStateResponseProto> getJournalState();

  /**
   * Begin a new epoch on the target node.
   */
  public ListenableFuture<NewEpochResponseProto> newEpoch(long epoch);

  /**
   * Fetch journaled edits from the cache.
   */
  public ListenableFuture<GetJournaledEditsResponseProto> getJournaledEdits(
      long fromTxnId, int maxTransactions);
  
  /**
   * Fetch the list of edit logs available on the remote node.
   */
  public ListenableFuture<RemoteEditLogManifest> getEditLogManifest(
      long fromTxnId, boolean inProgressOk);

  /**
   * Prepare recovery. See the HDFS-3077 design document for details.
   */
  public ListenableFuture<PrepareRecoveryResponseProto> prepareRecovery(
      long segmentTxId);

  /**
   * Accept a recovery proposal. See the HDFS-3077 design document for details.
   */
  public ListenableFuture<Void> acceptRecovery(SegmentStateProto log,
      URL fromUrl);

  /**
   * Set the epoch number used for all future calls.
   */
  public void setEpoch(long e);

  /**
   * Let the logger know the highest committed txid across all loggers in the
   * set. This txid may be higher than the last committed txid for <em>this</em>
   * logger. See HDFS-3863 for details.
   */
  public void setCommittedTxId(long txid);

  /**
   * Build an HTTP URL to fetch the log segment with the given startTxId.
   */
  public URL buildURLToFetchLogs(long segmentTxId);
  
  /**
   * Tear down any resources, connections, etc. The proxy may not be used
   * after this point, and any in-flight RPCs may throw an exception.
   */
  public void close();

  /**
   * Append an HTML-formatted report for this logger's status to the provided
   * StringBuilder. This is displayed on the NN web UI.
   */
  public void appendReport(StringBuilder sb);

  public ListenableFuture<Void> doPreUpgrade();

  public ListenableFuture<Void> doUpgrade(StorageInfo sInfo);

  public ListenableFuture<Void> doFinalize();

  public ListenableFuture<Boolean> canRollBack(StorageInfo storage,
      StorageInfo prevStorage, int targetLayoutVersion);

  public ListenableFuture<Void> doRollback();

  public ListenableFuture<Void> discardSegments(long startTxId);

  public ListenableFuture<Long> getJournalCTime();
}
