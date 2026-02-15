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
import java.util.Collection;

/**
 * Interface used to abstract over classes which manage edit logs that may need
 * to be purged.
 */
//抽象化管理编辑日志的操作，尤其是与日志清理和流选择相关的操作
interface LogsPurgeable {
  
  /**
   * Remove all edit logs with transaction IDs lower than the given transaction
   * ID.
   * 
   * @param minTxIdToKeep the lowest transaction ID that should be retained
   * @throws IOException in the event of error
   */
  //minTxIdToKeep：表示要保留的最小事务 ID。所有小于此 ID 的编辑日志将会被清除
  //作用是删除所有事务 ID 小于 minTxIdToKeep 的编辑日志。
  // 通常情况下，日志在事务提交并持久化后可以被清理，因此该方法允许在给定事务 ID 之后的日志保留其历史记录，而将更早的日志删除
  public void purgeLogsOlderThan(long minTxIdToKeep) throws IOException;
  
  /**
   * Get a list of edit log input streams.  The list will start with the
   * stream that contains fromTxnId, and continue until the end of the journal
   * being managed.
   * 
   * @param fromTxId the first transaction id we want to read
   * @param inProgressOk whether or not in-progress streams should be returned
   * @param onlyDurableTxns whether or not streams should be bounded by durable
   *                        TxId. A durable TxId is the committed txid in QJM
   *                        or the largest txid written into file in FJM
   * @throws IOException if the underlying storage has an error or is otherwise
   * inaccessible
   */
  //streams：用于存储结果的集合，方法执行后，将该集合填充为从指定事务 ID 开始的编辑日志输入流
  //fromTxId：表示读取编辑日志的起始事务 ID。即从此事务 ID 对应的日志开始读取
  //inProgressOk：标志位，表示是否允许返回处于进行中的（未完成的）日志流。如果为 true，即允许返回仍在进行中的日志流；如果为 false，则只返回已完成的日志流
  //onlyDurableTxns：标志位，表示是否仅返回包含持久化事务的日志流。持久化事务是指在 QJM（Quorum Journal Manager）中提交的事务或在 FJM（File Journal Manager）中写入的最大事务 ID
  //作用是获取一系列编辑日志输入流。输入流将从指定的 fromTxId 开始，直到日志管理系统中的最后一个日志为止
  void selectInputStreams(Collection<EditLogInputStream> streams,
      long fromTxId, boolean inProgressOk, boolean onlyDurableTxns)
      throws IOException;
  
}
