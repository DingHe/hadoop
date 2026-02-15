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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;

import java.io.Closeable;
import java.io.IOException;

/**
 * A generic abstract class to support reading edits log data from 
 * persistent storage.
 * 
 * It should stream bytes from the storage exactly as they were written
 * into the #{@link EditLogOutputStream}.
 */
// 用于从持久化存储（如磁盘或其他介质）读取编辑日志（Edit Log）的抽象类。
// HDFS 使用 Edit Log 来记录文件系统的元数据更改（如文件创建、删除、权限修改等操作）。
// 该类的主要职责是提供读取 Edit Log 的通用接口，子类根据具体存储介质实现读取逻辑
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class EditLogInputStream implements Closeable {
  //缓存最近读取但未消费的操作（Operation）。当需要再次读取时，优先返回缓存值，避免多次 I/O 操作，提高性能
  private FSEditLogOp cachedOp = null; 

  /**
   * Returns the name of the currently active underlying stream.  The default
   * implementation returns the same value as getName unless overridden by the
   * subclass.
   *  String，返回当前活动的日志流名称
   * @return String name of the currently active underlying stream
   */
  public String getCurrentStreamName() {
    return getName();
  }

  /**  当前 EditLogInputStream 对象的名称
   * @return the name of the EditLogInputStream
   */
  public abstract String getName();
  
  /**  子类实现该方法，返回此输入流中第一个有效事务的 ID，帮助定位日志起始位置
   * @return the first transaction which will be found in this stream
   */
  public abstract long getFirstTxId();
  
  /**  子类实现该方法，返回该流中最后一个有效事务的 ID，帮助确定日志结束位置
   * @return the last transaction which will be found in this stream
   */
  public abstract long getLastTxId();


  /** 子类需要实现具体的资源关闭逻辑（如关闭文件或网络连接）
   * Close the stream.
   * @throws IOException if an error occurred while closing
   */
  @Override
  public abstract void close() throws IOException;

  /** 
   * Read an operation from the stream
   * @return an operation from the stream or null if at end of stream
   * @throws IOException if there is an error reading from the stream
   */
  //如果 cachedOp 不为空，返回缓存的操作，并清空缓存
  //如果缓存为空，调用 nextOp() 方法读取下一个操作
  public FSEditLogOp readOp() throws IOException {
    FSEditLogOp ret;
    if (cachedOp != null) {
      ret = cachedOp;
      cachedOp = null;
      return ret;
    }
    return nextOp();
  }
  
  /** 
   * Position the stream so that a valid operation can be read from it with
   * readOp().
   *
   * This method can be used to skip over corrupted sections of edit logs.
   */
  //如果缓存已有有效操作，直接返回
  //否则，调用 nextValidOp() 方法，跳过损坏数据，重新同步流
  public void resync() {
    if (cachedOp != null) {
      return;
    }
    cachedOp = nextValidOp();
  }
  
  /** 
   * Get the next operation from the stream storage.
   * 
   * @return an operation from the stream or null if at end of stream
   * @throws IOException if there is an error reading from the stream
   */
  //返回下一个操作，若到达末尾返回 null
  protected abstract FSEditLogOp nextOp() throws IOException;

  /**
   * Go through the next operation from the stream storage.
   * @return the txid of the next operation.
   */
  //返回下一个操作的事务 ID，若无更多操作返回
  protected long scanNextOp() throws IOException {
    FSEditLogOp next = readOp();
    return next != null ? next.txid : HdfsServerConstants.INVALID_TXID;
  }
  
  /** 
   * Get the next valid operation from the stream storage.
   * 
   * This is exactly like nextOp, except that we attempt to skip over damaged
   * parts of the edit log
   * 
   * @return an operation from the stream or null if at end of stream
   */
  protected FSEditLogOp nextValidOp() {
    // This is a trivial implementation which just assumes that any errors mean
    // that there is nothing more of value in the log.  Subclasses that support
    // error recovery will want to override this.
    try {
      return nextOp();
    } catch (Throwable e) {
      return null;
    }
  }
  
  /** 
   * Skip edit log operations up to a given transaction ID, or until the
   * end of the edit log is reached.
   *
   * After this function returns, the next call to readOp will return either
   * end-of-file (null) or a transaction with a txid equal to or higher than
   * the one we asked for.
   *
   * @param txid    The transaction ID to read up until.
   * @return        Returns true if we found a transaction ID greater than
   *                or equal to 'txid' in the log.
   */
  //true：找到大于或等于目标事务 ID 的操作
  //false：到达日志末尾，未找到目标事务 ID
  public boolean skipUntil(long txid) throws IOException {
    while (true) {
      FSEditLogOp op = readOp();
      if (op == null) {
        return false;
      }
      if (op.getTransactionId() >= txid) {
        cachedOp = op;
        return true;
      }
    }
  }

  /** FSEditLogOp，返回缓存的操作，并清空缓存
   * return the cachedOp, and reset it to null. 
   */
  FSEditLogOp getCachedOp() {
    FSEditLogOp op = this.cachedOp;
    cachedOp = null;
    return op;
  }
  
  /** 
   * Get the layout version of the data in the stream.
   * @return the layout version of the ops in the stream.
   * @throws IOException if there is an error reading the version
   */
  //返回日志的布局版本（Layout Version）
  public abstract int getVersion(boolean verifyVersion) throws IOException;

  /**
   * Get the "position" of in the stream. This is useful for 
   * debugging and operational purposes.
   *
   * Different stream types can have a different meaning for 
   * what the position is. For file streams it means the byte offset
   * from the start of the file.
   *
   * @return the position in the stream
   */
  //long，返回当前流的位置（例如文件偏移量）
  public abstract long getPosition();

  /**
   * Return the size of the current edits log or -1 if unknown.
   * long，返回当前编辑日志的总长度，未知时返回 -1
   * @return long size of the current edits log or -1 if unknown
   */
  public abstract long length() throws IOException;
  
  /** boolean，当前日志是否处于写入状态
   * Return true if this stream is in progress, false if it is finalized.
   */
  public abstract boolean isInProgress();
  
  /** maxOpSize：设置最大操作字节数
   * Set the maximum opcode size in bytes.
   */
  public abstract void setMaxOpSize(int maxOpSize);

  /** boolean，当前流是否来自本地磁盘或更快的源
   * Returns true if we are currently reading the log from a local disk or an
   * even faster data source (e.g. a byte buffer).
   */
  public abstract boolean isLocalLog();

  @Override
  public String toString() {
    return getName();
  }
}
