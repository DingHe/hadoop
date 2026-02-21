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
package org.apache.hadoop.hdfs.util;

/** Read-write lock interface. */
// RwLock 接口的核心作用是抽象读写分离的并发控制机制。
// 在 HDFS（特别是 NameNode）中，元数据的访问非常频繁。如果使用简单的互斥锁（Mutex），同一时间只能有一个线程访问，性能会非常差。RwLock 允许：
// 多线程并发读：多个客户端可以同时查询文件信息，互不阻塞。
// 单线程独占写：当有修改目录结构或创建文件的操作时，必须独占锁，确保数据一致性。
// 标准化实现：它为 HDFS 内部不同的组件（如 FSNamesystem）提供了一个统一的锁操作协议，方便进行性能监控和日志记录（例如通过 opName 记录哪个操作占用了锁）。
public interface RwLock {
  /** Acquire read lock. */
  // 获取读锁。
  // 如果写锁未被占用，当前线程会立即获得读锁并返回；如果写锁已被其他线程占用，当前线程将阻塞（等待），直到写锁释放。
  public void readLock();

  /** Acquire read lock, unless interrupted while waiting  */
  // 获取可中断的读锁。
  // 与 readLock 类似，但如果在等待获取锁的过程中，当前线程收到了中断信号（Interrupt），它会抛出异常而不是继续等待。这在处理超时或系统关闭时非常有用。
  void readLockInterruptibly() throws InterruptedException;

  /** Release read lock. */
  // 释放读锁。
  // 表示当前线程已完成读取操作，减少读锁的计数。
  public void readUnlock();

  /**
   * Release read lock with operation name.
   * @param opName Option name.
   */
  // 释放读锁，并关联具体的操作名称。
  // 功能同上，但参数 opName 常用于性能度量（Metrics）或审计日志，帮助开发者分析是哪个具体业务操作（如 ls, getFileInfo）释放了锁。
  public void readUnlock(String opName);

  /** Check if the current thread holds read lock. */
  // 检查当前线程是否持有读锁。
  public boolean hasReadLock();

  /** Acquire write lock. */
  // 获取写锁。
  // 如果没有任何线程持有读锁或写锁，则当前线程获取成功。否则，线程进入阻塞状态，直到所有锁被释放。
  public void writeLock();
  
  /** Acquire write lock, unless interrupted while waiting  */
  // 获取可中断的写锁。
  // 在等待写锁期间，如果线程被中断，则停止等待并抛出 InterruptedException。
  void writeLockInterruptibly() throws InterruptedException;

  /** Release write lock. */
  // 释放当前的独占访问权，允许其他等待读或写的线程获取锁。
  public void writeUnlock();

  /**
   * Release write lock with operation name.
   * @param opName Option name.
   */
  // 释放写锁，并关联操作名称。
  // 用于监控。在 HDFS 中，写锁通常比较“重”，通过 opName 可以追踪是哪个修改操作（如 mkdir, delete）完成了事务。
  public void writeUnlock(String opName);

  /** Check if the current thread holds write lock. */
  // 检查当前线程是否持有写锁。
  // 如果当前线程持有写锁，返回 true。这对于防止重入死锁或验证修改权限非常关键。
  public boolean hasWriteLock();
}
