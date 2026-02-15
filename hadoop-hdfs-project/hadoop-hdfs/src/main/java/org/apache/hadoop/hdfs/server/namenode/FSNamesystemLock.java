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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.log.LogThrottlingHelper;
import org.apache.hadoop.metrics2.lib.MutableRatesWithAggregation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Timer;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_LOCK_SUPPRESS_WARNING_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_LOCK_SUPPRESS_WARNING_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_FSLOCK_FAIR_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_FSLOCK_FAIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LOCK_DETAILED_METRICS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LOCK_DETAILED_METRICS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_READ_LOCK_REPORTING_THRESHOLD_MS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_READ_LOCK_REPORTING_THRESHOLD_MS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_WRITE_LOCK_REPORTING_THRESHOLD_MS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_WRITE_LOCK_REPORTING_THRESHOLD_MS_KEY;
import static org.apache.hadoop.ipc.ProcessingDetails.Timing;
import static org.apache.hadoop.log.LogThrottlingHelper.LogAction;

/**
 * Mimics a ReentrantReadWriteLock but does not directly implement the interface
 * so more sophisticated locking capabilities and logging/metrics are possible.
 * {@link org.apache.hadoop.hdfs.DFSConfigKeys#DFS_NAMENODE_LOCK_DETAILED_METRICS_KEY}
 * to be true, metrics will be emitted into the FSNamesystem metrics registry
 * for each operation which acquires this lock indicating how long the operation
 * held the lock for. These metrics have names of the form
 * FSN(Read|Write)LockNanosOperationName, where OperationName denotes the name
 * of the operation that initiated the lock hold (this will be OTHER for certain
 * uncategorized operations) and they export the hold time values in
 * nanoseconds. Note that if a thread dies, metrics produced after the
 * most recent snapshot will be lost due to the use of
 * {@link MutableRatesWithAggregation}. However since threads are re-used
 * between operations this should not generally be an issue.
 */
//用于 FSNamesystem 的读写锁实现，确保对 NameNode 中核心数据结构的并发访问控制。
// 该类使用了 Java 的 ReentrantReadWriteLock 实现了细粒度锁，允许多个线程并发读取，但只有一个线程能写入，确保线程安全。其主要功能包括：
//读/写锁管理：为 HDFS 的核心数据结构提供线程安全的并发访问。
//锁性能监控：记录锁的持有时间、等待时间，支持详细的性能指标和超时警告。
//日志报告：当读写锁的持有时间超过设定阈值时，生成详细的日志记录并统计长时间持锁次数。
//锁公平性控制：支持根据配置选择锁是否是公平锁（FIFO 顺序获取）
class FSNamesystemLock {
  @VisibleForTesting
  //主读写锁，控制对 FSNamesystem 的访问
  protected ReentrantReadWriteLock coarseLock;
  //是否启用详细锁持有时间的监控。
  private volatile boolean metricsEnabled;
  //记录锁的详细持有时间统计信息。
  private final MutableRatesWithAggregation detailedHoldTimeMetrics;
  //提供时间测量功能，精确计算锁的持有时长。
  private final Timer timer;

  /**
   * Log statements about long lock hold times will not be produced more
   * frequently than this interval.
   */
  //控制锁警告日志的最小间隔，避免日志过多。
  private final long lockSuppressWarningIntervalMs;

  /** Threshold (ms) for long holding write lock report. */
  //写锁持有时间警告的阈值，超过此值则记录日志。
  private volatile long writeLockReportingThresholdMs;
  /** Last time stamp for write lock. Keep the longest one for multi-entrance.*/
  //记录写锁开始时间，单位纳秒。
  private long writeLockHeldTimeStampNanos;
  /** Frequency limiter used for reporting long write lock hold times. */
  //控制写锁超时日志的生成频率，防止日志泛滥。
  private final LogThrottlingHelper writeLockReportLogger;

  /** Threshold (ms) for long holding read lock report. */
  //读锁持有时间警告的阈值，超过此值则记录日志。
  private volatile long readLockReportingThresholdMs;
  /**
   * Last time stamp for read lock. Keep the longest one for
   * multi-entrance. This is ThreadLocal since there could be
   * many read locks held simultaneously.
   */
  //记录每个线程获取读锁的时间，单位纳秒。
  private final ThreadLocal<Long> readLockHeldTimeStampNanos =
      new ThreadLocal<Long>() {
        @Override
        public Long initialValue() {
          return Long.MAX_VALUE;
        }
      };
  //记录被抑制的读锁超时警告数量。
  private final AtomicInteger numReadLockWarningsSuppressed =
      new AtomicInteger(0);
  /** Time stamp (ms) of the last time a read lock report was written. */
  //上次读锁超时报告的时间戳，防止重复记录。
  private final AtomicLong timeStampOfLastReadLockReportMs = new AtomicLong(0);
  /**
   * The info (lock held time and stack trace) when longest time (ms) a read
   * lock was held since the last report.
   */
  //保存最长时间的读锁持有信息。
  private final AtomicReference<LockHeldInfo> longestReadLockHeldInfo =
      new AtomicReference<>(new LockHeldInfo());
  //保存最长时间的写锁持有信息。
  private LockHeldInfo longestWriteLockHeldInfo = new LockHeldInfo();
  /**
   * The number of time the read lock
   * has been held longer than the threshold.
   */
  //记录超过读锁阈值的次数。
  private final LongAdder numReadLockLongHold = new LongAdder();
  /**
   * The number of time the write lock
   * has been held for longer than the threshold.
   */
  //记录超过写锁阈值的次数。
  private final LongAdder numWriteLockLongHold = new LongAdder();

  @VisibleForTesting
  static final String OP_NAME_OTHER = "OTHER";
  private static final String READ_LOCK_METRIC_PREFIX = "FSNReadLock";
  private static final String WRITE_LOCK_METRIC_PREFIX = "FSNWriteLock";
  private static final String LOCK_METRIC_SUFFIX = "Nanos";

  private static final String OVERALL_METRIC_NAME = "Overall";

  FSNamesystemLock(Configuration conf,
      MutableRatesWithAggregation detailedHoldTimeMetrics) {
    this(conf, detailedHoldTimeMetrics, new Timer());
  }

  @VisibleForTesting
  FSNamesystemLock(Configuration conf,
      MutableRatesWithAggregation detailedHoldTimeMetrics, Timer timer) {
    boolean fair = conf.getBoolean(DFS_NAMENODE_FSLOCK_FAIR_KEY,
        DFS_NAMENODE_FSLOCK_FAIR_DEFAULT);
    FSNamesystem.LOG.info("fsLock is fair: " + fair);
    this.coarseLock = new ReentrantReadWriteLock(fair);
    this.timer = timer;

    this.writeLockReportingThresholdMs = conf.getLong(
        DFS_NAMENODE_WRITE_LOCK_REPORTING_THRESHOLD_MS_KEY,
        DFS_NAMENODE_WRITE_LOCK_REPORTING_THRESHOLD_MS_DEFAULT);
    this.readLockReportingThresholdMs = conf.getLong(
        DFS_NAMENODE_READ_LOCK_REPORTING_THRESHOLD_MS_KEY,
        DFS_NAMENODE_READ_LOCK_REPORTING_THRESHOLD_MS_DEFAULT);
    this.lockSuppressWarningIntervalMs = conf.getTimeDuration(
        DFS_LOCK_SUPPRESS_WARNING_INTERVAL_KEY,
        DFS_LOCK_SUPPRESS_WARNING_INTERVAL_DEFAULT, TimeUnit.MILLISECONDS);
    this.writeLockReportLogger =
        new LogThrottlingHelper(lockSuppressWarningIntervalMs);
    this.metricsEnabled = conf.getBoolean(
        DFS_NAMENODE_LOCK_DETAILED_METRICS_KEY,
        DFS_NAMENODE_LOCK_DETAILED_METRICS_DEFAULT);
    FSNamesystem.LOG.info("Detailed lock hold time metrics enabled: " +
        this.metricsEnabled);
    this.detailedHoldTimeMetrics = detailedHoldTimeMetrics;
  }
  //加读锁
  public void readLock() {
    doLock(false);
  }
  //可中断读锁
  public void readLockInterruptibly() throws InterruptedException {
    doLockInterruptibly(false);
  }

  public void readUnlock() {
    readUnlock(OP_NAME_OTHER, null);
  }

  public void readUnlock(String opName) {
    readUnlock(opName, null);
  }
  //用于释放读锁，并在锁持有时间超过阈值时，记录和报告相关信息，帮助监控和调试锁的使用情况
  //opName：执行操作的名称，通常用于标识当前锁保护的具体操作
  //lockReportInfoSupplier：一个 Supplier 对象，延迟生成额外的锁信息，提供更详细的上下文，只有在需要时才会执行
  public void readUnlock(String opName,
      Supplier<String> lockReportInfoSupplier) {
    final boolean needReport = coarseLock.getReadHoldCount() == 1;
    final long readLockIntervalNanos =
        timer.monotonicNowNanos() - readLockHeldTimeStampNanos.get();
    final long currentTimeMs = timer.now();
    coarseLock.readLock().unlock();

    if (needReport) {
      addMetric(opName, readLockIntervalNanos, false);
      readLockHeldTimeStampNanos.remove();
    }
    final long readLockIntervalMs =
        TimeUnit.NANOSECONDS.toMillis(readLockIntervalNanos);
    if (needReport && readLockIntervalMs >= this.readLockReportingThresholdMs) {
      numReadLockLongHold.increment();
      String lockReportInfo = null;
      boolean done = false;
      while (!done) {
        LockHeldInfo localLockHeldInfo = longestReadLockHeldInfo.get();
        if (localLockHeldInfo.getIntervalMs() <= readLockIntervalMs) {
          if (lockReportInfo == null) {
            lockReportInfo = lockReportInfoSupplier != null ? " (" +
                lockReportInfoSupplier.get() + ")" : "";
          }
          if (longestReadLockHeldInfo.compareAndSet(localLockHeldInfo,
              new LockHeldInfo(currentTimeMs, readLockIntervalMs,
              StringUtils.getStackTrace(Thread.currentThread()), opName,
              lockReportInfo))) {
            done = true;
          }
        } else {
          done = true;
        }
      }

      long localTimeStampOfLastReadLockReport;
      long nowMs;
      do {
        nowMs = timer.monotonicNow();
        localTimeStampOfLastReadLockReport =
            timeStampOfLastReadLockReportMs.get();
        if (nowMs - localTimeStampOfLastReadLockReport <
            lockSuppressWarningIntervalMs) {
          numReadLockWarningsSuppressed.incrementAndGet();
          return;
        }
      } while (!timeStampOfLastReadLockReportMs.compareAndSet(
          localTimeStampOfLastReadLockReport, nowMs));
      int numSuppressedWarnings = numReadLockWarningsSuppressed.getAndSet(0);
      LockHeldInfo lockHeldInfo =
          longestReadLockHeldInfo.getAndSet(new LockHeldInfo());
      FSNamesystem.LOG.info(
          "\tNumber of suppressed read-lock reports: {}"
              + "\n\tLongest read-lock held at {} for {}ms by {}{} via {}",
          numSuppressedWarnings, Time.formatTime(lockHeldInfo.getStartTimeMs()),
          lockHeldInfo.getIntervalMs(), lockHeldInfo.getOpName(),
          lockHeldInfo.getLockReportInfo(), lockHeldInfo.getStackTrace());
    }
  }
  
  public void writeLock() {
    doLock(true);
  }

  public void writeLockInterruptibly() throws InterruptedException {
    doLockInterruptibly(true);
  }

  /**
   * Unlocks FSNameSystem write lock. This internally calls {@link
   * FSNamesystemLock#writeUnlock(String, boolean, Supplier)}
   */
  public void writeUnlock() {
    writeUnlock(OP_NAME_OTHER, false, null);
  }

  /**
   * Unlocks FSNameSystem write lock. This internally calls {@link
   * FSNamesystemLock#writeUnlock(String, boolean, Supplier)}
   *
   * @param opName Operation name.
   */
  public void writeUnlock(String opName) {
    writeUnlock(opName, false, null);
  }

  /**
   * Unlocks FSNameSystem write lock. This internally calls {@link
   * FSNamesystemLock#writeUnlock(String, boolean, Supplier)}
   *
   * @param opName Operation name.
   * @param lockReportInfoSupplier The info shown in the lock report
   */
  public void writeUnlock(String opName,
      Supplier<String> lockReportInfoSupplier) {
    writeUnlock(opName, false, lockReportInfoSupplier);
  }

  /**
   * Unlocks FSNameSystem write lock. This internally calls {@link
   * FSNamesystemLock#writeUnlock(String, boolean, Supplier)}
   *
   * @param opName Operation name.
   * @param suppressWriteLockReport When false, event of write lock being held
   * for long time will be logged in logs and metrics.
   */
  public void writeUnlock(String opName, boolean suppressWriteLockReport) {
    writeUnlock(opName, suppressWriteLockReport, null);
  }

  /**
   * Unlocks FSNameSystem write lock.
   *
   * @param opName Operation name
   * @param suppressWriteLockReport When false, event of write lock being held
   * for long time will be logged in logs and metrics.
   * @param lockReportInfoSupplier The info shown in the lock report
   */
  private void writeUnlock(String opName, boolean suppressWriteLockReport,
      Supplier<String> lockReportInfoSupplier) {
    final boolean needReport = !suppressWriteLockReport && coarseLock
        .getWriteHoldCount() == 1 && coarseLock.isWriteLockedByCurrentThread();
    final long writeLockIntervalNanos =
        timer.monotonicNowNanos() - writeLockHeldTimeStampNanos;
    final long currentTimeMs = timer.now();
    final long writeLockIntervalMs =
        TimeUnit.NANOSECONDS.toMillis(writeLockIntervalNanos);

    LogAction logAction = LogThrottlingHelper.DO_NOT_LOG;
    if (needReport &&
        writeLockIntervalMs >= this.writeLockReportingThresholdMs) {
      numWriteLockLongHold.increment();
      if (longestWriteLockHeldInfo.getIntervalMs() <= writeLockIntervalMs) {
        String lockReportInfo = lockReportInfoSupplier != null ? " (" +
            lockReportInfoSupplier.get() + ")" : "";
        longestWriteLockHeldInfo = new LockHeldInfo(currentTimeMs,
            writeLockIntervalMs,
            StringUtils.getStackTrace(Thread.currentThread()), opName,
            lockReportInfo);
      }

      logAction = writeLockReportLogger
          .record("write", currentTimeMs, writeLockIntervalMs);
    }

    LockHeldInfo lockHeldInfo = longestWriteLockHeldInfo;
    if (logAction.shouldLog()) {
      longestWriteLockHeldInfo = new LockHeldInfo();
    }

    coarseLock.writeLock().unlock();

    if (needReport) {
      addMetric(opName, writeLockIntervalNanos, true);
    }

    if (logAction.shouldLog()) {
      FSNamesystem.LOG.info(
          "\tNumber of suppressed write-lock reports: {}"
              + "\n\tLongest write-lock held at {} for {}ms by {}{} via {}"
              + "\n\tTotal suppressed write-lock held time: {}",
          logAction.getCount() - 1,
          Time.formatTime(lockHeldInfo.getStartTimeMs()),
          lockHeldInfo.getIntervalMs(), lockHeldInfo.getOpName(),
          lockHeldInfo.getLockReportInfo(), lockHeldInfo.getStackTrace(),
          logAction.getStats(0).getSum() - lockHeldInfo.getIntervalMs());
    }
  }

  public int getReadHoldCount() {
    return coarseLock.getReadHoldCount();
  }
  
  public int getWriteHoldCount() {
    return coarseLock.getWriteHoldCount();
  }
  
  public boolean isWriteLockedByCurrentThread() {
    return coarseLock.isWriteLockedByCurrentThread();
  }

  public Condition newWriteLockCondition() {
    return coarseLock.writeLock().newCondition();
  }

  /**
   * Returns the QueueLength of waiting threads.
   *
   * A larger number indicates greater lock contention.
   *
   * @return int - Number of threads waiting on this lock
   */
  public int getQueueLength() {
    return coarseLock.getQueueLength();
  }

  /**
   * Returns the number of time the read lock
   * has been held longer than the threshold.
   *
   * @return long - Number of time the read lock
   * has been held longer than the threshold
   */
  public long getNumOfReadLockLongHold() {
    return numReadLockLongHold.longValue();
  }

  /**
   * Returns the number of time the write lock
   * has been held longer than the threshold.
   *
   * @return long - Number of time the write lock
   * has been held longer than the threshold.
   */
  public long getNumOfWriteLockLongHold() {
    return numWriteLockLongHold.longValue();
  }

  /**
   * Add the lock hold time for a recent operation to the metrics.
   * @param operationName Name of the operation for which to record the time
   * @param value Length of time the lock was held (nanoseconds)
   */
  private void addMetric(String operationName, long value, boolean isWrite) {
    if (metricsEnabled) {
      String opMetric = getMetricName(operationName, isWrite);
      detailedHoldTimeMetrics.add(opMetric, value);

      String overallMetric = getMetricName(OVERALL_METRIC_NAME, isWrite);
      detailedHoldTimeMetrics.add(overallMetric, value);
    }
    updateProcessingDetails(
        isWrite ? Timing.LOCKEXCLUSIVE : Timing.LOCKSHARED, value);
  }
  //加锁
  private void doLock(boolean isWrite) {
    long startNanos = timer.monotonicNowNanos();
    if (isWrite) {
      coarseLock.writeLock().lock();
    } else {
      coarseLock.readLock().lock();
    }
    updateLockWait(startNanos, isWrite);
  }
  //可中断的加锁，如果一个线程调用 lockInterruptibly() 方法，并且在等待锁的过程中被另一个线程调用了 interrupt() 方法，那么该线程会抛出 InterruptedException 异常，从而中断等待
  private void doLockInterruptibly(boolean isWrite)
      throws InterruptedException {
    long startNanos = timer.monotonicNowNanos();
    if (isWrite) {
      coarseLock.writeLock().lockInterruptibly();
    } else {
      coarseLock.readLock().lockInterruptibly();
    }
    updateLockWait(startNanos, isWrite);
  }
  //记录获取锁的等待时间，并在成功获取锁后更新相应的时间戳
  //startNanos：锁等待开始的时间（以纳秒为单位）
  //isWrite：布尔值，表示当前是否为写锁操作，true 表示写锁，false 表示读锁
  private void updateLockWait(long startNanos, boolean isWrite) {
    //通过 timer.monotonicNowNanos() 获取当前的单调时间（单调时间不会因系统时间调整而回退）
    long now = timer.monotonicNowNanos();
    //计算锁等待时间（当前时间减去锁等待开始时间）并调用 updateProcessingDetails 方法更新相关统计数据，标记为 Timing.LOCKWAIT 类型
    updateProcessingDetails(Timing.LOCKWAIT, now - startNanos);
    if (isWrite) {
      //如果当前是写锁，并且 coarseLock.getWriteHoldCount() 返回 1，说明这是当前线程首次获取写锁，更新 writeLockHeldTimeStampNanos 时间戳
      if (coarseLock.getWriteHoldCount() == 1) {
        writeLockHeldTimeStampNanos = now;
      }
    } else {
      //如果是读锁，且 getReadHoldCount() 返回 1，说明这是当前线程首次获取读锁，更新 readLockHeldTimeStampNanos 时间戳
      if (coarseLock.getReadHoldCount() == 1) {
        readLockHeldTimeStampNanos.set(now);
      }
    }
  }
  //type：Timing 类型的枚举，表示需要记录的操作类型（如锁等待时间、I/O 时间等
  //deltaNanos：以纳秒为单位的时间间隔，表示某个操作的耗时
  //更新当前 RPC（远程过程调用，Remote Procedure Call）请求的处理详情，记录某个时间片段的耗时信息
  private static void updateProcessingDetails(Timing type, long deltaNanos) {
    Server.Call call = Server.getCurCall().get();
    if (call != null) {
      call.getProcessingDetails().add(type, deltaNanos, TimeUnit.NANOSECONDS);
    }
  }

  private static String getMetricName(String operationName, boolean isWrite) {
    return (isWrite ? WRITE_LOCK_METRIC_PREFIX : READ_LOCK_METRIC_PREFIX) +
        org.apache.commons.lang3.StringUtils.capitalize(operationName) +
        LOCK_METRIC_SUFFIX;
  }

  @VisibleForTesting
  public void setMetricsEnabled(boolean metricsEnabled) {
    this.metricsEnabled = metricsEnabled;
  }

  public boolean isMetricsEnabled() {
    return metricsEnabled;
  }

  public void setReadLockReportingThresholdMs(long readLockReportingThresholdMs) {
    this.readLockReportingThresholdMs = readLockReportingThresholdMs;
  }

  @VisibleForTesting
  public long getReadLockReportingThresholdMs() {
    return readLockReportingThresholdMs;
  }

  public void setWriteLockReportingThresholdMs(long writeLockReportingThresholdMs) {
    this.writeLockReportingThresholdMs = writeLockReportingThresholdMs;
  }

  @VisibleForTesting
  public long getWriteLockReportingThresholdMs() {
    return writeLockReportingThresholdMs;
  }

  /**
   * Read lock Held Info.
   */
  private static class LockHeldInfo {
    /** Lock held start time. */
    private final Long startTimeMs;
    /** Lock held time. */
    private final Long intervalMs;
    /** The stack trace lock was held. */
    private final String stackTrace;
    /** The operation name. */
    private final String opName;
    /** The info shown in a lock report. */
    private final String lockReportInfo;

    LockHeldInfo() {
      this.startTimeMs = 0L;
      this.intervalMs = 0L;
      this.stackTrace = null;
      this.opName = null;
      this.lockReportInfo = null;
    }

    LockHeldInfo(long startTimeMs, long intervalMs, String stackTrace,
        String opName, String lockReportInfo) {
      this.startTimeMs = startTimeMs;
      this.intervalMs = intervalMs;
      this.stackTrace = stackTrace;
      this.opName = opName;
      this.lockReportInfo = lockReportInfo;
    }

    public Long getStartTimeMs() {
      return this.startTimeMs;
    }

    public Long getIntervalMs() {
      return this.intervalMs;
    }

    public String getStackTrace() {
      return this.stackTrace;
    }

    public String getOpName() {
      return opName;
    }

    public String getLockReportInfo() {
      return lockReportInfo;
    }
  }
}
