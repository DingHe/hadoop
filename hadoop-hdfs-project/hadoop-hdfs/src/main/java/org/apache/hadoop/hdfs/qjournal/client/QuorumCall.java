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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.StopWatch;
import org.apache.hadoop.util.Timer;

import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.FutureCallback;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.Futures;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.thirdparty.protobuf.Message;
import org.apache.hadoop.thirdparty.protobuf.TextFormat;


/**
 * Represents a set of calls for which a quorum of results is needed.
 * @param <KEY> a key used to identify each of the outgoing calls
 * @param <RESULT> the type of the call result
 */
// 用于管理和跟踪多个并发的 RPC（远程过程调用），目的是在分布式系统中实现对**多数响应（Quorum）**的等待和统计，
// 确保系统在达到一定数量的响应、成功或异常时及时返回结果或异常
// 在 HDFS 的 JournalNode 集群中，QuorumCall 主要负责协调客户端与多个 JournalNode 之间的通信，等待来自多个节点的响应，
// 确保系统在故障容忍范围内完成写入、同步或状态检查操作
class QuorumCall<KEY, RESULT> {
  //存储成功的 RPC 调用结果，KEY 是每个调用的唯一标识，RESULT 是调用的结果
  private final Map<KEY, RESULT> successes = Maps.newHashMap();
  //存储失败的 RPC 调用信息，Throwable 记录了每个调用的异常情况
  private final Map<KEY, Throwable> exceptions = Maps.newHashMap();

  /**
   * Interval, in milliseconds, at which a log message will be made
   * while waiting for a quorum call.
   */
  //在等待Quorum完成时，日志输出的间隔时间（单位：毫秒），默认 1000ms，即 1 秒
  private static final int WAIT_PROGRESS_INTERVAL_MILLIS = 1000;
  
  /**
   * Start logging messages at INFO level periodically after waiting for
   * this fraction of the configured timeout for any call.
   */
  //当等待时间超过设定超时时间的 30% 时，开始输出 INFO 级别的日志
  private static final float WAIT_PROGRESS_INFO_THRESHOLD = 0.3f;
  /**
   * Start logging messages at WARN level after waiting for this
   * fraction of the configured timeout for any call.
   */
  //当等待时间超过设定超时时间的 70% 时，开始输出 WARN 级别的日志，提示可能存在问题
  private static final float WAIT_PROGRESS_WARN_THRESHOLD = 0.7f;
  //记录 QuorumCall 的执行时间，主要用于监测和处理系统暂停（如 GC 导致的长时间暂停）
  private final StopWatch quorumStopWatch;
  private final Timer timer;
  //存储所有的异步调用 ListenableFuture 对象，方便后续取消未完成的调用
  private final List<ListenableFuture<RESULT>> allCalls;

  //创建并初始化一个 QuorumCall 对象，用于管理多个异步调用 (ListenableFuture) 的响应结果
  //calls 异步调用集合，每个 key 对应一个 Future 任务
  //timer 计时器，用于跟踪任务执行时间
  static <KEY, RESULT> QuorumCall<KEY, RESULT> create(
      Map<KEY, ? extends ListenableFuture<RESULT>> calls, Timer timer) {
    final QuorumCall<KEY, RESULT> qr = new QuorumCall<KEY, RESULT>(timer);
    for (final Entry<KEY, ? extends ListenableFuture<RESULT>> e : calls.entrySet()) {
      Preconditions.checkArgument(e.getValue() != null,
          "null future for key: " + e.getKey());
      qr.addCall(e.getValue());
      Futures.addCallback(e.getValue(), new FutureCallback<RESULT>() {
        @Override
        public void onFailure(Throwable t) {
          qr.addException(e.getKey(), t);
        }

        @Override
        public void onSuccess(RESULT res) {
          qr.addResult(e.getKey(), res);
        }
      }, MoreExecutors.directExecutor());
    }
    return qr;
  }

  static <KEY, RESULT> QuorumCall<KEY, RESULT> create(
      Map<KEY, ? extends ListenableFuture<RESULT>> calls) {
    return create(calls, new Timer());
  }

  /**
   * Not intended for outside use.
   */
  private QuorumCall() {
    this(new Timer());
  }

  private QuorumCall(Timer timer) {
    // Only instantiated from factory method above
    this.timer = timer;
    this.quorumStopWatch = new StopWatch(timer);
    this.allCalls = new ArrayList<>();
  }

  private void addCall(ListenableFuture<RESULT> call) {
    allCalls.add(call);
  }

  /**
   * Used in conjunction with {@link #getQuorumTimeoutIncreaseMillis(long, int)}
   * to check for pauses.
   */
  //用于重置并重新启动用于计时的 quorumStopWatch 对象，主要用于监测系统执行过程中是否发生了长时间暂停（如垃圾回收 GC 导致的停顿）
  private void restartQuorumStopWatch() {
    quorumStopWatch.reset().start();
  }

  /**
   * Check for a pause (e.g. GC) since the last time
   * {@link #restartQuorumStopWatch()} was called. If detected, return the
   * length of the pause; else, -1.
   * @param offset Offset the elapsed time by this amount; use if some amount
   *               of pause was expected
   * @param millis Total length of timeout in milliseconds
   * @return Length of pause, if detected, else -1
   */
  // 用于检测系统是否发生了长时间的暂停（例如 Java 的垃圾回收(GC)或其他线程阻塞），
  // 并根据检测到的暂停时间动态地调整超时时间，以避免因为暂停导致的错误超时
  //offset：偏移量，表示已知或预期的暂停时间，主要用于补偿在某些情况下的时间误差
  //millis：调用方法的超时时间，单位为毫秒，用于与暂停时间作比较，判断是否需要增加超时时间
  private long getQuorumTimeoutIncreaseMillis(long offset, int millis) {
    //通过 quorumStopWatch 记录的时间，获取自上次调用 restartQuorumStopWatch() 以来经过的时间（毫秒）
    long elapsed = quorumStopWatch.now(TimeUnit.MILLISECONDS);
    long pauseTime = elapsed + offset;
    if (pauseTime > (millis * WAIT_PROGRESS_INFO_THRESHOLD)) {
      QuorumJournalManager.LOG.info("Pause detected while waiting for " +
          "QuorumCall response; increasing timeout threshold by pause time " +
          "of " + pauseTime + " ms.");
      return pauseTime;
    } else {
      return -1;
    }
  }

  
  /**
   * Wait for the quorum to achieve a certain number of responses.
   * 
   * Note that, even after this returns, more responses may arrive,
   * causing the return value of other methods in this class to change.
   *
   * @param minResponses return as soon as this many responses have been
   * received, regardless of whether they are successes or exceptions
   * @param minSuccesses return as soon as this many successful (non-exception)
   * responses have been received
   * @param maxExceptions return as soon as this many exception responses
   * have been received. Pass 0 to return immediately if any exception is
   * received.
   * @param millis the number of milliseconds to wait for
   * @throws InterruptedException if the thread is interrupted while waiting
   * @throws TimeoutException if the specified timeout elapses before
   * achieving the desired conditions
   */
  //等待法定最小响应数（Quorum）达到指定条件。
  //millis          最大等待时间（毫秒）
  //operationName   当前等待操作的名称（用于日志记录）
  //该方法用于等待来自多个节点（或数据源）的响应，直到满足以下任一条件：
  //最小响应数 (minResponses)：接收到的响应总数（成功 + 失败）达到指定值。
  //最小成功数 (minSuccesses)：接收到的成功响应数量达到指定值。
  //最大异常数 (maxExceptions)：接收到的异常数量超过该值时立即返回
  public synchronized void waitFor(
      int minResponses, int minSuccesses, int maxExceptions,
      int millis, String operationName)
      throws InterruptedException, TimeoutException {
    long st = timer.monotonicNow();
    long nextLogTime = st + (long)(millis * WAIT_PROGRESS_INFO_THRESHOLD);// 下次打印日志的时间
    long et = st + millis;// 计算超时时间点
    while (true) {
      restartQuorumStopWatch();// 重置并启动计时器，监控是否发生长时间暂停
      checkAssertionErrors();// 检查是否有 AssertionError，若有则抛出
      if (minResponses > 0 && countResponses() >= minResponses) return;
      if (minSuccesses > 0 && countSuccesses() >= minSuccesses) return;
      if (maxExceptions >= 0 && countExceptions() > maxExceptions) return;
      long now = timer.monotonicNow();
      
      if (now > nextLogTime) {
        long waited = now - st;
        String msg = String.format(
            "Waited %s ms (timeout=%s ms) for a response for %s",
            waited, millis, operationName);
        if (!successes.isEmpty()) {
          msg += ". Succeeded so far: [" + Joiner.on(",").join(successes.keySet()) + "]";
        }
        if (!exceptions.isEmpty()) {
          msg += ". Exceptions so far: [" + getExceptionMapString() + "]";
        }
        if (successes.isEmpty() && exceptions.isEmpty()) {
          msg += ". No responses yet.";
        }
        if (waited > millis * WAIT_PROGRESS_WARN_THRESHOLD) {
          QuorumJournalManager.LOG.warn(msg);
        } else {
          QuorumJournalManager.LOG.info(msg);
        }
        nextLogTime = now + WAIT_PROGRESS_INTERVAL_MILLIS;
      }
      long rem = et - now;// 计算剩余等待时间
      if (rem <= 0) {
        // Increase timeout if a full GC occurred after restarting stopWatch
        long timeoutIncrease = getQuorumTimeoutIncreaseMillis(0, millis);
        if (timeoutIncrease > 0) {
          // 如果发生 GC 暂停，适当延长等待时间
          et += timeoutIncrease;
        } else {
          // 超时且无暂停，抛出 TimeoutException
          throw new TimeoutException();
        }
      }
      restartQuorumStopWatch();
      rem = Math.min(rem, nextLogTime - now);
      rem = Math.max(rem, 1);
      wait(rem);
      // Increase timeout if a full GC occurred after restarting stopWatch
      long timeoutIncrease = getQuorumTimeoutIncreaseMillis(-rem, millis);
      if (timeoutIncrease > 0) {
        et += timeoutIncrease;
      }
    }
  }

  /** 取消所有的异步调用
   * Cancel any outstanding calls.
   */
  void cancelCalls() {
    for (ListenableFuture<RESULT> call : allCalls) {
      call.cancel(true);
    }
  }

  /**
   * Check if any of the responses came back with an AssertionError.
   * If so, it re-throws it, even if there was a quorum of responses.
   * This code only runs if assertions are enabled for this class,
   * otherwise it should JIT itself away.
   * 
   * This is done since AssertionError indicates programmer confusion
   * rather than some kind of expected issue, and thus in the context
   * of test cases we'd like to actually fail the test case instead of
   * continuing through.
   */
  //检查是否有响应返回了 AssertionError
  private synchronized void checkAssertionErrors() {
    boolean assertsEnabled = false;
    assert assertsEnabled = true; // sets to true if enabled
    if (assertsEnabled) {
      for (Throwable t : exceptions.values()) {
        if (t instanceof AssertionError) {
          throw (AssertionError)t;
        } else if (t instanceof RemoteException &&
            ((RemoteException)t).getClassName().equals(
                AssertionError.class.getName())) {
          throw new AssertionError(t);
        }
      }
    }
  }
  //增加成功的调用
  private synchronized void addResult(KEY k, RESULT res) {
    successes.put(k, res);
    notifyAll();
  }
  //增加异常的调用
  private synchronized void addException(KEY k, Throwable t) {
    exceptions.put(k, t);
    notifyAll();
  }
  
  /**
   * @return the total number of calls for which a response has been received,
   * regardless of whether it threw an exception or returned a successful
   * result.
   */
  //所有的响应数量
  public synchronized int countResponses() {
    return successes.size() + exceptions.size();
  }
  
  /**
   * @return the number of calls for which a non-exception response has been
   * received.
   */
  //成功的响应数量
  public synchronized int countSuccesses() {
    return successes.size();
  }
  
  /**
   * @return the number of calls for which an exception response has been
   * received.
   */
  //异常的响应数量
  public synchronized int countExceptions() {
    return exceptions.size();
  }

  /**
   * @return the map of successful responses. A copy is made such that this
   * map will not be further mutated, even if further results arrive for the
   * quorum.
   */
  public synchronized Map<KEY, RESULT> getResults() {
    return Maps.newHashMap(successes);
  }

  public synchronized void rethrowException(String msg) throws QuorumException {
    Preconditions.checkState(!exceptions.isEmpty());
    throw QuorumException.create(msg, successes, exceptions);
  }

  public static <K> String mapToString(
      Map<K, ? extends Message> map) {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (Map.Entry<K, ? extends Message> e : map.entrySet()) {
      if (!first) {
        sb.append("\n");
      }
      first = false;
      sb.append(e.getKey()).append(": ")
        .append(TextFormat.shortDebugString(e.getValue()));
    }
    return sb.toString();
  }

  /**
   * Return a string suitable for displaying to the user, containing
   * any exceptions that have been received so far.
   */
  private String getExceptionMapString() {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (Map.Entry<KEY, Throwable> e : exceptions.entrySet()) {
      if (!first) {
        sb.append(", ");
      }
      first = false;
      sb.append(e.getKey()).append(": ")
        .append(e.getValue().getLocalizedMessage());
    }
    return sb.toString();
  }
}
