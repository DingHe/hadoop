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

import static org.apache.hadoop.util.Time.monotonicNow;

/** 
 * a class to throttle the data transfers.
 * This class is thread safe. It can be shared by multiple threads.
 * The parameter bandwidthPerSec specifies the total bandwidth shared by
 * threads.
 */
//HDFS 内部用于限制数据传输速率的工具类
//对数据传输进行流控（throttle），防止 I/O 过载，保证 HDFS 传输带宽在设定范围内
  //线程安全，可以被多个线程共享，适用于并发数据传输场景
  //动态调整带宽，支持 设置新的带宽值，并在新的时间周期内生效
public class DataTransferThrottler {
  //流控时间周期（毫秒），数据传输速率限制在这个时间窗口内生效（默认 500ms）
  private final long period;          // period over which bw is imposed
  //最大时间扩展周期（period * 3），如果 period 过期但 throttle() 没执行，会扩展 period 以适应长时间不活动的情况
  private final long periodExtension; // Max period over which bw accumulates.
  //每个周期允许传输的字节数，由 带宽（B/s） 计算得出：bytesPerPeriod = (bandwidthPerSec * period) / 1000
  private long bytesPerPeriod;  // total number of bytes can be sent in each period
  //前流控周期的开始时间（使用 monotonicNow() 获取）
  private long curPeriodStart;  // current period starting time
  //当前周期内剩余可传输字节数，每次传输数据时会减少，当小于 0 需要等待
  private long curReserve;      // remaining bytes can be sent in the period
  //当前周期已经使用的字节数，防止超过带宽限制。
  private long bytesAlreadyUsed;

  /** Constructor 
   * @param bandwidthPerSec bandwidth allowed in bytes per second. 
   */
  public DataTransferThrottler(long bandwidthPerSec) {
    this(500, bandwidthPerSec);  // by default throttling period is 500ms 
  }

  /**
   * Constructor
   * @param period in milliseconds. Bandwidth is enforced over this
   *        period.
   * @param bandwidthPerSec bandwidth allowed in bytes per second. 
   */
  public DataTransferThrottler(long period, long bandwidthPerSec) {
    this.curPeriodStart = monotonicNow();
    this.period = period;
    this.curReserve = this.bytesPerPeriod = bandwidthPerSec*period/1000;
    this.periodExtension = period*3;
  }

  /**
   * @return current throttle bandwidth in bytes per second.
   */
  //获取当前限速带宽
  public synchronized long getBandwidth() {
    return bytesPerPeriod*1000/period;
  }
  
  /**
   * Sets throttle bandwidth. This takes affect latest by the end of current
   * period.
   */
  //设定带宽
  public synchronized void setBandwidth(long bytesPerSecond) {
    if ( bytesPerSecond <= 0 ) {
      throw new IllegalArgumentException("" + bytesPerSecond);
    }
    bytesPerPeriod = bytesPerSecond*period/1000;
  }
  
  /** Given the numOfBytes sent/received since last time throttle was called,
   * make the current thread sleep if I/O rate is too fast
   * compared to the given bandwidth.
   *
   * @param numOfBytes
   *     number of bytes sent/received since last time throttle was called
   */
  //限制数据传输速率
  public synchronized void throttle(long numOfBytes) {
    throttle(numOfBytes, null);
  }

  /** Given the numOfBytes sent/received since last time throttle was called,
   * make the current thread sleep if I/O rate is too fast
   * compared to the given bandwidth.  Allows for optional external cancelation.
   *
   * @param numOfBytes
   *     number of bytes sent/received since last time throttle was called
   * @param canceler
   *     optional canceler to check for abort of throttle
   */
  //核心限速逻辑，控制当前线程数据传输速率，防止超出设定的带宽上限
  //计算 剩余可传输字节数，如果超出限制，则 阻塞线程等待
  //numOfBytes 传输的字节数
  public synchronized void throttle(long numOfBytes, Canceler canceler) {
    //若 numOfBytes <= 0，直接返回，不执行限速逻辑
    if ( numOfBytes <= 0 ) {
      return;
    }
    //减少当前可用带宽 curReserve，增加已使用带宽 bytesAlreadyUsed
    curReserve -= numOfBytes;
    bytesAlreadyUsed += numOfBytes;
    //如果 curReserve 变为负数，说明当前传输速率超出了限制，需要等待
    while (curReserve <= 0) {
      //如果 Canceler 被触发（canceler.isCancelled() 返回 true），直接返回，不再等待
      if (canceler != null && canceler.isCancelled()) {
        return;
      }
      long now = monotonicNow();
      long curPeriodEnd = curPeriodStart + period;
      //now < curPeriodEnd：当前仍在本周期内，等待 curPeriodEnd - now 毫秒，让 curReserve 重新恢复
      if ( now < curPeriodEnd ) {
        // Wait for next period so that curReserve can be increased.
        try {
          wait( curPeriodEnd - now );
        } catch (InterruptedException e) {
          // Abort throttle and reset interrupted status to make sure other
          // interrupt handling higher in the call stack executes.
          Thread.currentThread().interrupt();
          break;
        }
      } else if ( now <  (curPeriodStart + periodExtension)) {
        curPeriodStart = curPeriodEnd;
        curReserve += bytesPerPeriod;
      } else {
        // discard the prev period. Throttler might not have
        // been used for a long time.
        curPeriodStart = now;
        curReserve = bytesPerPeriod - bytesAlreadyUsed;
      }
    }

    bytesAlreadyUsed -= numOfBytes;
  }
}
