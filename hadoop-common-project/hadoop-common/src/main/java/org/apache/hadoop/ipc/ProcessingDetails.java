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

package org.apache.hadoop.ipc;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Stores the times that a call takes to be processed through each step and
 * its response status.
 */
// 记录 RPC（远程过程调用）执行过程中各个阶段所消耗的时间，以及该调用的返回状态。
// 此类主要用于性能监控和问题诊断，帮助跟踪请求在 HDFS 系统中处理的各个步骤耗时情况，方便定位瓶颈和异常
@InterfaceStability.Unstable
@InterfaceAudience.Private
public class ProcessingDetails {
  public static final Logger LOG =
      LoggerFactory.getLogger(ProcessingDetails.class);
  //用于设置和存储时间的单位（如纳秒、微秒、毫秒等）。所有的时间记录都依据该单位进行转换和存储
  private final TimeUnit valueTimeUnit;

  /**
   * The different stages to track the time of.
   */
  public enum Timing {
    ENQUEUE,          // time for reader to insert in call queue. 请求进入调用队列的时间，表示从读取请求到加入队列的耗时
    QUEUE,            // time in the call queue. 请求在调用队列中等待的时间，表示从入队到被处理器取走的耗时
    HANDLER,          // handler overhead not spent in processing/response. 处理器的开销时间，不包括实际处理和响应时间，主要记录调度和上下文切换等操作的耗时
    PROCESSING,       // time handler spent processing the call. always equal to
                      // lock_free + lock_wait + lock_shared + lock_exclusive 处理器处理请求的总时间，包含无锁、锁等待、共享锁、独占锁四个子阶段时间之和
    LOCKFREE,         // processing with no lock. 在没有锁竞争的情况下处理请求的时间
    LOCKWAIT,         // processing while waiting for lock. 等待锁的时间，表示请求处理过程中因获取锁而等待的时间
    LOCKSHARED,       // processing with a read lock. 持有共享锁（读锁）时处理请求的时间
    LOCKEXCLUSIVE,    // processing with a write lock. 持有独占锁（写锁）时处理请求的时间
    RESPONSE;         // time to encode and send response. 编码响应结果并发送给客户端所耗费的时间
  }
  //用于存储各个时间阶段的耗时，按 Timing 枚举的顺序依次保存
  private long[] timings = new long[Timing.values().length];

  // Rpc return status of this call
  private RpcStatusProto returnStatus = RpcStatusProto.SUCCESS;

  ProcessingDetails(TimeUnit timeUnit) {
    this.valueTimeUnit = timeUnit;
  }

  public long get(Timing type) {
    // When using nanoTime to fetch timing information, it is possible to see
    // time "move backward" slightly under unusual/rare circumstances. To avoid
    // displaying a confusing number, round such timings to 0 here.
    long ret = timings[type.ordinal()];
    return ret < 0 ? 0 : ret;
  }

  public long get(Timing type, TimeUnit timeUnit) {
    return timeUnit.convert(get(type), valueTimeUnit);
  }

  public void set(Timing type, long value) {
    timings[type.ordinal()] = value;
  }

  public void set(Timing type, long value, TimeUnit timeUnit) {
    set(type, valueTimeUnit.convert(value, timeUnit));
  }

  public void add(Timing type, long value, TimeUnit timeUnit) {
    timings[type.ordinal()] += valueTimeUnit.convert(value, timeUnit);
  }

  public void setReturnStatus(RpcStatusProto status) {
    this.returnStatus = status;
  }

  public RpcStatusProto getReturnStatus() {
    return returnStatus;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(256);
    for (Timing type : Timing.values()) {
      if (sb.length() > 0) {
        sb.append(" ");
      }
      sb.append(type.name().toLowerCase())
          .append("Time=").append(get(type));
    }
    return sb.toString();
  }
}
