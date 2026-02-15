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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueResourceQuotas;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
//跟踪队列资源使用情况的类，主要负责：
//记录资源使用情况（如已使用资源、等待资源等）。
//管理容器 (Container) 数量（增加/减少）。
//存储队列资源配额 (QueueResourceQuotas)。
//维护最新的任务提交时间戳（适用于动态队列）
public class CSQueueUsageTracker {
  //该队列的度量指标（如资源分配统计）。
  private final CSQueueMetrics metrics;
  //当前队列中运行的容器数量。
  private int numContainers;

  /**
   * The timestamp of the last submitted application to this queue.
   * Only applies to dynamic queues.
   */
  //该队列中最近提交应用的时间戳（仅适用于动态队列）。
  private long lastSubmittedTimestamp;

  /**
   * Tracks resource usage by label like used-resource / pending-resource.
   */
  //队列的资源使用情况（如已使用资源、等待资源等）。
  private final ResourceUsage queueUsage;
  //队列的资源配额（如最大资源限制）。
  private final QueueResourceQuotas queueResourceQuotas;

  public CSQueueUsageTracker(CSQueueMetrics metrics) {
    this.metrics = metrics;
    this.queueUsage = new ResourceUsage();
    this.queueResourceQuotas = new QueueResourceQuotas();
  }

  public int getNumContainers() {
    return numContainers;
  }

  public synchronized void increaseNumContainers() {
    numContainers++;
  }

  public synchronized void decreaseNumContainers() {
    numContainers--;
  }

  public CSQueueMetrics getMetrics() {
    return metrics;
  }

  public long getLastSubmittedTimestamp() {
    return lastSubmittedTimestamp;
  }

  public void setLastSubmittedTimestamp(long lastSubmittedTimestamp) {
    this.lastSubmittedTimestamp = lastSubmittedTimestamp;
  }

  public ResourceUsage getQueueUsage() {
    return queueUsage;
  }

  public QueueResourceQuotas getQueueResourceQuotas() {
    return queueResourceQuotas;
  }

}
