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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import java.util.List;
import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 *
 * Represents a queue in Scheduler.
 *表示 调度器中的一个队列，用于 管理队列的层级结构（父队列、子队列）
 * 用于组织 资源队列的层级结构 和 调度控制
 */
@SuppressWarnings("rawtypes")
@LimitedPrivate("yarn")
public interface SchedulerQueue<T extends SchedulerQueue> extends Queue {

  /** 返回当前队列的 子队列列表
   * Get list of child queues.
   * @return a list of child queues
   */
  List<T> getChildQueues();

  /**返回 当前队列的父队列
   * Get the parent queue.
   * @return the parent queue
   */
  T getParent();

  /**获取当前队列的 状态
   * Get current queue state.
   * @return the queue state
   */
  QueueState getState();

  /**更新队列的 状态
   * Update the queue state.
   * @param state the queue state
   */
  void updateQueueState(QueueState state);

  /**停止当前队列
   * Stop the queue.
   */
  void stopQueue();

  /**激活一个已停止的队列，允许其重新接受任务调度
   * Activate the queue.
   * @throws YarnException if the queue can not be activated.
   */
  void activateQueue() throws YarnException;
}
