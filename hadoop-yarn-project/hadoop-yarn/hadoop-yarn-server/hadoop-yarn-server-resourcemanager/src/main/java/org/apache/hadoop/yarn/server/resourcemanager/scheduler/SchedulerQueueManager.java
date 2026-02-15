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

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSchedulerConfiguration;

/**
 *
 * Context of the Queues in Scheduler.
 *
 */
//用于管理调度器中的 所有队列，包括：
//获取根队列
//管理（增加、删除、获取）队列
//重初始化队列
@SuppressWarnings("rawtypes")
@Private
@Unstable
public interface SchedulerQueueManager<T extends SchedulerQueue,
    E extends ReservationSchedulerConfiguration> {

  /**获取 根队列
   * Get the root queue.
   * @return root queue
   */
  T getRootQueue();

  /**获取 所有队列的映射表
   * Get all the queues.
   * @return a map contains all the queues as well as related queue names
   */
  Map<String, T> getQueues();

  /**移除指定名称的队列
   * Remove the queue from the existing queue.
   * @param queueName the queue name
   */
  void removeQueue(String queueName);

  /**向调度器中 添加新队列
   * Add a new queue to the existing queues.
   * @param queueName the queue name
   * @param queue the queue object
   */
  void addQueue(String queueName, T queue);

  /**根据 队列名称 获取 队列对象
   * Get a queue matching the specified queue name.
   * @param queueName the queue name
   * @return a queue object
   */
  T getQueue(String queueName);

  /**重新初始化队列，基于新的 ReservationSchedulerConfiguration 配置
   * Reinitialize the queues.
   * @param newConf the configuration
   * @throws IOException if fails to re-initialize queues
   */
  void reinitializeQueues(E newConf) throws IOException;
}
