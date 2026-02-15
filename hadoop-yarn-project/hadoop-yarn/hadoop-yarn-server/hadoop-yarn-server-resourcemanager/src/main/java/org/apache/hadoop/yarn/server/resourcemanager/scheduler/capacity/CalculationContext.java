/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.QueueCapacityVectorEntry;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType;

/**
 * A storage class that wraps arguments used in a resource calculation iteration.
 */
//计算资源分配时的 上下文存储类，用于封装 队列（queue）、资源名称 和 计算类型，并提供了一些快捷方法来获取队列的最小/最大容量配置
public class CalculationContext {
  private final String resourceName; //表示计算的资源名称，如 "memory-mb" 或 "vcores"
  private final ResourceUnitCapacityType capacityType;//表示 计算单位（如百分比或绝对值）
  private final CSQueue queue;//表示当前计算的 CSQueue（即 CapacityScheduler 的队列）

  public CalculationContext(String resourceName, ResourceUnitCapacityType capacityType,
                            CSQueue queue) {
    this.resourceName = resourceName;
    this.capacityType = capacityType;
    this.queue = queue;
  }

  public String getResourceName() {
    return resourceName;
  }

  public ResourceUnitCapacityType getCapacityType() {
    return capacityType;
  }

  public CSQueue getQueue() {
    return queue;
  }

  /**
   * A shorthand to return the minimum capacity vector entry for the currently evaluated child and
   * resource name.
   *
   * @param label node label
   * @return capacity vector entry
   */
  public QueueCapacityVectorEntry getCurrentMinimumCapacityEntry(String label) {
    return queue.getConfiguredCapacityVector(label).getResource(resourceName);
  }

  /**
   * A shorthand to return the maximum capacity vector entry for the currently evaluated child and
   * resource name.
   *
   * @param label node label
   * @return capacity vector entry
   */
  public QueueCapacityVectorEntry getCurrentMaximumCapacityEntry(String label) {
    return queue.getConfiguredMaxCapacityVector(label).getResource(resourceName);
  }
}
