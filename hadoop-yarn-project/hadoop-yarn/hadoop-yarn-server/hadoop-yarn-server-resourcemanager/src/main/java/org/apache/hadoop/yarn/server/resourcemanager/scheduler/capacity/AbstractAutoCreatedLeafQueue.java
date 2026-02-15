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

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .SchedulerDynamicEditException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common
    .QueueEntitlement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager
    .NO_LABEL;

/**
 * Abstract class for dynamic auto created queues managed by an implementation
 * of AbstractManagedParentQueue
 */
//用于管理 动态自动创建的队列。
//它主要由 AbstractManagedParentQueue 负责管理，在需要时自动创建或删除，以适应 动态资源分配需求
public class AbstractAutoCreatedLeafQueue extends AbstractLeafQueue {
  private static final Logger LOG = LoggerFactory.getLogger(
      AbstractAutoCreatedLeafQueue.class);
  //指向当前自动创建队列的 父队列
  //所有 AbstractAutoCreatedLeafQueue 实例都必须属于某个 管理型父队列，这个属性用于追踪其父队列
  protected AbstractManagedParentQueue parent;

  public AbstractAutoCreatedLeafQueue(CapacitySchedulerQueueContext queueContext,
      String queueName, AbstractManagedParentQueue parent, CSQueue old)
      throws IOException {
    super(queueContext, queueName, parent, old);
    this.parent = parent;
  }

  /**
   * This methods to change capacity for a queue and adjusts its
   * absoluteCapacity
   *
   * @param entitlement the new entitlement for the queue (capacity,
   *                    maxCapacity, etc..)
   * @throws SchedulerDynamicEditException when setEntitlement fails.
   */
  //修改队列的容量
  public void setEntitlement(QueueEntitlement entitlement)
      throws SchedulerDynamicEditException {
     setEntitlement(NO_LABEL, entitlement);
  }

  @Override
  protected Resource getMinimumAbsoluteResource(String queuePath,
      String label) {
    return super.getMinimumAbsoluteResource(queueContext.getConfiguration()
        .getAutoCreatedQueueTemplateConfPrefix(this.getParent().getQueuePath()),
        label);
  }

  @Override
  protected Resource getMaximumAbsoluteResource(String queuePath,
      String label) {
    return super.getMaximumAbsoluteResource(queueContext.getConfiguration()
        .getAutoCreatedQueueTemplateConfPrefix(this.getParent().getQueuePath()),
        label);
  }

  @Override
  protected boolean checkConfigTypeIsAbsoluteResource(String queuePath,
      String label) {
    return super.checkConfigTypeIsAbsoluteResource(queueContext.getConfiguration()
        .getAutoCreatedQueueTemplateConfPrefix(this.getParent().getQueuePath()),
        label);
  }

  /**
   * This methods to change capacity for a queue and adjusts its
   * absoluteCapacity.
   *
   * @param nodeLabel nodeLabel.
   * @param entitlement the new entitlement for the queue (capacity,
   *                    maxCapacity, etc..)
   * @throws SchedulerDynamicEditException when setEntitlement fails.
   */
  //用于 动态调整队列的资源配额（即 capacity、maxCapacity 等），并更新队列的 绝对容量（absoluteCapacity）
  //nodeLabel 代表 节点标签（Node Label），用于标识 特定类型的计算资源
  //entitlement 队列的新 资源配额 capacity（容量，表示分配给该队列的比例） maxCapacity（最大容量，表示队列的上限比例）
  public void setEntitlement(String nodeLabel, QueueEntitlement entitlement)
      throws SchedulerDynamicEditException {
    writeLock.lock();
    try {
      //capacity 代表队列的 相对资源占比，取值必须在 0 到 1 之间
      float capacity = entitlement.getCapacity();
      if (capacity < 0 || capacity > 1.0f) {
        throw new SchedulerDynamicEditException(
            "Capacity demand is not in the [0,1] range: " + capacity);
      }
      //负责 更新当前队列的资源容量
      setCapacity(nodeLabel, capacity);
      //计算并设置队列的 absoluteCapacity
      setAbsoluteCapacity(nodeLabel,
          this.getParent().getQueueCapacities().
              getAbsoluteCapacity(nodeLabel)
              * getQueueCapacities().getCapacity(nodeLabel));
      // note: we currently set maxCapacity to capacity
      // this might be revised later
      //设置 maxCapacity
      setMaxCapacity(nodeLabel, entitlement.getMaxCapacity());
      //更新最小/最大容量向量
      setConfiguredMinCapacityVector(nodeLabel,
          QueueCapacityVector.of(queueCapacities.getCapacity(nodeLabel) * 100,
              QueueCapacityVector.ResourceUnitCapacityType.PERCENTAGE));
      setConfiguredMaxCapacityVector(nodeLabel,
          QueueCapacityVector.of(queueCapacities.getMaximumCapacity(nodeLabel) * 100,
              QueueCapacityVector.ResourceUnitCapacityType.PERCENTAGE));

      LOG.debug("successfully changed to {} for queue {}", capacity, this
            .getQueuePath());

      //update queue used capacity etc
      CSQueueUtils.updateQueueStatistics(resourceCalculator,
          queueContext.getClusterResource(),
          this, labelManager, nodeLabel);
    } finally {
      writeLock.unlock();
    }
  }
}
