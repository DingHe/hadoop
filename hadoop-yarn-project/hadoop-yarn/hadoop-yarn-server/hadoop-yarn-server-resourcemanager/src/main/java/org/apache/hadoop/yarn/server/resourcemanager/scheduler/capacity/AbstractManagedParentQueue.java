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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerDynamicEditException;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common
    .QueueEntitlement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * A container class for automatically created child leaf queues.
 * From the user perspective this is equivalent to a LeafQueue,
 * but functionality wise is a sub-class of ParentQueue
 */
//充当自动创建的子叶队列（AutoCreatedLeafQueue）的管理容器，
// 从用户的角度来看，它的功能与 LeafQueue 类似，但实际上它是 ParentQueue 的子类，具备父队列的功能
  //管理自动创建的子队列（LeafQueue），允许动态添加或删除子队列
  //管理子队列的容量，提供方法计算所有子队列的总容量
  //提供子队列配置模板，以便新创建的子队列继承默认配置
  //支持动态修改队列的资源分配，确保子队列的容量变更符合调度策略
public abstract class AbstractManagedParentQueue extends AbstractParentQueue {

  private static final Logger LOG = LoggerFactory.getLogger(
      AbstractManagedParentQueue.class);
  //存储自动创建的 LeafQueue 的默认配置
  protected AutoCreatedLeafQueueConfig leafQueueTemplate;
  // 负责定义自动创建队列的管理策略
  protected AutoCreatedQueueManagementPolicy queueManagementPolicy = null;

  public AbstractManagedParentQueue(CapacitySchedulerQueueContext queueContext,
      String queueName, CSQueue parent, CSQueue old) throws IOException {
    super(queueContext, queueName, parent, old);
  }

  @Override
  public void reinitialize(CSQueue newlyParsedQueue, Resource clusterResource)
      throws IOException {
    writeLock.lock();
    try {
      // Set new configs
      setupQueueConfigs(clusterResource);

    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Add the specified child queue.
   * @param childQueue reference to the child queue to be added
   * @throws SchedulerDynamicEditException when addChildQueue fails.
   * @throws IOException an I/O exception has occurred.
   */
  public void addChildQueue(CSQueue childQueue)
      throws SchedulerDynamicEditException, IOException {
    writeLock.lock();
    try {
      if (childQueue.getCapacity() > 0) {
        throw new SchedulerDynamicEditException(
            "Queue " + childQueue + " being added has non zero capacity.");
      }
      boolean added = this.childQueues.add(childQueue);
      if (LOG.isDebugEnabled()) {
        LOG.debug("updateChildQueues (action: add queue): " + added + " "
            + getChildQueuesToPrint());
      }
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Remove the specified child queue.
   * @param childQueue reference to the child queue to be removed
   * @throws SchedulerDynamicEditException when removeChildQueue fails.
   */
  public void removeChildQueue(CSQueue childQueue)
      throws SchedulerDynamicEditException {
    writeLock.lock();
    try {
      if (childQueue.getCapacity() > 0) {
        throw new SchedulerDynamicEditException(
            "Queue " + childQueue + " being removed has non zero capacity.");
      }
      Iterator<CSQueue> qiter = childQueues.iterator();
      while (qiter.hasNext()) {
        CSQueue cs = qiter.next();
        if (cs.equals(childQueue)) {
          qiter.remove();
          LOG.debug("Removed child queue: {}", cs.getQueuePath());
        }
      }
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Remove the specified child queue.
   * @param childQueueName name of the child queue to be removed
   * @return child queue.
   * @throws SchedulerDynamicEditException when removeChildQueue fails.
   */
  public CSQueue removeChildQueue(String childQueueName)
      throws SchedulerDynamicEditException {
    CSQueue childQueue;
    writeLock.lock();
    try {
      childQueue = queueContext.getQueueManager().getQueue(childQueueName);
      if (childQueue != null) {
        removeChildQueue(childQueue);
      } else {
        throw new SchedulerDynamicEditException("Cannot find queue to delete "
            + ": " + childQueueName);
      }
    } finally {
      writeLock.unlock();
    }
    return childQueue;
  }

  protected float sumOfChildCapacities() {
    writeLock.lock();
    try {
      float ret = 0;
      for (CSQueue l : childQueues) {
        ret += l.getCapacity();
      }
      return ret;
    } finally {
      writeLock.unlock();
    }
  }

  protected float sumOfChildAbsCapacities() {
    writeLock.lock();
    try {
      float ret = 0;
      for (CSQueue l : childQueues) {
        ret += l.getAbsoluteCapacity();
      }
      return ret;
    } finally {
      writeLock.unlock();
    }
  }

  public AutoCreatedLeafQueueConfig getLeafQueueTemplate() {
    return leafQueueTemplate;
  }

  public AutoCreatedQueueManagementPolicy
  getAutoCreatedQueueManagementPolicy() {
    return queueManagementPolicy;
  }

  protected CapacitySchedulerConfiguration initializeLeafQueueConfigs(String
      configPrefix) {

    CapacitySchedulerConfiguration leafQueueConfigs = new
        CapacitySchedulerConfiguration(new Configuration(false), false);

    Map<String, String> templateConfigs = queueContext
        .getConfiguration().getConfigurationProperties()
        .getPropertiesWithPrefix(configPrefix, true);

    for (Map.Entry<String, String> confKeyValuePair : templateConfigs.entrySet()) {
      leafQueueConfigs.set(confKeyValuePair.getKey(), confKeyValuePair.getValue());
    }

    return leafQueueConfigs;
  }

  protected void validateQueueEntitlementChange(AbstractAutoCreatedLeafQueue
      leafQueue, QueueEntitlement entitlement)
      throws SchedulerDynamicEditException {

    float sumChilds = sumOfChildCapacities();
    float newChildCap =
        sumChilds - leafQueue.getCapacity() + entitlement.getCapacity();

    if (!(newChildCap >= 0 && newChildCap < 1.0f + CSQueueUtils.EPSILON)) {
      throw new SchedulerDynamicEditException(
          "Sum of child queues should exceed 100% for auto creating parent "
              + "queue : " + getQueueName());
    }
  }
}
