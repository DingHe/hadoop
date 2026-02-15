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

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.apache.hadoop.yarn.api.records.ResourceInformation.MEMORY_URI;
import static org.apache.hadoop.yarn.api.records.ResourceInformation.VCORES_URI;
import static org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager.NO_LABEL;

/**
 * Controls how capacity and resource values are set and calculated for a queue.
 * Effective minimum and maximum resource values are set for each label and resource separately.
 */
//负责控制 YARN 容量调度器（CapacityScheduler）中队列的资源计算和更新。它的核心职责包括：
//计算队列的最小/最大有效资源值，并根据计算结果更新队列的容量。
//根据不同的资源单位类型（绝对值、百分比、权重）进行计算，以支持不同的容量调度方式。
//维护队列之间的资源关系，确保父子队列的资源分配符合调度策略。
//更新根队列的资源信息，确保其总资源与集群资源一致。
//在传统（Legacy）和新模式之间切换，兼容不同的调度策略

public class CapacitySchedulerQueueCapacityHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(CapacitySchedulerQueueCapacityHandler.class);
  //存储不同类型的容量计算器，根据不同的 ResourceUnitCapacityType（绝对值、百分比、权重）使用不同的计算方式
  private final Map<ResourceUnitCapacityType, AbstractQueueCapacityCalculator>
      calculators;
  //根队列容量计算器，用于计算根队列的资源分配
  private final AbstractQueueCapacityCalculator rootCalculator =
      new RootQueueCapacityCalculator();
  //节点标签管理器，用于管理不同节点标签（如 GPU 资源、CPU 资源）的资源划分
  private final RMNodeLabelsManager labelsManager;
  //定义的资源集合，存储集群支持的资源类型（如 memory-mb 和 vcores）
  private final Collection<String> definedResources = new LinkedHashSet<>();
  //是否启用传统队列模式，决定是否使用旧版调度逻辑
  private final boolean isLegacyQueueMode;

  public CapacitySchedulerQueueCapacityHandler(RMNodeLabelsManager labelsManager,
                                               CapacitySchedulerConfiguration configuration) {
    this.calculators = new HashMap<>();
    this.labelsManager = labelsManager;

    this.calculators.put(ResourceUnitCapacityType.ABSOLUTE,
        new AbsoluteResourceCapacityCalculator());
    this.calculators.put(ResourceUnitCapacityType.PERCENTAGE,
        new PercentageQueueCapacityCalculator());
    this.calculators.put(ResourceUnitCapacityType.WEIGHT,
        new WeightQueueCapacityCalculator());
    this.isLegacyQueueMode = configuration.isLegacyQueueMode();

    loadResourceNames();
  }

  /**
   * Updates the resource and metrics values of all children under a specific queue.
   * These values are calculated at runtime.
   *
   * @param clusterResource resource of the cluster  整个 YARN 集群的资源总量
   * @param queue           parent queue whose children will be updated 需要更新子队列的父队列对象
   * @return update context that contains information about the update phase
   */
  public QueueCapacityUpdateContext updateChildren(Resource clusterResource, CSQueue queue) {
    ResourceLimits resourceLimits = new ResourceLimits(clusterResource);
    QueueCapacityUpdateContext updateContext =
        new QueueCapacityUpdateContext(clusterResource, labelsManager);

    update(queue, updateContext, resourceLimits);
    return updateContext;
  }

  /**
   * Updates the resource and metrics value of the root queue. Root queue always has percentage
   * capacity type and is assigned the cluster resource as its minimum and maximum effective
   * resource.
   * @param rootQueue root queue
   * @param clusterResource cluster resource
   */
  public void updateRoot(CSQueue rootQueue, Resource clusterResource) {
    ResourceLimits resourceLimits = new ResourceLimits(clusterResource);
    QueueCapacityUpdateContext updateContext =
        new QueueCapacityUpdateContext(clusterResource, labelsManager);

    RootCalculationDriver rootCalculationDriver = new RootCalculationDriver(rootQueue,
        updateContext,
        rootCalculator, definedResources);
    rootCalculationDriver.calculateResources();
    rootQueue.refreshAfterResourceCalculation(updateContext.getUpdatedClusterResource(),
        resourceLimits);
  }
  //该方法用于更新某个队列及其子队列的资源容量和度量值。
  //它会对 queue 及其所有子队列进行资源计算，并在计算完成后，调用 updateChildrenAfterCalculation 继续处理子队列的资源调整
  //CSQueue queue：要更新的目标队列，可能是 rootQueue 或其他父级队列
  //updateContext：更新上下文
  //resourceLimits：资源限制对象，表示队列的资源边界（如最小/最大可用资源）
  private void update(
      CSQueue queue, QueueCapacityUpdateContext updateContext, ResourceLimits resourceLimits) {
    //如果 queue 为空，或它没有子队列（意味着不需要再做资源分配），直接返回，不进行后续计算。
    if (queue == null || CollectionUtils.isEmpty(queue.getChildQueues())) {
      return;
    }

    ResourceCalculationDriver resourceCalculationDriver = new ResourceCalculationDriver(
        queue, updateContext, calculators, definedResources);
    resourceCalculationDriver.calculateResources();

    updateChildrenAfterCalculation(resourceCalculationDriver, resourceLimits);
  }
  //是在资源计算完成后，更新父队列的所有子队列的资源容量，并递归地对每个子队列继续进行资源更新。
  // 该方法保证了在父队列的资源变化后，子队列的资源分配会同步更新
  private void updateChildrenAfterCalculation(
      ResourceCalculationDriver resourceCalculationDriver, ResourceLimits resourceLimits) {
    //获取父队列并遍历子队列
    AbstractParentQueue parentQueue = (AbstractParentQueue) resourceCalculationDriver.getQueue();
    for (CSQueue childQueue : parentQueue.getChildQueues()) {
      //更新子队列的容量
      updateQueueCapacities(resourceCalculationDriver, childQueue);
      //获取子队列的资源限制
      ResourceLimits childLimit = parentQueue.getResourceLimitsOfChild(childQueue,
          resourceCalculationDriver.getUpdateContext().getUpdatedClusterResource(),
          resourceLimits, NO_LABEL, false);
      //刷新子队列的资源计算
      childQueue.refreshAfterResourceCalculation(resourceCalculationDriver.getUpdateContext()
              .getUpdatedClusterResource(), childLimit);
      //递归调用更新子队列
      update(childQueue, resourceCalculationDriver.getUpdateContext(), childLimit);
    }
  }

  /**
   * Updates the capacity values of the currently evaluated child.
   * @param queue queue on which the capacities are set
   */
  //是更新指定队列（queue）的容量值。该方法根据计算出的资源信息，通过锁定队列并更新其资源容量，确保线程安全和准确的资源分配。
  // 该方法还根据是否为遗留队列模式（isLegacyQueueMode）来决定使用哪种容量更新逻辑
  private void updateQueueCapacities(
      ResourceCalculationDriver resourceCalculationDriver, CSQueue queue) {
    queue.getWriteLock().lock();
    try {
      for (String label : queue.getConfiguredNodeLabels()) {
        //非遗留队列模式
        if (!isLegacyQueueMode) {
          // Post update capacities based on the calculated effective resource values
          setQueueCapacities(resourceCalculationDriver.getUpdateContext().getUpdatedClusterResource(
              label), queue, label);
        } else {
          // Update capacities according to the legacy logic
          for (ResourceUnitCapacityType capacityType :
              queue.getConfiguredCapacityVector(label).getDefinedCapacityTypes()) {
            AbstractQueueCapacityCalculator calculator = calculators.get(capacityType);
            calculator.updateCapacitiesAfterCalculation(resourceCalculationDriver, queue, label);
          }
        }
      }
    } finally {
      queue.getWriteLock().unlock();
    }
  }

  /**
   * Sets capacity and absolute capacity values of a queue based on minimum and
   * maximum effective resources.
   *
   * @param clusterResource overall cluster resource
   * @param queue child queue for which the capacities are set
   * @param label node label
   */
  // 更新子队列的 capacity（最小资源容量）和 maxCapacity（最大资源容量），以保证调度器按照最新的集群资源状况正确分配资源
  //clusterResource 当前集群的总资源（CPU、内存等）
  public static void setQueueCapacities(Resource clusterResource, CSQueue queue, String label) {
    //只有 AbstractCSQueue 类型的队列才进行处理
    if (!(queue instanceof AbstractCSQueue)) {
      return;
    }
    //如果 csQueue 是 ReservationQueue 或 PlanQueue，但 clusterResource 资源为空（所有值 ≤ 0），则直接返回。
    //这是为了避免 在资源未初始化的情况下修改队列的预留资源
    AbstractCSQueue csQueue = (AbstractCSQueue) queue;
    // Do not override reservations when there are no cluster resources yet
    if ((csQueue instanceof ReservationQueue ||
        csQueue instanceof PlanQueue) &&
        Stream.of(clusterResource.getResources())
            .map(ResourceInformation::getValue)
            .noneMatch(num -> num > 0)) {
      return;
    }

    ResourceCalculator resourceCalculator = csQueue.resourceCalculator;

    CSQueue parent = queue.getParent();
    if (parent == null) {
      return;
    }
    // Update capacity with a double calculated from the parent's minResources
    // and the recently changed queue minResources.
    // capacity = effectiveMinResource / {parent's effectiveMinResource}
    //计算 capacity（最小资源容量）
    //capacity = 子队列最小资源 / 父队列最小资源
    float result = resourceCalculator.divide(clusterResource,
        queue.getQueueResourceQuotas().getEffectiveMinResource(label),
        parent.getQueueResourceQuotas().getEffectiveMinResource(label));
    queue.getQueueCapacities().setCapacity(label,
        Float.isInfinite(result) ? 0 : result);

    // Update maxCapacity with a double calculated from the parent's maxResources
    // and the recently changed queue maxResources.
    // maxCapacity = effectiveMaxResource / parent's effectiveMaxResource
    //计算 maxCapacity（最大资源容量）
    //maxCapacity = 子队列最大资源 / 父队列最大资源
    result = resourceCalculator.divide(clusterResource,
        queue.getQueueResourceQuotas().getEffectiveMaxResource(label),
        parent.getQueueResourceQuotas().getEffectiveMaxResource(label));
    queue.getQueueCapacities().setMaximumCapacity(label,
        Float.isInfinite(result) ? 0 : result);
    //更新队列的绝对容量
    csQueue.updateAbsoluteCapacities();
  }
  //识别 memory-mb 和 vcores 资源类型，并将它们存入 definedResources
  //将其他资源类型也加入 definedResources
  private void loadResourceNames() {
    Set<String> resources = new HashSet<>(ResourceUtils.getResourceTypes().keySet());
    if (resources.contains(MEMORY_URI)) {
      resources.remove(MEMORY_URI);
      definedResources.add(MEMORY_URI);
    }

    if (resources.contains(VCORES_URI)) {
      resources.remove(VCORES_URI);
      definedResources.add(VCORES_URI);
    }

    definedResources.addAll(resources);
  }
}