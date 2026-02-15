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

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.QueueCapacityVectorEntry;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueUpdateWarning.QueueUpdateWarningType;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.yarn.api.records.ResourceInformation.MEMORY_URI;

/**
 * Drives the main logic of resource calculation for all children under a queue. Acts as a
 * bookkeeper of disposable update information that is used by all children under the common parent.
 */
//负责计算 YARN CapacityScheduler 中队列的资源分配。它管理并跟踪在一个父队列下的所有子队列的资源计算过程，确保资源按照绝对值、百分比或权重等方式进行合理分配。
// 该类维护多个用于计算的中间状态变量，并调用不同的计算策略来确定每个子队列的最小、最大和有效资源配额
public class ResourceCalculationDriver {
  //定义资源计算的优先级，计算顺序依次是绝对值 (ABSOLUTE) → 百分比 (PERCENTAGE) → 权重 (WEIGHT)
  private static final ResourceUnitCapacityType[] CALCULATOR_PRECEDENCE =
      new ResourceUnitCapacityType[] {
          ResourceUnitCapacityType.ABSOLUTE,
          ResourceUnitCapacityType.PERCENTAGE,
          ResourceUnitCapacityType.WEIGHT};
  //资源单位，表示 MiB。
  static final String MB_UNIT = "Mi";
  //资源舍入策略，用于对计算后的资源值进行四舍五入等操作。
  protected final QueueResourceRoundingStrategy roundingStrategy =
      new DefaultQueueResourceRoundingStrategy(CALCULATOR_PRECEDENCE);
  //该计算驱动器所管理的父队列，即所有计算的资源分配均基于此父队列
  protected final CSQueue queue;
  //队列资源更新的上下文
  protected final QueueCapacityUpdateContext updateContext;
  //资源计算器的映射表，每种资源计算方式 (绝对值、百分比、权重) 均有对应的计算器实现
  protected final Map<ResourceUnitCapacityType, AbstractQueueCapacityCalculator> calculators;
  //定义了需要计算的资源类型（如 CPU、内存等）
  protected final Collection<String> definedResources;
  //整体剩余资源，在整个资源计算周期内跟踪每个 node label (节点标签) 下的可用资源
  protected final Map<String, ResourceVector> overallRemainingResourcePerLabel = new HashMap<>();
  //批次剩余资源，用于在当前计算阶段跟踪每个 node label 下的可用资源，仅在特定计算步骤后更新
  protected final Map<String, ResourceVector> batchRemainingResourcePerLabel = new HashMap<>();
  // Used by ABSOLUTE capacity types
  //归一化资源比率，用于存储子队列配置的绝对资源相对于父队列的最小资源的比率
  protected final Map<String, ResourceVector> normalizedResourceRatioPerLabel = new HashMap<>();
  // Used by WEIGHT capacity types
  //累计权重表，记录每个 node label 下，所有子队列在特定资源上的总权重。
  protected final Map<String, Map<String, Double>> sumWeightsPerLabel = new HashMap<>();
  //当前计算器已使用的资源，记录当前计算阶段已使用的资源总量，以便计算剩余可用资源
  protected Map<String, Double> usedResourceByCurrentCalculatorPerLabel = new HashMap<>();

  public ResourceCalculationDriver(
      CSQueue queue, QueueCapacityUpdateContext updateContext,
      Map<ResourceUnitCapacityType, AbstractQueueCapacityCalculator> calculators,
      Collection<String> definedResources) {
    this.queue = queue;
    this.updateContext = updateContext;
    this.calculators = calculators;
    this.definedResources = definedResources;
  }


  /**
   * Returns the parent that is driving the calculation.
   *
   * @return a common parent queue
   */
  public CSQueue getQueue() {
    return queue;
  }

  /**
   * Returns all the children defined under the driver parent queue.
   *
   * @return child queues
   */
  public Collection<CSQueue> getChildQueues() {
    return queue.getChildQueues();
  }

  /**
   * Returns the context that is used throughout the whole update phase.
   *
   * @return update context
   */
  public QueueCapacityUpdateContext getUpdateContext() {
    return updateContext;
  }

  /**
   * Increments the aggregated weight.
   *
   * @param label        node label
   * @param resourceName resource unit name
   * @param value        weight value
   */
  public void incrementWeight(String label, String resourceName, double value) {
    sumWeightsPerLabel.putIfAbsent(label, new HashMap<>());
    sumWeightsPerLabel.get(label).put(resourceName,
        sumWeightsPerLabel.get(label).getOrDefault(resourceName, 0d) + value);
  }

  /**
   * Returns the aggregated children weights.
   *
   * @param label        node label
   * @param resourceName resource unit name
   * @return aggregated weights of children
   */
  public double getSumWeightsByResource(String label, String resourceName) {
    return sumWeightsPerLabel.get(label).get(resourceName);
  }

  /**
   * Returns the ratio of the summary of children absolute configured resources and the parent's
   * effective minimum resource.
   *
   * @return normalized resource ratio for all labels
   */
  public Map<String, ResourceVector> getNormalizedResourceRatios() {
    return normalizedResourceRatioPerLabel;
  }

  /**
   * Returns the remaining resource ratio under the parent queue. The remaining resource is only
   * decremented after a capacity type is fully evaluated.
   * 返回剩余资源的比例
   * @param label node label
   * @param resourceName name of resource unit
   * @return resource ratio
   */
  public double getRemainingRatioOfResource(String label, String resourceName) {
    return batchRemainingResourcePerLabel.get(label).getValue(resourceName)
        / queue.getEffectiveCapacity(label).getResourceValue(resourceName);
  }

  /**
   * Returns the ratio of the parent queue's effective minimum resource relative to the full cluster
   * resource.
   *
   * @param label node label
   * @param resourceName name of resource unit
   * @return absolute minimum capacity
   */
  public double getParentAbsoluteMinCapacity(String label, String resourceName) {
    return (double) queue.getEffectiveCapacity(label).getResourceValue(resourceName)
        / getUpdateContext().getUpdatedClusterResource(label).getResourceValue(resourceName);
  }

  /**
   * Returns the ratio of the parent queue's effective maximum resource relative to the full cluster
   * resource.
   *
   * @param label node label
   * @param resourceName name of resource unit
   * @return absolute maximum capacity
   */
  public double getParentAbsoluteMaxCapacity(String label, String resourceName) {
    return (double) queue.getEffectiveMaxCapacity(label).getResourceValue(resourceName)
        / getUpdateContext().getUpdatedClusterResource(label).getResourceValue(resourceName);
  }

  /**
   * Returns the remaining resources of a parent that is still available for its
   * children. Decremented only after the calculator is finished its work on the corresponding
   * resources.
   *
   * @param label node label
   * @return remaining resources
   */
  public ResourceVector getBatchRemainingResource(String label) {
    batchRemainingResourcePerLabel.putIfAbsent(label, ResourceVector.newInstance());
    return batchRemainingResourcePerLabel.get(label);
  }

  /**
   * Calculates and sets the minimum and maximum effective resources for all children under the
   * parent queue with which this driver was initialized.
   */
  //主要作用是计算并设置父队列下所有子队列的最小和最大有效资源。这是 Apache YARN CapacityScheduler 中用于资源分配的核心逻辑之一。
  // 它确保子队列按照特定的计算优先级分配资源，并根据不同的计算类型（绝对值、百分比、权重）对资源进行调整
  public void calculateResources() {
    // Reset both remaining resource storage to the parent's available resource
    //重置剩余资源存储
    for (String label : queue.getConfiguredNodeLabels()) {
      overallRemainingResourcePerLabel.put(label,
          ResourceVector.of(queue.getEffectiveCapacity(label)));//表示初始化时，剩余资源等于队列的 有效容量 (effectiveCapacity)
      batchRemainingResourcePerLabel.put(label,
          ResourceVector.of(queue.getEffectiveCapacity(label)));
    }
    //计算资源的先决条件
    for (AbstractQueueCapacityCalculator capacityCalculator : calculators.values()) {
      capacityCalculator.calculateResourcePrerequisites(this);
    }

    for (String resourceName : definedResources) {
      for (ResourceUnitCapacityType capacityType : CALCULATOR_PRECEDENCE) {
        for (CSQueue childQueue : getChildQueues()) {
          CalculationContext context = new CalculationContext(resourceName, capacityType,
              childQueue);
          //计算 当前子队列 的资源分配情况
          calculateResourceOnChild(context);
        }

        // Flush aggregated used resource by labels at the end of a calculator phase
        //刷新计算后的资源
        for (Map.Entry<String, Double> entry : usedResourceByCurrentCalculatorPerLabel.entrySet()) {
          //遍历 所有已使用的资源记录，从 batchRemainingResourcePerLabel（批量资源池）中扣除已分配的资源，确保当前计算阶段后剩余资源是正确的
          batchRemainingResourcePerLabel.get(entry.getKey()).decrement(resourceName,
              entry.getValue());
        }
        //不同计算阶段（绝对值、百分比、权重）使用不同的计算策略，因此每次计算完 一个阶段 后，清空已使用的资源记录，防止影响后续计算
        usedResourceByCurrentCalculatorPerLabel = new HashMap<>();
      }
    }
    //检查 batchRemainingResourcePerLabel是否全部被分配完
    validateRemainingResource();
  }
  //主要功能是 计算并分配子队列的资源，确保资源按照正确的优先级和策略进行分配，并更新 全局资源跟踪器。
  // 它是 calculateResources 方法的一部分，负责 对子队列执行具体的资源计算
  private void calculateResourceOnChild(CalculationContext context) {
    context.getQueue().getWriteLock().lock();
    try {
      for (String label : context.getQueue().getConfiguredNodeLabels()) {
        //检查当前资源是否适用于该队列
        if (!context.getQueue().getConfiguredCapacityVector(label).isResourceOfType(
            context.getResourceName(), context.getCapacityType())) {
          continue;
        }
        //检查全局剩余资源是否包含该标签
        if (!overallRemainingResourcePerLabel.containsKey(label)) {
          continue;
        }
        //分配资源给子队列
        double usedResourceByChild = setChildResources(context, label);
        double aggregatedUsedResource = usedResourceByCurrentCalculatorPerLabel.getOrDefault(label,
            0d);
        double resourceUsedByLabel = aggregatedUsedResource + usedResourceByChild;

        overallRemainingResourcePerLabel.get(label).decrement(context.getResourceName(),
            usedResourceByChild);
        usedResourceByCurrentCalculatorPerLabel.put(label, resourceUsedByLabel);
      }
    } finally {
      context.getQueue().getWriteLock().unlock();
    }
  }
  //计算子队列的资源分配，并返回 子队列的最小资源量 (minimum resource)。主要功能包括：
  //获取子队列的 最小/最大资源容量 信息。
  //使用适当的 计算策略 计算 最小/最大资源。
  //四舍五入 资源数值，确保符合容量要求。
  //验证计算结果，防止超配或违背约束。
  //更新队列的最小/最大资源配额

  private double setChildResources(CalculationContext context, String label) {
    //获取当前队列的资源容量信息（最小资源容量）
    QueueCapacityVectorEntry capacityVectorEntry = context.getQueue().getConfiguredCapacityVector(
        label).getResource(context.getResourceName());
    //获取 当前队列的最大资源容量信息
    QueueCapacityVectorEntry maximumCapacityVectorEntry = context.getQueue()
        .getConfiguredMaxCapacityVector(label).getResource(context.getResourceName());
    //获取最大资源的计算器
    AbstractQueueCapacityCalculator maximumCapacityCalculator = calculators.get(
        maximumCapacityVectorEntry.getVectorResourceType());
    //计算最小/最大资源
    double minimumResource =
        calculators.get(context.getCapacityType()).calculateMinimumResource(this, context, label);
    double maximumResource = maximumCapacityCalculator.calculateMaximumResource(this, context,
        label);
    //资源四舍五入
    minimumResource = roundingStrategy.getRoundedResource(minimumResource, capacityVectorEntry);
    maximumResource = roundingStrategy.getRoundedResource(maximumResource,
        maximumCapacityVectorEntry);
    Pair<Double, Double> resources = validateCalculatedResources(context, label,
        new ImmutablePair<>(
        minimumResource, maximumResource));
    minimumResource = resources.getLeft();
    maximumResource = resources.getRight();
    //验证计算结果
    context.getQueue().getQueueResourceQuotas().getEffectiveMinResource(label).setResourceValue(
        context.getResourceName(), (long) minimumResource);
    context.getQueue().getQueueResourceQuotas().getEffectiveMaxResource(label).setResourceValue(
        context.getResourceName(), (long) maximumResource);

    return minimumResource;
  }

  private Pair<Double, Double> validateCalculatedResources(CalculationContext context,
      String label, Pair<Double, Double> calculatedResources) {
    double minimumResource = calculatedResources.getLeft();
    long minimumMemoryResource =
        context.getQueue().getQueueResourceQuotas().getEffectiveMinResource(label).getMemorySize();

    double remainingResourceUnderParent = overallRemainingResourcePerLabel.get(label).getValue(
        context.getResourceName());

    long parentMaximumResource = queue.getEffectiveMaxCapacity(label).getResourceValue(
        context.getResourceName());
    double maximumResource = calculatedResources.getRight();

    // Memory is the primary resource, if its zero, all other resource units are zero as well.
    if (!context.getResourceName().equals(MEMORY_URI) && minimumMemoryResource == 0) {
      minimumResource = 0;
    }

    if (maximumResource != 0 && maximumResource > parentMaximumResource) {
      updateContext.addUpdateWarning(QueueUpdateWarningType.QUEUE_MAX_RESOURCE_EXCEEDS_PARENT
          .ofQueue(context.getQueue().getQueuePath()));
    }
    maximumResource = maximumResource == 0 ? parentMaximumResource : Math.min(maximumResource,
        parentMaximumResource);

    if (maximumResource < minimumResource) {
      updateContext.addUpdateWarning(QueueUpdateWarningType.QUEUE_EXCEEDS_MAX_RESOURCE.ofQueue(
          context.getQueue().getQueuePath()));
      minimumResource = maximumResource;
    }

    if (minimumResource > remainingResourceUnderParent) {
      // Legacy auto queues are assigned a zero resource if not enough resource is left
      if (queue instanceof ManagedParentQueue) {
        minimumResource = 0;
      } else {
        updateContext.addUpdateWarning(
            QueueUpdateWarningType.QUEUE_OVERUTILIZED.ofQueue(
                context.getQueue().getQueuePath()).withInfo(
                    "Resource name: " + context.getResourceName() +
                        " resource value: " + minimumResource));
        minimumResource = remainingResourceUnderParent;
      }
    }

    if (minimumResource == 0) {
      updateContext.addUpdateWarning(QueueUpdateWarningType.QUEUE_ZERO_RESOURCE.ofQueue(
          context.getQueue().getQueuePath())
          .withInfo("Resource name: " + context.getResourceName()));
    }

    return new ImmutablePair<>(minimumResource, maximumResource);
  }

  private void validateRemainingResource() {
    for (String label : queue.getConfiguredNodeLabels()) {
      if (!batchRemainingResourcePerLabel.get(label).equals(ResourceVector.newInstance())) {
        updateContext.addUpdateWarning(QueueUpdateWarningType.BRANCH_UNDERUTILIZED.ofQueue(
            queue.getQueuePath()).withInfo("Label: " + label));
      }
    }
  }
}
