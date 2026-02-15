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

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType;
import org.apache.hadoop.yarn.util.UnitsConversionUtil;

import java.util.Map;

import static org.apache.hadoop.yarn.api.records.ResourceInformation.MEMORY_URI;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueUpdateWarning.QueueUpdateWarningType.BRANCH_DOWNSCALED;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ResourceCalculationDriver.MB_UNIT;
//AbsoluteResourceCapacityCalculator 是 Apache YARN 容量调度器 (CapacityScheduler) 计算资源容量的实现类，主要作用是：
//基于 绝对资源 (ABSOLUTE 类型) 计算队列的最小/最大资源。
//在资源计算前设置归一化资源比例，用于调整子队列资源的计算方式。
//计算和更新队列的资源容量，确保计算结果能够被调度器正确应用。

public class AbsoluteResourceCapacityCalculator extends AbstractQueueCapacityCalculator {
  //在计算资源前，设置队列的 归一化资源比例
  //归一化的作用是 防止子队列总资源超过父队列的资源限制
  @Override
  public void calculateResourcePrerequisites(ResourceCalculationDriver resourceCalculationDriver) {
    setNormalizedResourceRatio(resourceCalculationDriver);
  }
  //用于 计算队列的最小资源容量，确保分配的资源不会超过可用资源，并且按照 归一化比例 和 剩余资源比例 进行调整
  @Override
  public double calculateMinimumResource(
      ResourceCalculationDriver resourceCalculationDriver, CalculationContext context,
      String label) {
    //获取当前计算的资源类型
    String resourceName = context.getResourceName();
    //获取 父队列的归一化资源比例（防止子队列总资源超过父队列）
    double normalizedRatio = resourceCalculationDriver.getNormalizedResourceRatios().getOrDefault(
        label, ResourceVector.of(1)).getValue(resourceName);
    //获取剩余资源比例
    double remainingResourceRatio = resourceCalculationDriver.getRemainingRatioOfResource(
        label, resourceName);

    return normalizedRatio * remainingResourceRatio * context.getCurrentMinimumCapacityEntry(
        label).getResourceValue();
  }

  @Override
  public double calculateMaximumResource(
      ResourceCalculationDriver resourceCalculationDriver, CalculationContext context,
      String label) {
    return context.getCurrentMaximumCapacityEntry(label).getResourceValue();
  }
  //在计算完有效资源后，更新队列的资源容量
  @Override
  public void updateCapacitiesAfterCalculation(
      ResourceCalculationDriver resourceCalculationDriver, CSQueue queue, String label) {
    CapacitySchedulerQueueCapacityHandler.setQueueCapacities(
        resourceCalculationDriver.getUpdateContext()
            .getUpdatedClusterResource(label), queue, label);
  }

  @Override
  public ResourceUnitCapacityType getCapacityType() {
    return ResourceUnitCapacityType.ABSOLUTE;
  }

  /**
   * Calculates the normalized resource ratio of a parent queue, under which children are defined
   * with absolute capacity type. If the effective resource of the parent is less, than the
   * aggregated configured absolute resource of its children, the resource ratio will be less,
   * than 1.
   *
   * @param calculationDriver the driver, which contains the parent queue that will form the base
   *                          of the normalization calculation
   */
  // 用于计算父队列的归一化资源比例，确保子队列的资源总和不会超过父队列的有效资源。
  // 如果子队列的配置资源总量 大于 父队列的可用资源，则需要 缩小比例，保证资源分配合理
  public static void setNormalizedResourceRatio(ResourceCalculationDriver calculationDriver) {
    //当前计算的父队列
    CSQueue queue = calculationDriver.getQueue();
    //每个标签对应一组 计算资源 (CPU、内存等)，需要分别计算 归一化比例
    for (String label : queue.getConfiguredNodeLabels()) {
      // ManagedParents assign zero capacity to queues in case of overutilization, downscaling is
      // turned off for their children
      //ManagedParentQueue 在资源超配时会 直接分配 0 资源，因此 无需归一化
      if (queue instanceof ManagedParentQueue) {
        return;
      }
      //遍历资源类型
      //resourceName 表示 资源类型，例如：
      //"memory-mb" (内存)
      //"vcores" (CPU 核心数)
      //"gpu" (GPU 数量)
      for (String resourceName : queue.getConfiguredCapacityVector(label).getResourceNames()) {
        //所有子队列配置的最小资源总和
        long childrenConfiguredResource = 0;
        //父队列的最小有效资源
        long effectiveMinResource = queue.getQueueResourceQuotas().getEffectiveMinResource(
            label).getResourceValue(resourceName);

        // Total configured min resources of direct children of the queue
        //计算所有子队列的资源总和
        for (CSQueue childQueue : queue.getChildQueues()) {
          if (!childQueue.getConfiguredNodeLabels().contains(label)) {
            continue;
          }
          QueueCapacityVector capacityVector = childQueue.getConfiguredCapacityVector(label);
          if (capacityVector.isResourceOfType(resourceName, ResourceUnitCapacityType.ABSOLUTE)) {
            childrenConfiguredResource += capacityVector.getResource(resourceName)
                .getResourceValue();
          }
        }
        // If no children is using ABSOLUTE capacity type, normalization is not needed
        if (childrenConfiguredResource == 0) {
          continue;
        }
        // Factor to scale down effective resource: When cluster has sufficient
        // resources, effective_min_resources will be same as configured
        // min_resources.
        float numeratorForMinRatio = childrenConfiguredResource;
        if (effectiveMinResource < childrenConfiguredResource) {
          //如果 父队列的 有效最小资源 (effectiveMinResource) 小于子队列资源总和：
          //numeratorForMinRatio = effectiveMinResource（防止超配）
          numeratorForMinRatio = queue.getQueueResourceQuotas().getEffectiveMinResource(label)
              .getResourceValue(resourceName);
          calculationDriver.getUpdateContext().addUpdateWarning(BRANCH_DOWNSCALED.ofQueue(
              queue.getQueuePath()));
        }
        //资源单位转换
        String unit = resourceName.equals(MEMORY_URI) ? MB_UNIT : "";
        long convertedValue = UnitsConversionUtil.convert(unit, calculationDriver.getUpdateContext()
            .getUpdatedClusterResource(label).getResourceInformation(resourceName).getUnits(),
            childrenConfiguredResource);
        //存储归一化比例，也就是父队列/子队列资源总和，后面的子队列都要通过这个因子归一化
        //归一化的因子设置到ResourceCalculationDriver的 normalizedResourceRatioPerLabel
        if (convertedValue != 0) {
          Map<String, ResourceVector> normalizedResourceRatios =
              calculationDriver.getNormalizedResourceRatios();
          normalizedResourceRatios.putIfAbsent(label, ResourceVector.newInstance());
          normalizedResourceRatios.get(label).setValue(resourceName, numeratorForMinRatio /
              convertedValue);
        }
      }
    }
  }
}
