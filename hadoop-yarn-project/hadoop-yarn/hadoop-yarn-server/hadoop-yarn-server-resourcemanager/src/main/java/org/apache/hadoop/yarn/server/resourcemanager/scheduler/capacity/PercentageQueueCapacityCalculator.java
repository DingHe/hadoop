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

//用于按照 百分比方式 计算队列（queue）的最小和最大资源容量。
//该类继承自 AbstractQueueCapacityCalculator，并且：
//计算最小资源（calculateMinimumResource）：根据父队列的 最小绝对容量 计算子队列的 最小资源值。
//计算最大资源（calculateMaximumResource）：根据父队列的 最大绝对容量 计算子队列的 最大资源值。
//更新计算后的容量（updateCapacitiesAfterCalculation）：在计算完成后，更新 CSQueue 的绝对容量。
//返回计算类型（getCapacityType）：指定该计算器是 基于百分比（PERCENTAGE） 进行容量计算。

public class PercentageQueueCapacityCalculator extends AbstractQueueCapacityCalculator {
  //计算最小资源
  @Override
  public double calculateMinimumResource(
      ResourceCalculationDriver resourceCalculationDriver, CalculationContext context,
      String label) {
    //获取资源名称：
    String resourceName = context.getResourceName();
    //获取父队列的最小绝对容量
    double parentAbsoluteCapacity = resourceCalculationDriver.getParentAbsoluteMinCapacity(label,
        resourceName);
    //获取剩余资源比例
    double remainingPerEffectiveResourceRatio =
        resourceCalculationDriver.getRemainingRatioOfResource(label, resourceName);
    //计算子队列的最小资源容量（百分比转换）
    double absoluteCapacity = parentAbsoluteCapacity * remainingPerEffectiveResourceRatio
        * context.getCurrentMinimumCapacityEntry(label).getResourceValue() / 100;

    return resourceCalculationDriver.getUpdateContext().getUpdatedClusterResource(label)
        .getResourceValue(resourceName) * absoluteCapacity;
  }

  @Override
  public double calculateMaximumResource(
      ResourceCalculationDriver resourceCalculationDriver, CalculationContext context,
      String label) {
    String resourceName = context.getResourceName();

    double parentAbsoluteMaxCapacity =
        resourceCalculationDriver.getParentAbsoluteMaxCapacity(label, resourceName);
    double absoluteMaxCapacity = parentAbsoluteMaxCapacity
        * context.getCurrentMaximumCapacityEntry(label).getResourceValue() / 100;

    return resourceCalculationDriver.getUpdateContext().getUpdatedClusterResource(label)
        .getResourceValue(resourceName) * absoluteMaxCapacity;
  }

  @Override
  public void calculateResourcePrerequisites(ResourceCalculationDriver resourceCalculationDriver) {

  }

  @Override
  public void updateCapacitiesAfterCalculation(ResourceCalculationDriver resourceCalculationDriver,
      CSQueue queue, String label) {
    ((AbstractCSQueue) queue).updateAbsoluteCapacities();
  }

  @Override
  public ResourceUnitCapacityType getCapacityType() {
    return ResourceUnitCapacityType.PERCENTAGE;
  }
}
