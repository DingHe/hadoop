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

import java.util.Collection;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType.WEIGHT;

//用于基于 权重（Weight） 的策略计算队列的 最小资源 和 最大资源
public class WeightQueueCapacityCalculator extends AbstractQueueCapacityCalculator {
  //计算资源前置条件
  @Override
  public void calculateResourcePrerequisites(ResourceCalculationDriver resourceCalculationDriver) {
    // Precalculate the summary of children's weight
    for (CSQueue childQueue : resourceCalculationDriver.getChildQueues()) {
      for (String label : childQueue.getConfiguredNodeLabels()) {
        for (String resourceName : childQueue.getConfiguredCapacityVector(label)
            .getResourceNamesByCapacityType(getCapacityType())) {
          //获取资源名称和每个资源的权重值
          //将每个资源的权重累加到 resourceCalculationDriver 中，确保资源分配时考虑到权重
          resourceCalculationDriver.incrementWeight(label, resourceName, childQueue
              .getConfiguredCapacityVector(label).getResource(resourceName).getResourceValue());
        }
      }
    }
  }
  //计算最小资源
  @Override
  public double calculateMinimumResource(ResourceCalculationDriver resourceCalculationDriver,
                                        CalculationContext context,
                                        String label) {
    //获取资源名称
    String resourceName = context.getResourceName();
    //计算标准化权重
    //标准化权重通过当前队列的最小容量除以子队列权重总和，得出当前队列占比
    double normalizedWeight = context.getCurrentMinimumCapacityEntry(label).getResourceValue() /
        resourceCalculationDriver.getSumWeightsByResource(label, resourceName);

    double remainingResource = resourceCalculationDriver.getBatchRemainingResource(label)
        .getValue(resourceName);

    // Due to rounding loss it is better to use all remaining resources if no other resource uses
    // weight
    if (normalizedWeight == 1) {
      return remainingResource;
    }

    double remainingResourceRatio = resourceCalculationDriver.getRemainingRatioOfResource(
        label, resourceName);
    double parentAbsoluteCapacity = resourceCalculationDriver.getParentAbsoluteMinCapacity(
        label, resourceName);
    double queueAbsoluteCapacity = parentAbsoluteCapacity * remainingResourceRatio
        * normalizedWeight;

    return resourceCalculationDriver.getUpdateContext()
        .getUpdatedClusterResource(label).getResourceValue(resourceName) * queueAbsoluteCapacity;
  }

  @Override
  public double calculateMaximumResource(ResourceCalculationDriver resourceCalculationDriver,
                                        CalculationContext context,
                                        String label) {
    throw new IllegalStateException("Resource " + context.getCurrentMinimumCapacityEntry(
        label).getResourceName() +
        " has " + "WEIGHT maximum capacity type, which is not supported");
  }

  @Override
  public ResourceUnitCapacityType getCapacityType() {
    return WEIGHT;
  }

  @Override
  public void updateCapacitiesAfterCalculation(
      ResourceCalculationDriver resourceCalculationDriver, CSQueue queue, String label) {
    double sumCapacityPerResource = 0f;

    Collection<String> resourceNames = getResourceNames(queue, label);
    for (String resourceName : resourceNames) {
      double sumBranchWeight = resourceCalculationDriver.getSumWeightsByResource(label,
          resourceName);
      double capacity =  queue.getConfiguredCapacityVector(
          label).getResource(resourceName).getResourceValue() / sumBranchWeight;
      sumCapacityPerResource += capacity;
    }

    queue.getQueueCapacities().setNormalizedWeight(label,
        (float) (sumCapacityPerResource / resourceNames.size()));
    ((AbstractCSQueue) queue).updateAbsoluteCapacities();
  }
}
