/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.UNDEFINED;

/**
 * This class determines minimum and maximum allocation settings based on the
 * {@link CapacitySchedulerConfiguration} and other queue
 * properties.
 **/
//负责管理 YARN CapacityScheduler（容量调度器）中队列的最小和最大资源分配
//存储队列的最小资源分配（minimumAllocation）
//计算队列的最大资源分配（maximumAllocation），并确保其不会超出集群最大资源限制
public class QueueAllocationSettings {
  //存储该队列允许的最小资源分配
  private final Resource minimumAllocation;
  //存储该队列允许的最大资源分配
  private Resource maximumAllocation;

  public QueueAllocationSettings(Resource minimumAllocation) {
    this.minimumAllocation = minimumAllocation;
  }
  //根据 CapacitySchedulerConfiguration（调度器配置）计算该队列的最大资源分配
  void setupMaximumAllocation(CapacitySchedulerConfiguration configuration, String queuePath,
      CSQueue parent) {
    //获取集群最大资源分配
    Resource clusterMax = ResourceUtils
        .fetchMaximumAllocationFromConfig(configuration);
    //获取当前队列最大资源限制
    Resource queueMax = configuration.getQueueMaximumAllocation(queuePath);

    maximumAllocation = Resources.clone(
        parent == null ? clusterMax : parent.getMaximumAllocation());

    String errMsg =
        "Queue maximum allocation cannot be larger than the cluster setting"
            + " for queue " + queuePath
            + " max allocation per queue: %s"
            + " cluster setting: " + clusterMax;

    if (queueMax == Resources.none()) {
      // Handle backward compatibility
      long queueMemory = configuration.getQueueMaximumAllocationMb(queuePath);
      int queueVcores = configuration.getQueueMaximumAllocationVcores(queuePath);
      if (queueMemory != UNDEFINED) {
        maximumAllocation.setMemorySize(queueMemory);
      }

      if (queueVcores != UNDEFINED) {
        maximumAllocation.setVirtualCores(queueVcores);
      }

      if ((queueMemory != UNDEFINED && queueMemory > clusterMax.getMemorySize()
          || (queueVcores != UNDEFINED
          && queueVcores > clusterMax.getVirtualCores()))) {
        throw new IllegalArgumentException(
            String.format(errMsg, maximumAllocation));
      }
    } else {
      // Queue level maximum-allocation can't be larger than cluster setting
      for (ResourceInformation ri : queueMax.getResources()) {
        if (ri.compareTo(clusterMax.getResourceInformation(ri.getName())) > 0) {
          throw new IllegalArgumentException(String.format(errMsg, queueMax));
        }

        maximumAllocation.setResourceInformation(ri.getName(), ri);
      }
    }
  }

  public Resource getMinimumAllocation() {
    return minimumAllocation;
  }

  public Resource getMaximumAllocation() {
    return maximumAllocation;
  }
}
