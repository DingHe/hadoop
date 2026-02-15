/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.preemption;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

// 负责管理可抢占（Preemptable）的资源和容器信息。
// 它主要用于存储和跟踪各个 partition（节点分区）中当前可被抢占的资源及相应的容器信息。
// 该类支持 层级结构，即可以有父级 PreemptableQueue，用于在更高层级（如队列层面）同步抢占资源的信息

public class PreemptableQueue {
  // Partition -> killable resources and containers
  // 存储 各个分区（partition）内可抢占资源的总量
  private Map<String, Resource> totalKillableResources = new HashMap<>();
  //存储 各个分区内具体可被抢占的容器列表
  private Map<String, Map<ContainerId, RMContainer>> killableContainers =
      new HashMap<>();
  //指向 父级队列，用于支持层级管理
  private PreemptableQueue parent;

  public PreemptableQueue(PreemptableQueue parent) {
    this.parent = parent;
  }
  //totalKillableResources：初始化的 可抢占资源总量
  //killableContainers：初始化的可抢占容器信息
  public PreemptableQueue(Map<String, Resource> totalKillableResources,
      Map<String, Map<ContainerId, RMContainer>> killableContainers) {
    this.totalKillableResources = totalKillableResources;
    this.killableContainers = killableContainers;
  }
  //添加可抢占容器
  //container：要添加的 可抢占容器（KillableContainer）
  void addKillableContainer(KillableContainer container) {
    //获取该容器所在的分区
    String partition = container.getNodePartition();
    //如果 totalKillableResources 中没有该分区，说明该分区之前没有可抢占资源，因此：
    //创建空资源对象（Resources.createResource(0)）。
    //创建空的 ConcurrentSkipListMap 来存储容器信息
    if (!totalKillableResources.containsKey(partition)) {
      totalKillableResources.put(partition, Resources.createResource(0));
      killableContainers.put(partition,
          new ConcurrentSkipListMap<ContainerId, RMContainer>());
    }
    //添加容器
    RMContainer c = container.getRMContainer();
    Resources.addTo(totalKillableResources.get(partition),
        c.getAllocatedResource());
    killableContainers.get(partition).put(c.getContainerId(), c);
    //向父级 PreemptableQueue 递归添加
    if (null != parent) {
      parent.addKillableContainer(container);
    }
  }
  //移除可抢占容器
  //container：要移除的 可抢占容器（KillableContainer）
  void removeKillableContainer(KillableContainer container) {
    //获取该容器所在的分区
    String partition = container.getNodePartition();
    Map<ContainerId, RMContainer> partitionKillableContainers =
        killableContainers.get(partition);
    if (partitionKillableContainers != null) {
      RMContainer rmContainer = partitionKillableContainers.remove(
          container.getRMContainer().getContainerId());
      if (null != rmContainer) {
        Resources.subtractFrom(totalKillableResources.get(partition),
            rmContainer.getAllocatedResource());
      }
    }

    if (null != parent) {
      parent.removeKillableContainer(container);
    }
  }

  public Resource getKillableResource(String partition) {
    Resource res = totalKillableResources.get(partition);
    return res == null ? Resources.none() : res;
  }

  public Map<String, Map<ContainerId, RMContainer>> getKillableContainers() {
    return killableContainers;
  }

  Map<String, Resource> getTotalKillableResources() {
    return totalKillableResources;
  }
}
