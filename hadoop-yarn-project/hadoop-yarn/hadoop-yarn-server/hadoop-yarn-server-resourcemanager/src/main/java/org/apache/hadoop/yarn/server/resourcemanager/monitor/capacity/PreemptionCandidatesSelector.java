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

package org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

import org.apache.hadoop.classification.VisibleForTesting;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

//用于选择抢占（Preemption）候选者的一个抽象类。
//在 YARN 资源调度过程中，当某些应用程序超额占用了资源，而其他应用程序无法获得公平分配时，YARN 可能需要触发抢占，释放部分资源供其他应用程序使用
public abstract class PreemptionCandidatesSelector {
  protected CapacitySchedulerPreemptionContext preemptionContext;
  protected ResourceCalculator rc;
  //允许等待被杀死（抢占）的最大时间，单位为毫秒。
  // -1 表示未设置，默认使用 preemptionContext 里的 DefaultMaximumKillWaitTimeout
  private long maximumKillWaitTime = -1;

  PreemptionCandidatesSelector(
      CapacitySchedulerPreemptionContext preemptionContext) {
    this.preemptionContext = preemptionContext;
    this.rc = preemptionContext.getResourceCalculator();
  }

  /**
   * Get preemption candidates from computed resource sharing and already
   * selected candidates.
   *
   * @param selectedCandidates already selected candidates from previous policies
   * @param clusterResource total resource
   * @param totalPreemptedResourceAllowed how many resources allowed to be
   *                                      preempted in this round. Should be
   *                                      updated(in-place set) after the call
   * @return merged selected candidates.
   */
  //用于选择本轮抢占的候选容器。子类需要实现该方法，根据当前资源分配情况、调度策略等信息，挑选需要被抢占的容器
  //selectedCandidates：已经选出的抢占候选者（可能是之前的策略选出的）
  //clusterResource：整个集群的资源总量
  //totalPreemptedResourceAllowed：本轮最多允许被抢占的资源数量，会在方法调用过程中被更新（即调用方会修改这个参数）
  public abstract Map<ApplicationAttemptId, Set<RMContainer>> selectCandidates(
      Map<ApplicationAttemptId, Set<RMContainer>> selectedCandidates,
      Resource clusterResource, Resource totalPreemptedResourceAllowed);

  /**
   * Compare by reversed priority order first, and then reversed containerId
   * order.
   *
   * @param containers list of containers to sort for.
   */
  @VisibleForTesting
  static void sortContainers(List<RMContainer> containers) {
    Collections.sort(containers, new Comparator<RMContainer>() {
      @Override
      public int compare(RMContainer a, RMContainer b) {
        //优先级倒序（priority 降序）：优先级高的容器（SchedulerKey 大的）排在前面
        //Container ID 倒序：在优先级相同的情况下，ID 较大的容器排在前面
        int schedKeyComp = b.getAllocatedSchedulerKey()
            .compareTo(a.getAllocatedSchedulerKey());
        if (schedKeyComp != 0) {
          return schedKeyComp;
        }
        return b.getContainerId().compareTo(a.getContainerId());
      }
    });
  }

  public long getMaximumKillWaitTimeMs() {
    if (maximumKillWaitTime > 0) {
      return maximumKillWaitTime;
    }
    return preemptionContext.getDefaultMaximumKillWaitTimeout();
  }

  public void setMaximumKillWaitTime(long maximumKillWaitTime) {
    this.maximumKillWaitTime = maximumKillWaitTime;
  }
}
