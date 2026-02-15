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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * Resource limits for queues/applications, this means max overall (please note
 * that, it's not "extra") resource you can get.
 */
//资源调度器中队列或应用的资源限制，它主要用于控制一个调度单元（如队列或应用）可以获取的最大资源量。它的核心作用包括：
//设定 资源限制（limit），即最大可用资源。
//设定 预留回收资源量（amountNeededUnreserve），用于计算需要释放多少预留资源来满足新分配。
//设定 可用资源（headroom），表示当前可用于分配的资源量。
//设定 被阻塞的资源（blockedHeadroom），表示因高优先级队列而需要保留的资源。
//控制 是否允许抢占（allowPreempt），即是否允许强制回收资源来满足新需求。
//这个类通常用于 队列调度（如 CapacityScheduler），它帮助调度器管理和计算资源的可用性、预留以及资源回收策略
public class ResourceLimits {
  //代表当前调度单元（队列或应用）可以使用的 最大资源限制
  private volatile Resource limit;

  // This is special limit that goes with the RESERVE_CONT_LOOK_ALL_NODES
  // config. This limit indicates how much we need to unreserve to allocate
  // another container.
  //表示 需要取消预留（unreserve）的资源量，以便满足新的资源请求
  private volatile Resource amountNeededUnreserve;

  // How much resource you can use for next allocation, if this isn't enough for
  // next container allocation, you may need to consider unreserve some
  // containers.
  //表示当前调度单元 还能使用多少资源，即 limit - 已使用资源
  private volatile Resource headroom;

  // How much resource should be reserved for high-priority blocked queues
  //表示 为高优先级队列或任务预留的资源，这些资源不允许被低优先级任务使用
  private Resource blockedHeadroom;
  //控制是否允许 资源抢占（Preemption），即能否强制回收其他低优先级任务的资源来满足新的高优先级任务需求
  private boolean allowPreempt = false;

  public ResourceLimits(Resource limit) {
    this(limit, Resources.none());
  }

  public ResourceLimits(Resource limit, Resource amountNeededUnreserve) {
    this.amountNeededUnreserve = amountNeededUnreserve;
    this.headroom = limit;
    this.limit = limit;
  }

  public Resource getLimit() {
    return limit;
  }

  public Resource getHeadroom() {
    return headroom;
  }

  public void setHeadroom(Resource headroom) {
    this.headroom = headroom;
  }

  public Resource getAmountNeededUnreserve() {
    return amountNeededUnreserve;
  }

  public void setLimit(Resource limit) {
    this.limit = limit;
  }

  public void setAmountNeededUnreserve(Resource amountNeededUnreserve) {
    this.amountNeededUnreserve = amountNeededUnreserve;
  }

  public boolean isAllowPreemption() {
    return allowPreempt;
  }

  public void setIsAllowPreemption(boolean allowPreempt) {
   this.allowPreempt = allowPreempt;
  }

  public void addBlockedHeadroom(Resource resource) {
    if (blockedHeadroom == null) {
      blockedHeadroom = Resource.newInstance(0, 0);
    }
    Resources.addTo(blockedHeadroom, resource);
  }

  public Resource getBlockedHeadroom() {
    if (blockedHeadroom == null) {
      return Resources.none();
    }
    return blockedHeadroom;
  }

  public Resource getNetLimit() {
    if (blockedHeadroom != null) {
      return Resources.subtract(limit, blockedHeadroom);
    }
    return limit;
  }
}
