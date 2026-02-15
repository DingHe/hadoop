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


import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.Resources;


/**
 * Abstract temporary data-structure for tracking resource availability,pending
 * resource need, current utilization for app/queue.
 */
//用于跟踪资源可用性、待分配资源需求以及当前应用或队列资源利用情况的临时数据结构。
// 它在 Apache YARN 的资源管理和抢占调度中起着重要作用，主要用于在候选选择策略中对资源的需求、预分配、已占用资源等进行管理和调整
public class AbstractPreemptionEntity {
  // Following fields are copied from scheduler
  //表示该实体所对应的队列名称
  final String queueName;
  //表示当前队列或应用已经使用的资源
  protected final Resource current;
  //表示该队列或应用的 ApplicationMaster (AM) 已经使用的资源。通常，AM 是 YARN 中应用的资源管理器
  protected final Resource amUsed;
  //表示已为该队列或应用预留的资源。预留的资源通常在某些条件下待用，但还没有被实际分配
  protected final Resource reserved;
  //表示该队列或应用请求的资源，但尚未被分配的资源。它通常表示待处理的资源需求
  protected Resource pending;

  // Following fields are settled and used by candidate selection policies
  //表示理想情况下该队列或应用应当被分配的资源量。它是根据调度策略计算出来的预期资源
  Resource idealAssigned;
  //表示该队列或应用计划要被抢占的资源量
  Resource toBePreempted;
  //表示已选择进行抢占的资源量。它可能会基于调度策略和资源需求来进行调整
  Resource selected;
  //表示实际要抢占的资源量。这通常是经过调度决策后的最终资源抢占量
  private Resource actuallyToBePreempted;
  //表示该队列或应用需要从其他队列或应用中抢占的资源量
  private Resource toBePreemptFromOther;

  AbstractPreemptionEntity(String queueName, Resource usedPerPartition,
      Resource amUsedPerPartition, Resource reserved,
      Resource pendingPerPartition) {
    this.queueName = queueName;
    this.current = usedPerPartition;
    this.pending = pendingPerPartition;
    this.reserved = reserved;
    this.amUsed = amUsedPerPartition;

    this.idealAssigned = Resource.newInstance(0, 0);
    this.actuallyToBePreempted = Resource.newInstance(0, 0);
    this.toBePreempted = Resource.newInstance(0, 0);
    this.toBePreemptFromOther = Resource.newInstance(0, 0);
    this.selected = Resource.newInstance(0, 0);
  }

  public String getQueueName() {
    return queueName;
  }

  public Resource getUsed() {
    return current;
  }

  public Resource getUsedDeductAM() {
    return Resources.subtract(current, amUsed);
  }

  public Resource getAMUsed() {
    return amUsed;
  }

  public Resource getPending() {
    return pending;
  }

  public Resource getReserved() {
    return reserved;
  }

  public Resource getActuallyToBePreempted() {
    return actuallyToBePreempted;
  }

  public void setActuallyToBePreempted(Resource actuallyToBePreempted) {
    this.actuallyToBePreempted = actuallyToBePreempted;
  }

  public Resource getToBePreemptFromOther() {
    return toBePreemptFromOther;
  }

  public void setToBePreemptFromOther(Resource toBePreemptFromOther) {
    this.toBePreemptFromOther = toBePreemptFromOther;
  }

}
