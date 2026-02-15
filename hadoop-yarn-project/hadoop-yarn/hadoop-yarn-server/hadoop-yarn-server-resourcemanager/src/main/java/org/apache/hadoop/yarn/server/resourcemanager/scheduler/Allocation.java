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

import java.util.List;
import java.util.Set;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.RejectedSchedulingRequest;
// YARN 资源管理器（ResourceManager）调度器（Scheduler）中用于表示资源分配结果的类。
// 它封装了调度器为应用程序分配的容器（Container）、资源请求的状态、拒绝的调度请求等信息。
//该类的主要作用包括：
//存储调度结果：包含新分配的容器、被提升/降级的容器、资源增加/减少的容器等信息。
//管理资源限制：跟踪该次调度的资源上限，确保调度器不会超配资源。
//处理调度请求：包括严格匹配的容器、可变匹配的容器（Fungible Containers）、被拒绝的资源请求等。
//支持 NodeManager 交互：维护 NMToken（NodeManager 令牌），用于节点之间的认证和交互

public class Allocation {

  final List<Container> containers;//成功分配的容器列表，表示调度器本次为应用程序分配的资源
  final Set<ContainerId> strictContainers;//严格分配的容器ID集合，必须完全满足用户需求的容器
  final Set<ContainerId> fungibleContainers;//可变（Fungible）分配的容器 ID 集合，可以调整资源配置以满足调度需求的容器
  final List<ResourceRequest> fungibleResources;//可变资源请求，表示可以灵活调整的资源需求列表
  final List<NMToken> nmTokens;//NodeManager 令牌列表，用于应用程序与 NodeManager 之间的安全通信
  final List<Container> increasedContainers;//资源增加的容器列表，表示调度器为某些容器增加了资源
  final List<Container> decreasedContainers;//资源减少的容器列表，表示调度器为某些容器减少了资源
  final List<Container> promotedContainers;//被提升（Promoted）的容器列表，用于动态调整容器的优先级
  final List<Container> demotedContainers;//被降级（Demoted）的容器列表，表示由于资源紧张或策略调整，某些容器被降低了优先级
  private final List<Container> previousAttemptContainers;//前一次尝试（Attempt）的容器列表，表示在应用程序的前一个尝试中分配的容器
  private Resource resourceLimit;//当前调度周期的资源限制，确保调度不会超配资源
  private List<RejectedSchedulingRequest> rejectedRequest;//被拒绝的调度请求列表，记录了调度过程中未能满足的资源请求

  public Allocation(List<Container> containers, Resource resourceLimit,
      Set<ContainerId> strictContainers, Set<ContainerId> fungibleContainers,
      List<ResourceRequest> fungibleResources) {
    this(containers,  resourceLimit,strictContainers,  fungibleContainers,
      fungibleResources, null);
  }

  public Allocation(List<Container> containers, Resource resourceLimit,
      Set<ContainerId> strictContainers, Set<ContainerId> fungibleContainers,
      List<ResourceRequest> fungibleResources, List<NMToken> nmTokens) {
    this(containers, resourceLimit, strictContainers, fungibleContainers,
        fungibleResources, nmTokens, null, null, null, null, null, null);
  }

  public Allocation(List<Container> containers, Resource resourceLimit,
      Set<ContainerId> strictContainers, Set<ContainerId> fungibleContainers,
      List<ResourceRequest> fungibleResources, List<NMToken> nmTokens,
      List<Container> increasedContainers, List<Container> decreasedContainer) {
    this(containers, resourceLimit, strictContainers, fungibleContainers,
        fungibleResources, nmTokens, increasedContainers, decreasedContainer,
        null, null, null, null);
  }

  public Allocation(List<Container> containers, Resource resourceLimit,
      Set<ContainerId> strictContainers, Set<ContainerId> fungibleContainers,
      List<ResourceRequest> fungibleResources, List<NMToken> nmTokens,
      List<Container> increasedContainers, List<Container> decreasedContainer,
      List<Container> promotedContainers, List<Container> demotedContainer,
      List<Container> previousAttemptContainers, List<RejectedSchedulingRequest>
      rejectedRequest) {
    this.containers = containers;
    this.resourceLimit = resourceLimit;
    this.strictContainers = strictContainers;
    this.fungibleContainers = fungibleContainers;
    this.fungibleResources = fungibleResources;
    this.nmTokens = nmTokens;
    this.increasedContainers = increasedContainers;
    this.decreasedContainers = decreasedContainer;
    this.promotedContainers = promotedContainers;
    this.demotedContainers = demotedContainer;
    this.previousAttemptContainers = previousAttemptContainers;
    this.rejectedRequest = rejectedRequest;
  }

  public List<Container> getContainers() {
    return containers;
  }

  public Resource getResourceLimit() {
    return resourceLimit;
  }

  public Set<ContainerId> getStrictContainerPreemptions() {
    return strictContainers;
  }

  public Set<ContainerId> getContainerPreemptions() {
    return fungibleContainers;
  }

  public List<ResourceRequest> getResourcePreemptions() {
    return fungibleResources;
  }

  public List<NMToken> getNMTokens() {
    return nmTokens;
  }
  
  public List<Container> getIncreasedContainers() {
    return increasedContainers;
  }
  
  public List<Container> getDecreasedContainers() {
    return decreasedContainers;
  }

  public List<Container> getPromotedContainers() {
    return promotedContainers;
  }

  public List<Container> getDemotedContainers() {
    return demotedContainers;
  }

  public List<Container> getPreviousAttemptContainers() {
    return previousAttemptContainers;
  }

  public List<RejectedSchedulingRequest> getRejectedRequest() {
    return rejectedRequest;
  }

  @VisibleForTesting
  public void setResourceLimit(Resource resource) {
    this.resourceLimit = resource;
  }

  @Override
  public String toString() {
    return "Allocation{" + "containers=" + containers + ", strictContainers="
        + strictContainers + ", fungibleContainers=" + fungibleContainers
        + ", fungibleResources=" + fungibleResources + ", nmTokens=" + nmTokens
        + ", increasedContainers=" + increasedContainers
        + ", decreasedContainers=" + decreasedContainers
        + ", promotedContainers=" + promotedContainers + ", demotedContainers="
        + demotedContainers + ", previousAttemptContainers="
        + previousAttemptContainers + ", resourceLimit=" + resourceLimit + '}';
  }
}
