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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.QueueStatistics;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.AccessRequest;
import org.apache.hadoop.yarn.security.PrivilegedEntity;
import org.apache.hadoop.yarn.security.PrivilegedEntity.EntityType;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

import org.apache.hadoop.classification.VisibleForTesting;
// FSQueue 是 YARN Fair Scheduler 中的一个抽象类，用于表示一个调度队列。
// 它负责管理队列的资源、资源的公平分配、调度策略以及与父队列和子队列的关系。
// 每个队列可以有自己的资源配置、调度策略以及最大和最小资源限制
@Private
@Unstable
public abstract class FSQueue implements Queue, Schedulable {
  private static final Logger LOG = LoggerFactory.getLogger(
      FSQueue.class.getName());
  //表示队列当前的公平资源份额。fairShare 用来决定该队列的资源分配，以确保每个队列获得公平的资源量
  private Resource fairShare = Resources.createResource(0, 0);
  //表示队列的稳定资源份额。这个属性可能用于控制队列资源分配的平稳性
  private Resource steadyFairShare = Resources.createResource(0, 0);
  //表示队列的预留资源。当某些资源被预留时，这个属性会跟踪预留资源的数量
  private Resource reservedResource = Resources.createResource(0, 0);
  //跟踪队列的资源使用情况，如已分配的资源数量
  private final Resource resourceUsage = Resource.newInstance(0, 0);
  //队列的名称，用于标识队列
  private final String name;
  //FairScheduler 实例，用于执行调度逻辑。它是 FSQueue 所在的调度器
  protected final FairScheduler scheduler;
  //用于授权和验证对队列的访问
  private final YarnAuthorizationProvider authorizer;
  //代表队列的权限实体，用于执行访问控制操作
  private final PrivilegedEntity queueEntity;
  //队列的性能指标，用于收集关于队列的资源使用和运行状况的数据。
  private final FSQueueMetrics metrics;
  //队列的父队列。如果队列是根队列或子队列，parent 表示其父队列
  protected final FSParentQueue parent;
  protected final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);
  //队列的调度策略。调度策略定义了资源如何在队列中分配，可能包括公平调度、权重调度等
  protected SchedulingPolicy policy = SchedulingPolicy.DEFAULT_POLICY;
  //队列的权重，用于决定队列在资源调度中的优先级。权重越大，队列分配的资源越多
  protected float weights;
  //队列的最小资源分配量。队列至少应获得这些资源
  protected Resource minShare;
  //队列的最大资源分配量。队列最多可以获得这些资源
  private ConfigurableResource maxShare;
  //队列中最大可以同时运行的应用程序数
  protected int maxRunningApps;
  //队列的子队列的最大资源限制。这个属性用于管理子队列的资源限制
  private ConfigurableResource maxChildQueueResource;

  // maxAMShare is a value between 0 and 1.
  //队列中应用程序的最大资源分配比例，值在 0 到 1 之间
  protected float maxAMShare;
  //公平资源分配的抢占超时。它控制了当资源不足时，抢占其他队列资源的等待时间
  private long fairSharePreemptionTimeout = Long.MAX_VALUE;
  //最小资源分配的抢占超时，控制当队列的最小资源未满足时，抢占其他队列资源的等待时间
  private long minSharePreemptionTimeout = Long.MAX_VALUE;
  //公平资源分配抢占的阈值。当资源分配低于这个阈值时，可能会启动抢占机制
  private float fairSharePreemptionThreshold = 0.5f;
  //标记队列是否可以被抢占。如果为 true，队列中的任务可以被其他队列抢占
  private boolean preemptable = true;
  //标记队列是否是动态的。如果为 true，队列的资源可以根据需要动态调整
  private boolean isDynamic = true;
  //队列中容器的最大资源限制。它控制了每个容器的资源大小
  protected Resource maxContainerAllocation;

  public FSQueue(String name, FairScheduler scheduler, FSParentQueue parent) {
    this.name = name;
    this.scheduler = scheduler;
    this.authorizer =
        YarnAuthorizationProvider.getInstance(scheduler.getConf());
    this.queueEntity = new PrivilegedEntity(EntityType.QUEUE, name);
    this.metrics = FSQueueMetrics.forQueue(getName(), parent, true, scheduler.getConf());
    this.parent = parent;
    setPolicy(scheduler.getAllocationConfiguration().getSchedulingPolicy(name));
    reinit(false);
  }

  /**
   * Initialize a queue by setting its queue-specific properties and its
   * metrics. This method is invoked when creating a new queue or reloading
   * the allocation file.
   * This method does not set policies for queues when reloading the allocation
   * file since we need to either set all new policies or nothing, which is
   * handled by method {@link #verifyAndSetPolicyFromConf}.
   *
   * @param recursive whether child queues should be reinitialized recursively
   */
  public final void reinit(boolean recursive) {
    AllocationConfiguration allocConf = scheduler.getAllocationConfiguration();
    allocConf.initFSQueue(this);
    updatePreemptionVariables();

    if (recursive) {
      for (FSQueue child : getChildQueues()) {
        child.reinit(recursive);
      }
    }
  }

  public String getName() {
    return name;
  }

  @Override
  public String getQueueName() {
    return name;
  }

  public SchedulingPolicy getPolicy() {
    return policy;
  }

  public FSParentQueue getParent() {
    return parent;
  }

  public void setPolicy(SchedulingPolicy policy) {
    policy.initialize(scheduler.getContext());
    this.policy = policy;
  }

  public void setWeights(float weights) {
    this.weights = weights;
  }

  @Override
  public float getWeight() {
    return weights;
  }

  public void setMinShare(Resource minShare){
    this.minShare = minShare;
  }

  @Override
  public Resource getMinShare() {
    return minShare;
  }

  public void setMaxShare(ConfigurableResource maxShare){
    this.maxShare = maxShare;
  }

  public void setMaxContainerAllocation(Resource maxContainerAllocation){
    this.maxContainerAllocation = maxContainerAllocation;
  }

  public abstract Resource getMaximumContainerAllocation();

  @Override
  public Resource getMaxShare() {
    Resource maxResource = maxShare.getResource(scheduler.getClusterResource());

    // Max resource should be greater than or equal to min resource
    Resource result = Resources.componentwiseMax(maxResource, minShare);

    if (!Resources.equals(maxResource, result)) {
      LOG.warn(String.format("Queue %s has max resources %s less than "
          + "min resources %s", getName(), maxResource, minShare));
    }
    return result;
  }

  public ConfigurableResource getRawMaxShare() {
    return maxShare;
  }

  public Resource getReservedResource() {
    reservedResource.setMemorySize(metrics.getReservedMB());
    reservedResource.setVirtualCores(metrics.getReservedVirtualCores());
    return reservedResource;
  }

  public void setMaxChildQueueResource(ConfigurableResource maxChildShare){
    this.maxChildQueueResource = maxChildShare;
  }

  public ConfigurableResource getMaxChildQueueResource() {
    return maxChildQueueResource;
  }

  public void setMaxRunningApps(int maxRunningApps){
    this.maxRunningApps = maxRunningApps;
  }

  public int getMaxRunningApps() {
    return maxRunningApps;
  }

  @VisibleForTesting
  public float getMaxAMShare() {
    return maxAMShare;
  }

  public void setMaxAMShare(float maxAMShare){
    this.maxAMShare = maxAMShare;
  }

  @Override
  public long getStartTime() {
    return 0;
  }

  @Override
  public Priority getPriority() {
    Priority p = recordFactory.newRecordInstance(Priority.class);
    p.setPriority(1);
    return p;
  }
  
  @Override
  public QueueInfo getQueueInfo(boolean includeChildQueues, boolean recursive) {
    QueueInfo queueInfo = recordFactory.newRecordInstance(QueueInfo.class);
    queueInfo.setSchedulerType("FairScheduler");
    queueInfo.setQueueName(getQueueName());

    if (scheduler.getClusterResource().getMemorySize() == 0) {
      queueInfo.setCapacity(0.0f);
    } else {
      queueInfo.setCapacity((float) getFairShare().getMemorySize() /
          scheduler.getClusterResource().getMemorySize());
    }

    if (getFairShare().getMemorySize() == 0) {
      queueInfo.setCurrentCapacity(0.0f);
    } else {
      queueInfo.setCurrentCapacity((float) getResourceUsage().getMemorySize() /
          getFairShare().getMemorySize());
    }

    // set Weight
    queueInfo.setWeight(getWeight());

    // set MinShareResource
    Resource minShareResource = getMinShare();
    queueInfo.setMinResourceVCore(minShareResource.getVirtualCores());
    queueInfo.setMinResourceMemory(minShareResource.getMemorySize());

    // set MaxShareResource
    Resource maxShareResource =
        Resources.componentwiseMin(getMaxShare(), scheduler.getClusterResource());
    queueInfo.setMaxResourceVCore(maxShareResource.getVirtualCores());
    queueInfo.setMaxResourceMemory(maxShareResource.getMemorySize());

    // set ReservedResource
    Resource newReservedResource = getReservedResource();
    queueInfo.setReservedResourceVCore(newReservedResource.getVirtualCores());
    queueInfo.setReservedResourceMemory(newReservedResource.getMemorySize());

    // set SteadyFairShare
    Resource newSteadyFairShare = getSteadyFairShare();
    queueInfo.setSteadyFairShareVCore(newSteadyFairShare.getVirtualCores());
    queueInfo.setSteadyFairShareMemory(newSteadyFairShare.getMemorySize());

    // set MaxRunningApp
    queueInfo.setMaxRunningApp(getMaxRunningApps());

    // set Preemption
    queueInfo.setPreemptionDisabled(isPreemptable());

    ArrayList<QueueInfo> childQueueInfos = new ArrayList<>();
    if (includeChildQueues) {
      Collection<FSQueue> childQueues = getChildQueues();
      for (FSQueue child : childQueues) {
        childQueueInfos.add(child.getQueueInfo(recursive, recursive));
      }
    }
    queueInfo.setChildQueues(childQueueInfos);
    queueInfo.setQueueState(QueueState.RUNNING);
    queueInfo.setQueueStatistics(getQueueStatistics());
    return queueInfo;
  }

  public QueueStatistics getQueueStatistics() {
    QueueStatistics stats =
        recordFactory.newRecordInstance(QueueStatistics.class);
    stats.setNumAppsSubmitted(getMetrics().getAppsSubmitted());
    stats.setNumAppsRunning(getMetrics().getAppsRunning());
    stats.setNumAppsPending(getMetrics().getAppsPending());
    stats.setNumAppsCompleted(getMetrics().getAppsCompleted());
    stats.setNumAppsKilled(getMetrics().getAppsKilled());
    stats.setNumAppsFailed(getMetrics().getAppsFailed());
    stats.setNumActiveUsers(getMetrics().getActiveUsers());
    stats.setAvailableMemoryMB(getMetrics().getAvailableMB());
    stats.setAllocatedMemoryMB(getMetrics().getAllocatedMB());
    stats.setPendingMemoryMB(getMetrics().getPendingMB());
    stats.setReservedMemoryMB(getMetrics().getReservedMB());
    stats.setAvailableVCores(getMetrics().getAvailableVirtualCores());
    stats.setAllocatedVCores(getMetrics().getAllocatedVirtualCores());
    stats.setPendingVCores(getMetrics().getPendingVirtualCores());
    stats.setReservedVCores(getMetrics().getReservedVirtualCores());
    stats.setAllocatedContainers(getMetrics().getAllocatedContainers());
    stats.setPendingContainers(getMetrics().getPendingContainers());
    stats.setReservedContainers(getMetrics().getReservedContainers());
    return stats;
  }
  
  @Override
  public FSQueueMetrics getMetrics() {
    return metrics;
  }

  /** Get the fair share assigned to this Schedulable. */
  public Resource getFairShare() {
    return fairShare;
  }

  @Override
  public void setFairShare(Resource fairShare) {
    this.fairShare = fairShare;
    metrics.setFairShare(fairShare);
    LOG.debug("The updated fairShare for {} is {}", getName(), fairShare);
  }

  /**
   * Get the steady fair share assigned to this Schedulable.
   * @return the steady fair share assigned to this Schedulable.
   */
  public Resource getSteadyFairShare() {
    return steadyFairShare;
  }

  void setSteadyFairShare(Resource steadyFairShare) {
    this.steadyFairShare = steadyFairShare;
    metrics.setSteadyFairShare(steadyFairShare);
  }

  public boolean hasAccess(QueueACL acl, UserGroupInformation user) {
    return authorizer.checkPermission(
        new AccessRequest(queueEntity, user,
            SchedulerUtils.toAccessType(acl), null, null,
            Server.getRemoteAddress(), null));
  }

  long getFairSharePreemptionTimeout() {
    return fairSharePreemptionTimeout;
  }

  void setFairSharePreemptionTimeout(long fairSharePreemptionTimeout) {
    this.fairSharePreemptionTimeout = fairSharePreemptionTimeout;
  }

  long getMinSharePreemptionTimeout() {
    return minSharePreemptionTimeout;
  }

  void setMinSharePreemptionTimeout(long minSharePreemptionTimeout) {
    this.minSharePreemptionTimeout = minSharePreemptionTimeout;
  }

  float getFairSharePreemptionThreshold() {
    return fairSharePreemptionThreshold;
  }

  void setFairSharePreemptionThreshold(float fairSharePreemptionThreshold) {
    this.fairSharePreemptionThreshold = fairSharePreemptionThreshold;
  }

  @Override
  public boolean isPreemptable() {
    return preemptable;
  }

  /**
   * Recomputes the shares for all child queues and applications based on this
   * queue's current share.
   *
   * To be called holding the scheduler writelock.
   */
  abstract void updateInternal();

  /**
   * Set the queue's fairshare and update the demand/fairshare of child
   * queues/applications.
   *
   * To be called holding the scheduler writelock.
   *
   * @param fairShare queue's fairshare.
   */
  public void update(Resource fairShare) {
    setFairShare(fairShare);
    updateInternal();
  }

  /**
   * Update the min/fair share preemption timeouts, threshold and preemption
   * disabled flag for this queue.
   */
  private void updatePreemptionVariables() {
    // For min share timeout
    minSharePreemptionTimeout = scheduler.getAllocationConfiguration()
        .getMinSharePreemptionTimeout(getName());
    if (minSharePreemptionTimeout == -1 && parent != null) {
      minSharePreemptionTimeout = parent.getMinSharePreemptionTimeout();
    }
    // For fair share timeout
    fairSharePreemptionTimeout = scheduler.getAllocationConfiguration()
        .getFairSharePreemptionTimeout(getName());
    if (fairSharePreemptionTimeout == -1 && parent != null) {
      fairSharePreemptionTimeout = parent.getFairSharePreemptionTimeout();
    }
    // For fair share preemption threshold
    fairSharePreemptionThreshold = scheduler.getAllocationConfiguration()
        .getFairSharePreemptionThreshold(getName());
    if (fairSharePreemptionThreshold < 0 && parent != null) {
      fairSharePreemptionThreshold = parent.getFairSharePreemptionThreshold();
    }
    // For option whether allow preemption from this queue.
    // If the parent is non-preemptable, this queue is non-preemptable as well,
    // otherwise get the value from the allocation file.
    if (parent != null && !parent.isPreemptable()) {
      preemptable = false;
    } else {
      preemptable = scheduler.getAllocationConfiguration()
          .isPreemptable(getName());
    }
  }

  /**
   * Gets the children of this queue, if any.
   *
   * @return the children of this queue.
   */
  public abstract List<FSQueue> getChildQueues();
  
  /**
   * Adds all applications in the queue and its subqueues to the given collection.
   * @param apps the collection to add the applications to
   */
  public abstract void collectSchedulerApplications(
      Collection<ApplicationAttemptId> apps);
  
  /**
   * Return the number of apps for which containers can be allocated.
   * Includes apps in subqueues.
   *
   * @return the number of apps.
   */
  public abstract int getNumRunnableApps();
  
  /**
   * Helper method to check if the queue should attempt assigning resources
   * 
   * @return true if check passes (can assign) or false otherwise
   */
  boolean assignContainerPreCheck(FSSchedulerNode node) {
    if (node.getReservedContainer() != null) {
      LOG.debug("Assigning container failed on node '{}' because it has"
          + " reserved containers.", node.getNodeName());
      return false;
    } else if (!Resources.fitsIn(getResourceUsage(), getMaxShare())) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Assigning container failed on node '" + node.getNodeName()
            + " because queue resource usage is larger than MaxShare: "
            + dumpState());
      }
      return false;
    } else {
      return true;
    }
  }

  /**
   * Returns true if queue has at least one app running.
   *
   * @return true, if queue has at least one app running; otherwise, false;
   */
  public boolean isActive() {
    return getNumRunnableApps() > 0;
  }

  /** Convenient toString implementation for debugging. */
  @Override
  public String toString() {
    return String.format("[%s, demand=%s, running=%s, share=%s, w=%s]",
        getName(), getDemand(), getResourceUsage(), fairShare, getWeight());
  }
  
  @Override
  public Set<String> getAccessibleNodeLabels() {
    // TODO, add implementation for FS
    return null;
  }
  
  @Override
  public String getDefaultNodeLabelExpression() {
    // TODO, add implementation for FS
    return null;
  }
  
  @Override
  public void incPendingResource(String nodeLabel, Resource resourceToInc) {
  }
  
  @Override
  public void decPendingResource(String nodeLabel, Resource resourceToDec) {
  }

  @Override
  public void incReservedResource(String nodeLabel, Resource resourceToInc) {
  }

  @Override
  public void decReservedResource(String nodeLabel, Resource resourceToDec) {
  }

  @Override
  public Resource getResourceUsage() {
    return resourceUsage;
  }

  /**
   * Increase resource usage for this queue and all parent queues.
   *
   * @param res the resource to increase
   */
  protected void incUsedResource(Resource res) {
    synchronized (resourceUsage) {
      Resources.addTo(resourceUsage, res);
      if (parent != null) {
        parent.incUsedResource(res);
      }
    }
  }

  /**
   * Decrease resource usage for this queue and all parent queues.
   *
   * @param res the resource to decrease
   */
  protected void decUsedResource(Resource res) {
    synchronized (resourceUsage) {
      Resources.subtractFrom(resourceUsage, res);
      if (parent != null) {
        parent.decUsedResource(res);
      }
    }
  }

  @Override
  public Priority getDefaultApplicationPriority() {
    // TODO add implementation for FSParentQueue
    return null;
  }

  boolean fitsInMaxShare(Resource additionalResource) {
    Resource usagePlusAddition =
        Resources.add(getResourceUsage(), additionalResource);

    if (!Resources.fitsIn(usagePlusAddition, getMaxShare())) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Resource usage plus resource request: " + usagePlusAddition
            + " exceeds maximum resource allowed:" + getMaxShare()
            + " in queue " + getName());
      }
      return false;
    }

    FSQueue parentQueue = getParent();
    if (parentQueue != null) {
      return parentQueue.fitsInMaxShare(additionalResource);
    }
    return true;
  }

  /**
   * Recursively check policies for queues in pre-order. Get queue policies
   * from the allocation file instead of properties of {@link FSQueue} objects.
   * Set the policy for current queue if there is no policy violation for its
   * children. This method is invoked while reloading the allocation file.
   *
   * @param queueConf allocation configuration
   * @return true if no policy violation and successfully set polices
   *         for queues; false otherwise
   */
  public boolean verifyAndSetPolicyFromConf(AllocationConfiguration queueConf) {
    SchedulingPolicy queuePolicy = queueConf.getSchedulingPolicy(getName());

    for (FSQueue child : getChildQueues()) {
      if (!queuePolicy.isChildPolicyAllowed(
          queueConf.getSchedulingPolicy(child.getName()))) {
        return false;
      }
      boolean success = child.verifyAndSetPolicyFromConf(queueConf);
      if (!success) {
        return false;
      }
    }

    // Set the policy if no policy violation for all children
    setPolicy(queuePolicy);
    return true;
  }

  /**
   * Recursively dump states of all queues.
   *
   * @return a string which holds all queue states
   */
  public String dumpState() {
    StringBuilder sb = new StringBuilder();
    dumpStateInternal(sb);
    return sb.toString();
  }


  /**
   * Recursively dump states of all queues.
   *
   * @param sb the {code StringBuilder} which holds queue states
   */
  protected abstract void dumpStateInternal(StringBuilder sb);

  public boolean isDynamic() {
    return isDynamic;
  }

  public void setDynamic(boolean dynamic) {
    this.isDynamic = dynamic;
  }

  public abstract boolean isEmpty();
}
