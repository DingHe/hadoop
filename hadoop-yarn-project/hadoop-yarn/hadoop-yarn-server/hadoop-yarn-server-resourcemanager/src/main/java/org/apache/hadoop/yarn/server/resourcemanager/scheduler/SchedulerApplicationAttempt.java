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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerUpdateType;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.api.records.UpdateContainerError;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.api.ContainerType;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMServerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AggregateAppResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerReservedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerUpdatesAcquiredEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeCleanContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeUpdateContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ContainerRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.PendingAsk;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.AppPlacementAllocator;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.SchedulableEntity;
import org.apache.hadoop.yarn.server.scheduler.OpportunisticContainerContext;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.state.InvalidStateTransitionException;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.ConcurrentHashMultiset;

/**
 * Represents an application attempt from the viewpoint of the scheduler.
 * Each running app attempt in the RM corresponds to one instance
 * of this class.
 */
//表示 YARN 调度器视角中的一个应用程序尝试。
// 每个正在运行的应用程序尝试对应于资源管理器（RM）中的一个 SchedulerApplicationAttempt 实例。
// 该类主要负责管理应用程序在调度器中的资源使用、容器的分配、状态管理以及与调度器的交互。
// 它处理了与应用程序尝试相关的各种调度决策，包括资源请求、容器分配、优先级处理等
@Private
@Unstable
public class SchedulerApplicationAttempt implements SchedulableEntity {
  
  private static final Logger LOG = LoggerFactory
      .getLogger(SchedulerApplicationAttempt.class);

  private FastDateFormat fdf =
      FastDateFormat.getInstance("EEE MMM dd HH:mm:ss Z yyyy");

  private static final long MEM_AGGREGATE_ALLOCATION_CACHE_MSECS = 3000;
  protected long lastMemoryAggregateAllocationUpdateTime = 0;
  private Map<String, Long> lastResourceSecondsMap = new HashMap<>();
  //存储与该应用程序尝试调度相关的信息，如资源使用情况、调度环境等
  protected final AppSchedulingInfo appSchedulingInfo;
  //表示应用程序尝试的唯一标识符
  protected ApplicationAttemptId attemptId;
  //存储当前应用程序尝试的所有活动容器
  protected Map<ContainerId, RMContainer> liveContainers =
      new ConcurrentHashMap<>();
  //存储被保留的容器信息。SchedulerRequestKey 用于区分请求的不同容器，NodeId 用于标识分配容器的节点
  protected final Map<SchedulerRequestKey, Map<NodeId, RMContainer>>
      reservedContainers = new HashMap<>();
  //表示重新预定的容器请求的计数
  private final ConcurrentHashMultiset<SchedulerRequestKey> reReservations =
      ConcurrentHashMultiset.create();
  //表示此应用程序尝试的资源限制 (Resource)，例如内存和 vCore 的最大使用量
  private volatile Resource resourceLimit = Resource.newInstance(0, 0);
  //表示应用程序是否使用非托管的 AM（Application Master）。如果是，表示 AM 由外部系统管理
  private boolean unmanagedAM = true;
  //表示 AM 是否正在运行
  private boolean amRunning = false;
  //表示应用程序的日志聚合上下文，用于控制日志存储和聚合行为
  private LogAggregationContext logAggregationContext;
  //表示应用程序的优先级
  private volatile Priority appPriority = null;
  //表示应用程序是否处于恢复状态
  private boolean isAttemptRecovering;
  //表示此应用程序尝试的资源使用情况（如内存、CPU 等）
  protected ResourceUsage attemptResourceUsage = new ResourceUsage();
  /** Resource usage of opportunistic containers. */
  //表示此应用程序尝试的机会容器的资源使用情况
  protected ResourceUsage attemptOpportunisticResourceUsage =
      new ResourceUsage();
  /** Scheduled by a remote scheduler. */
  //表示通过远程调度器分配的资源使用情况
  protected ResourceUsage attemptResourceUsageAllocatedRemotely =
      new ResourceUsage();
  //表示首次请求资源的时间戳
  private AtomicLong firstAllocationRequestSentTime = new AtomicLong(0);
  //表示首次分配容器的时间戳
  private AtomicLong firstContainerAllocatedTime = new AtomicLong(0);
  //表示新分配的容器列表
  protected List<RMContainer> newlyAllocatedContainers = new ArrayList<>();
  //表示临时需要杀死的容器列表
  protected List<RMContainer> tempContainerToKill = new ArrayList<>();
  //表示刚刚晋升为活跃容器的容器映射
  protected Map<ContainerId, RMContainer> newlyPromotedContainers = new HashMap<>();
  //表示刚刚降级的容器映射
  protected Map<ContainerId, RMContainer> newlyDemotedContainers = new HashMap<>();
  //表示资源减少的容器映射
  protected Map<ContainerId, RMContainer> newlyDecreasedContainers = new HashMap<>();
  //表示资源增加的容器映射
  protected Map<ContainerId, RMContainer> newlyIncreasedContainers = new HashMap<>();
  //表示更新过的 NodeManager token 集合
  protected Set<NMToken> updatedNMTokens = new HashSet<>();
 //表示容器更新错误列表
  protected List<UpdateContainerError> updateContainerErrors = new ArrayList<>();

  //Keeps track of recovered containers from previous attempt which haven't
  //been reported to the AM.
  //表示从上次尝试中恢复的容器，这些容器尚未报告给 AM
  private List<Container> recoveredPreviousAttemptContainers =
      new ArrayList<>();

  // This pendingRelease is used in work-preserving recovery scenario to keep
  // track of the AM's outstanding release requests. RM on recovery could
  // receive the release request form AM before it receives the container status
  // from NM for recovery. In this case, the to-be-recovered containers reported
  // by NM should not be recovered.
  //用于工作保护恢复场景，跟踪 AM 的未完成释放请求
  private Set<ContainerId> pendingRelease = null;
  //表示机会容器上下文，用于管理机会容器
  private OpportunisticContainerContext oppContainerContext;

  /**
   * Count how many times the application has been given an opportunity to
   * schedule a task at each priority. Each time the scheduler asks the
   * application for a task at this priority, it is incremented, and each time
   * the application successfully schedules a task (at rack or node local), it
   * is reset to 0.
   */
  //记录应用程序在每个优先级下调度任务的机会次数
  private ConcurrentHashMultiset<SchedulerRequestKey> schedulingOpportunities =
      ConcurrentHashMultiset.create();

  /**
   * Count how many times the application has been given an opportunity to
   * schedule a non-partitioned resource request at each priority. Each time the
   * scheduler asks the application for a task at this priority, it is
   * incremented, and each time the application successfully schedules a task,
   * it is reset to 0 when schedule any task at corresponding priority.
   */
  private ConcurrentHashMultiset<SchedulerRequestKey>
      missedNonPartitionedReqSchedulingOpportunity =
      ConcurrentHashMultiset.create();
  
  // Time of the last container scheduled at the current allowed level
  //记录应用程序在每个优先级下错过的非分区资源请求调度机会次数
  protected Map<SchedulerRequestKey, Long> lastScheduledContainer =
      new ConcurrentHashMap<>();
  //表示该应用程序尝试所属于的队列
  protected volatile Queue queue;
  //表示该应用程序是否已停止
  protected volatile boolean isStopped = false;
  //表示与此应用程序 AM 关联的节点分区名
  protected String appAMNodePartitionName = CommonNodeLabelsManager.NO_LABEL;

  protected final RMContext rmContext;
  //表示与此应用程序尝试相关的应用程序尝试对象
  private RMAppAttempt appAttempt;

  protected ReentrantReadWriteLock.ReadLock readLock;
  protected ReentrantReadWriteLock.WriteLock writeLock;
  //表示应用程序的调度环境变量
  private Map<String, String> applicationSchedulingEnvs = new HashMap<>();

  // Not confirmed allocation resource, will be used to avoid too many proposal
  // rejected because of duplicated allocation
  //表示尚未确认分配的内存和 vCore 数量
  private AtomicLong unconfirmedAllocatedMem = new AtomicLong();
  private AtomicInteger unconfirmedAllocatedVcores = new AtomicInteger();
  //表示与此应用程序相关的节点标签表达式
  private String nodeLabelExpression;
  //表示应用程序尝试的开始时间戳
  private final long startTime;

  public SchedulerApplicationAttempt(ApplicationAttemptId applicationAttemptId, 
      String user, Queue queue, AbstractUsersManager abstractUsersManager,
      RMContext rmContext) {
    Preconditions.checkNotNull(rmContext, "RMContext should not be null");
    this.rmContext = rmContext;
    this.queue = queue;
    this.pendingRelease = Collections.newSetFromMap(
        new ConcurrentHashMap<ContainerId, Boolean>());
    this.attemptId = applicationAttemptId;
    if (rmContext.getRMApps() != null &&
        rmContext.getRMApps()
            .containsKey(applicationAttemptId.getApplicationId())) {
      RMApp rmApp = rmContext.getRMApps().get(applicationAttemptId.getApplicationId());
      ApplicationSubmissionContext appSubmissionContext =
          rmApp
              .getApplicationSubmissionContext();
      appAttempt = rmApp.getCurrentAppAttempt();
      if (appSubmissionContext != null) {
        unmanagedAM = appSubmissionContext.getUnmanagedAM();
        this.logAggregationContext =
            appSubmissionContext.getLogAggregationContext();
        this.nodeLabelExpression =
            appSubmissionContext.getNodeLabelExpression();
      }
      applicationSchedulingEnvs = rmApp.getApplicationSchedulingEnvs();
    }

    this.appSchedulingInfo = new AppSchedulingInfo(applicationAttemptId, user,
        queue, abstractUsersManager, rmContext.getEpoch(), attemptResourceUsage,
        applicationSchedulingEnvs, rmContext, unmanagedAM);
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();
    startTime = SystemClock.getInstance().getTime();
  }

  public void setOpportunisticContainerContext(
      OpportunisticContainerContext oppContext) {
    this.oppContainerContext = oppContext;
  }

  public OpportunisticContainerContext
      getOpportunisticContainerContext() {
    return this.oppContainerContext;
  }

  /**
   * Get the live containers of the application.
   * @return live containers of the application
   */
  public Collection<RMContainer> getLiveContainers() {
    readLock.lock();
    try {
      return new ArrayList<>(liveContainers.values());
    } finally {
      readLock.unlock();
    }
  }

  public AppSchedulingInfo getAppSchedulingInfo() {
    return this.appSchedulingInfo;
  }

  public ContainerUpdateContext getUpdateContext() {
    return this.appSchedulingInfo.getUpdateContext();
  }

  /**
   * Is this application pending?
   * @return true if it is else false.
   */
  public boolean isPending() {
    return appSchedulingInfo.isPending();
  }
  
  /**
   * Get {@link ApplicationAttemptId} of the application master.
   * @return <code>ApplicationAttemptId</code> of the application master
   */
  public ApplicationAttemptId getApplicationAttemptId() {
    return appSchedulingInfo.getApplicationAttemptId();
  }
  
  public ApplicationId getApplicationId() {
    return appSchedulingInfo.getApplicationId();
  }
  
  public String getUser() {
    return appSchedulingInfo.getUser();
  }

  public Set<ContainerId> getPendingRelease() {
    return this.pendingRelease;
  }

  public long getNewContainerId() {
    return appSchedulingInfo.getNewContainerId();
  }

  public Collection<SchedulerRequestKey> getSchedulerKeys() {
    return appSchedulingInfo.getSchedulerKeys();
  }

  public PendingAsk getPendingAsk(
      SchedulerRequestKey schedulerKey, String resourceName) {
    readLock.lock();
    try {
      return appSchedulingInfo.getPendingAsk(schedulerKey, resourceName);
    } finally {
      readLock.unlock();
    }
  }

  public int getOutstandingAsksCount(SchedulerRequestKey schedulerKey) {
    return getOutstandingAsksCount(schedulerKey, ResourceRequest.ANY);
  }

  public int getOutstandingAsksCount(SchedulerRequestKey schedulerKey,
      String resourceName) {
    readLock.lock();
    try {
      AppPlacementAllocator ap = appSchedulingInfo.getAppPlacementAllocator(
          schedulerKey);
      return ap == null ? 0 : ap.getOutstandingAsksCount(resourceName);
    } finally {
      readLock.unlock();
    }
  }

  public String getQueueName() {
    return appSchedulingInfo.getQueueName();
  }
  
  public Resource getAMResource() {
    return attemptResourceUsage.getAMUsed();
  }

  public Resource getAMResource(String label) {
    return attemptResourceUsage.getAMUsed(label);
  }

  public void setAMResource(Resource amResource) {
    attemptResourceUsage.setAMUsed(amResource);
  }

  public void setAMResource(String label, Resource amResource) {
    attemptResourceUsage.setAMUsed(label, amResource);
  }

  public boolean isAmRunning() {
    return amRunning;
  }

  public void setAmRunning(boolean bool) {
    amRunning = bool;
  }

  public boolean getUnmanagedAM() {
    return unmanagedAM;
  }

  public RMContainer getRMContainer(ContainerId id) {
    return liveContainers.get(id);
  }

  public void addRMContainer(
      ContainerId id, RMContainer rmContainer) {
    writeLock.lock();
    try {
      if (!getApplicationAttemptId().equals(
          rmContainer.getApplicationAttemptId()) &&
          !liveContainers.containsKey(id)) {
        LOG.info("recovered container " + id +
            " from previous attempt " + rmContainer.getApplicationAttemptId());
        recoveredPreviousAttemptContainers.add(rmContainer.getContainer());
      }
      liveContainers.put(id, rmContainer);
      if (rmContainer.getExecutionType() == ExecutionType.OPPORTUNISTIC) {
        this.attemptOpportunisticResourceUsage.incUsed(
            rmContainer.getAllocatedResource());
      }
      if (rmContainer.isRemotelyAllocated()) {
        this.attemptResourceUsageAllocatedRemotely.incUsed(
            rmContainer.getAllocatedResource());
      }
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Removes an RM container from the map of live containers
   * related to this application attempt.
   * @param containerId The container ID of the RMContainer to remove
   * @return true if the container is in the map
   */
  public boolean removeRMContainer(ContainerId containerId) {
    writeLock.lock();
    try {
      RMContainer rmContainer = liveContainers.remove(containerId);
      if (rmContainer != null) {
        if (rmContainer.getExecutionType() == ExecutionType.OPPORTUNISTIC) {
          this.attemptOpportunisticResourceUsage
              .decUsed(rmContainer.getAllocatedResource());
        }
        if (rmContainer.isRemotelyAllocated()) {
          this.attemptResourceUsageAllocatedRemotely
              .decUsed(rmContainer.getAllocatedResource());
        }

        return true;
      }

      return false;
    } finally {
      writeLock.unlock();
    }
  }

  protected void resetReReservations(
      SchedulerRequestKey schedulerKey) {
    reReservations.setCount(schedulerKey, 0);
  }

  protected void addReReservation(
      SchedulerRequestKey schedulerKey) {
    try {
      reReservations.add(schedulerKey);
    } catch (IllegalArgumentException e) {
      // This happens when count = MAX_INT, ignore the exception
    }
  }

  public int getReReservations(SchedulerRequestKey schedulerKey) {
    return reReservations.count(schedulerKey);
  }

  /**
   * Get total current reservations.
   * Used only by unit tests
   * @return total current reservations
   */
  @Stable
  @Private
  public Resource getCurrentReservation() {
    return attemptResourceUsage.getReserved();
  }
  
  public Queue getQueue() {
    return queue;
  }
  
  public boolean updateResourceRequests(
      List<ResourceRequest> requests) {
    writeLock.lock();
    try {
      if (!isStopped) {
        return appSchedulingInfo.updateResourceRequests(requests, false);
      }
      return false;
    } finally {
      writeLock.unlock();
    }
  }

  public boolean updateSchedulingRequests(
      List<SchedulingRequest> requests) {
    if (requests == null) {
      return false;
    }

    writeLock.lock();
    try {
      if (!isStopped) {
        return appSchedulingInfo.updateSchedulingRequests(requests, false);
      }
      return false;
    } finally {
      writeLock.unlock();
    }
  }
  
  public void recoverResourceRequestsForContainer(
      ContainerRequest containerRequest) {
    writeLock.lock();
    try {
      if (!isStopped) {
        appSchedulingInfo.updateResourceRequests(
            containerRequest.getResourceRequests(), true);
      }
    } finally {
      writeLock.unlock();
    }
  }
  
  public void stop(RMAppAttemptState rmAppAttemptFinalState) {
    writeLock.lock();
    try {
      // Cleanup all scheduling information
      isStopped = true;
      appSchedulingInfo.stop();
    } finally {
      writeLock.unlock();
    }
  }

  public boolean isStopped() {
    return isStopped;
  }

  /**
   * Get the list of reserved containers
   * @return All of the reserved containers.
   */
  public List<RMContainer> getReservedContainers() {
    List<RMContainer> list = new ArrayList<>();
    readLock.lock();
    try {
      for (Entry<SchedulerRequestKey, Map<NodeId, RMContainer>> e :
          this.reservedContainers.entrySet()) {
        list.addAll(e.getValue().values());
      }
      return list;
    } finally {
      readLock.unlock();
    }

  }
  
  public boolean reserveIncreasedContainer(SchedulerNode node,
      SchedulerRequestKey schedulerKey, RMContainer rmContainer,
      Resource reservedResource) {
    writeLock.lock();
    try {
      if (commonReserve(node, schedulerKey, rmContainer, reservedResource)) {
        attemptResourceUsage.incReserved(node.getPartition(), reservedResource);
        // succeeded
        return true;
      }

      return false;
    } finally {
      writeLock.unlock();
    }

  }
  
  private boolean commonReserve(SchedulerNode node,
      SchedulerRequestKey schedulerKey, RMContainer rmContainer,
      Resource reservedResource) {
    try {
      rmContainer.handle(new RMContainerReservedEvent(rmContainer
          .getContainerId(), reservedResource, node.getNodeID(), schedulerKey));
    } catch (InvalidStateTransitionException e) {
      // We reach here could be caused by container already finished, return
      // false indicate it fails
      return false;
    }
    
    Map<NodeId, RMContainer> reservedContainers = 
        this.reservedContainers.get(schedulerKey);
    if (reservedContainers == null) {
      reservedContainers = new HashMap<NodeId, RMContainer>();
      this.reservedContainers.put(schedulerKey, reservedContainers);
    }
    reservedContainers.put(node.getNodeID(), rmContainer);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Application attempt " + getApplicationAttemptId()
          + " reserved container " + rmContainer + " on node " + node
          + ". This attempt currently has " + reservedContainers.size()
          + " reserved containers at priority " + schedulerKey.getPriority()
          + "; currentReservation " + reservedResource);
    }
    
    return true;
  }
  
  public RMContainer reserve(SchedulerNode node,
      SchedulerRequestKey schedulerKey, RMContainer rmContainer,
      Container container) {
    writeLock.lock();
    try {
      // Create RMContainer if necessary
      if (rmContainer == null) {
        rmContainer = new RMContainerImpl(container, schedulerKey,
            getApplicationAttemptId(), node.getNodeID(),
            appSchedulingInfo.getUser(), rmContext);
      }
      if (rmContainer.getState() == RMContainerState.NEW) {
        attemptResourceUsage.incReserved(node.getPartition(),
            container.getResource());

        ResourceScheduler scheduler = this.rmContext.getScheduler();
        String qn = this.getQueueName();
        if (scheduler instanceof CapacityScheduler) {
          qn = ((CapacityScheduler)scheduler).normalizeQueueName(qn);
        }
        ((RMContainerImpl) rmContainer).setQueueName(qn);

        // Reset the re-reservation count
        resetReReservations(schedulerKey);
      } else{
        // Note down the re-reservation
        addReReservation(schedulerKey);
      }

      commonReserve(node, schedulerKey, rmContainer, container.getResource());

      return rmContainer;
    } finally {
      writeLock.unlock();
    }

  }

  public void setHeadroom(Resource globalLimit) {
    this.resourceLimit = Resources.componentwiseMax(globalLimit,
        Resources.none());
  }

  /**
   * Get available headroom in terms of resources for the application's user.
   * @return available resource headroom
   */
  public Resource getHeadroom() {
    return resourceLimit;
  }
  
  public int getNumReservedContainers(
      SchedulerRequestKey schedulerKey) {
    readLock.lock();
    try {
      Map<NodeId, RMContainer> map = this.reservedContainers.get(
          schedulerKey);
      return (map == null) ? 0 : map.size();
    } finally {
      readLock.unlock();
    }
  }
  
  @SuppressWarnings("unchecked")
  public void containerLaunchedOnNode(ContainerId containerId,
      NodeId nodeId) {
    readLock.lock();
    try {
      // Inform the container
      RMContainer rmContainer = getRMContainer(containerId);
      if (rmContainer == null) {
        // Some unknown container sneaked into the system. Kill it.
        rmContext.getDispatcher().getEventHandler().handle(
            new RMNodeCleanContainerEvent(nodeId, containerId));
        return;
      }

      rmContainer.handle(
          new RMContainerEvent(containerId, RMContainerEventType.LAUNCHED));
    } finally {
      readLock.unlock();
    }
  }
  
  public void showRequests() {
    if (LOG.isDebugEnabled()) {
      readLock.lock();
      try {
        for (SchedulerRequestKey schedulerKey : getSchedulerKeys()) {
          AppPlacementAllocator ap = getAppPlacementAllocator(schedulerKey);
          if (ap != null &&
              ap.getOutstandingAsksCount(ResourceRequest.ANY) > 0) {
            LOG.debug("showRequests:" + " application=" + getApplicationId()
                + " headRoom=" + getHeadroom() + " currentConsumption="
                + attemptResourceUsage.getUsed().getMemorySize());
            ap.showRequests();
          }
        }
      } finally {
        readLock.unlock();
      }
    }
  }
  
  public Resource getCurrentConsumption() {
    return attemptResourceUsage.getUsed();
  }
  
  private Container updateContainerAndNMToken(RMContainer rmContainer,
      ContainerUpdateType updateType) {
    Container container = rmContainer.getContainer();
    ContainerType containerType = ContainerType.TASK;
    if (updateType != null) {
      container.setVersion(container.getVersion() + 1);
    }
    // The working knowledge is that masterContainer for AM is null as it
    // itself is the master container.
    if (isWaitingForAMContainer()) {
      containerType = ContainerType.APPLICATION_MASTER;
    }
    try {
      // create container token and NMToken altogether.
      container.setContainerToken(rmContext.getContainerTokenSecretManager()
          .createContainerToken(container.getId(), container.getVersion(),
              container.getNodeId(), getUser(), container.getResource(),
              container.getPriority(), rmContainer.getCreationTime(),
              this.logAggregationContext, rmContainer.getNodeLabelExpression(),
              containerType, container.getExecutionType(),
              container.getAllocationRequestId(),
              rmContainer.getAllocationTags()));
      container.setAllocationTags(rmContainer.getAllocationTags());
      updateNMToken(container);
    } catch (IllegalArgumentException e) {
      // DNS might be down, skip returning this container.
      LOG.error("Error trying to assign container token and NM token to"
          + " an updated container " + container.getId(), e);
      return null;
    }

    if (updateType == null) {
      // This is a newly allocated container
      rmContainer.handle(new RMContainerEvent(
          rmContainer.getContainerId(), RMContainerEventType.ACQUIRED));
    } else {
      // Resource increase is handled as follows:
      // If the AM does not use the updated token to increase the container
      // for a configured period of time, the RM will automatically rollback
      // the update by performing a container decrease. This rollback (which
      // essentially is another resource decrease update) is notified to the
      // NM heartbeat response. If autoUpdate flag is set, then AM does not
      // need to do anything - same code path as resource decrease.
      //
      // Resource Decrease is always automatic: the AM never has to do
      // anything. It is always via NM heartbeat response.
      //
      // ExecutionType updates (both Promotion and Demotion) are either
      // always automatic (if the flag is set) or the AM has to explicitly
      // call updateContainer() on the NM. There is no expiry
      boolean autoUpdate =
          ContainerUpdateType.DECREASE_RESOURCE == updateType ||
              ((AbstractYarnScheduler)rmContext.getScheduler())
                  .shouldContainersBeAutoUpdated();
      if (autoUpdate) {
        this.rmContext.getDispatcher().getEventHandler().handle(
            new RMNodeUpdateContainerEvent(rmContainer.getNodeId(),
                Collections.singletonMap(
                    rmContainer.getContainer(), updateType)));
      } else {
        rmContainer.handle(new RMContainerUpdatesAcquiredEvent(
            rmContainer.getContainerId(),
            ContainerUpdateType.INCREASE_RESOURCE == updateType));
      }
    }
    return container;
  }

  public void updateNMTokens(Collection<Container> containers) {
    for (Container container : containers) {
      updateNMToken(container);
    }
  }

  private void updateNMToken(Container container) {
    NMToken nmToken =
        rmContext.getNMTokenSecretManager().createAndGetNMToken(getUser(),
            getApplicationAttemptId(), container);
    if (nmToken != null) {
      updatedNMTokens.add(nmToken);
    }
  }

  /**
   * Called when AM registers. These containers are reported to the AM in the
   * <code>
   * RegisterApplicationMasterResponse#containersFromPreviousAttempts
   * </code>.
   */
  List<RMContainer> pullContainersToTransfer() {
    writeLock.lock();
    try {
      recoveredPreviousAttemptContainers.clear();
      return new ArrayList<>(liveContainers.values());
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Called when AM heartbeats. These containers were recovered by the RM after
   * the AM had registered. They are reported to the AM in the
   * <code>AllocateResponse#containersFromPreviousAttempts</code>.
   * @return Container List.
   */
  public List<Container> pullPreviousAttemptContainers() {
    writeLock.lock();
    try {
      if (recoveredPreviousAttemptContainers.isEmpty()) {
        return null;
      }
      List<Container> returnContainerList = new ArrayList<>
          (recoveredPreviousAttemptContainers);
      recoveredPreviousAttemptContainers.clear();
      updateNMTokens(returnContainerList);
      return returnContainerList;
    } finally {
      writeLock.unlock();
    }
  }

  // Create container token and update NMToken altogether, if either of them fails for
  // some reason like DNS unavailable, do not return this container and keep it
  // in the newlyAllocatedContainers waiting to be refetched.
  public List<Container> pullNewlyAllocatedContainers() {
    writeLock.lock();
    try {
      List<Container> returnContainerList = new ArrayList<Container>(
          newlyAllocatedContainers.size());

      Iterator<RMContainer> i = newlyAllocatedContainers.iterator();
      while (i.hasNext()) {
        RMContainer rmContainer = i.next();
        Container updatedContainer =
            updateContainerAndNMToken(rmContainer, null);
        // Only add container to return list when it's not null.
        // updatedContainer could be null when generate token failed, it can be
        // caused by DNS resolving failed.
        if (updatedContainer != null) {
          returnContainerList.add(updatedContainer);
          i.remove();
        }
      }
      return returnContainerList;
    } finally {
      writeLock.unlock();
    }
  }

  public synchronized void addToNewlyDemotedContainers(ContainerId containerId,
      RMContainer rmContainer) {
    newlyDemotedContainers.put(containerId, rmContainer);
  }

  public synchronized void addToNewlyDecreasedContainers(
      ContainerId containerId, RMContainer rmContainer) {
    newlyDecreasedContainers.put(containerId, rmContainer);
  }

  protected synchronized void addToUpdateContainerErrors(
      UpdateContainerError error) {
    updateContainerErrors.add(error);
  }

  protected synchronized void addToNewlyAllocatedContainers(
      SchedulerNode node, RMContainer rmContainer) {
    ContainerId matchedContainerId =
        getUpdateContext().matchContainerToOutstandingIncreaseReq(
            node, rmContainer.getAllocatedSchedulerKey(), rmContainer);
    if (matchedContainerId != null) {
      if (ContainerUpdateContext.UNDEFINED == matchedContainerId) {
        // This is a spurious allocation (relaxLocality = false
        // resulted in the Container being allocated on an NM on the same host
        // but not on the NM running the container to be updated. Can
        // happen if more than one NM exists on the same host.. usually
        // occurs when using MiniYARNCluster to test).
        tempContainerToKill.add(rmContainer);
      } else {
        RMContainer existingContainer = getRMContainer(matchedContainerId);
        // If this container was already GUARANTEED, then it is an
        // increase, else its a promotion
        if (existingContainer == null ||
            EnumSet.of(RMContainerState.COMPLETED, RMContainerState.KILLED,
                RMContainerState.EXPIRED, RMContainerState.RELEASED).contains(
                    existingContainer.getState())) {
          tempContainerToKill.add(rmContainer);
        } else {
          if (ExecutionType.GUARANTEED == existingContainer.getExecutionType()) {
            newlyIncreasedContainers.put(matchedContainerId, rmContainer);
          } else {
            newlyPromotedContainers.put(matchedContainerId, rmContainer);
          }
        }
      }
    } else {
      newlyAllocatedContainers.add(rmContainer);
    }
  }

  public List<Container> pullNewlyPromotedContainers() {
    return pullNewlyUpdatedContainers(newlyPromotedContainers,
        ContainerUpdateType.PROMOTE_EXECUTION_TYPE);
  }

  public List<Container> pullNewlyDemotedContainers() {
    return pullNewlyUpdatedContainers(newlyDemotedContainers,
        ContainerUpdateType.DEMOTE_EXECUTION_TYPE);
  }

  public List<Container> pullNewlyIncreasedContainers() {
    return pullNewlyUpdatedContainers(newlyIncreasedContainers,
        ContainerUpdateType.INCREASE_RESOURCE);
  }

  public List<Container> pullNewlyDecreasedContainers() {
    return pullNewlyUpdatedContainers(newlyDecreasedContainers,
        ContainerUpdateType.DECREASE_RESOURCE);
  }

  public List<UpdateContainerError> pullUpdateContainerErrors() {
    List<UpdateContainerError> errors =
        new ArrayList<>(updateContainerErrors);
    updateContainerErrors.clear();
    return errors;
  }

  /**
   * A container is promoted if its executionType is changed from
   * OPPORTUNISTIC to GUARANTEED. It id demoted if the change is from
   * GUARANTEED to OPPORTUNISTIC.
   * @return Newly Promoted and Demoted containers
   */
  private List<Container> pullNewlyUpdatedContainers(
      Map<ContainerId, RMContainer> newlyUpdatedContainers,
      ContainerUpdateType updateTpe) {
    List<Container> updatedContainers = new ArrayList<>();
    if (oppContainerContext == null &&
        (ContainerUpdateType.DEMOTE_EXECUTION_TYPE == updateTpe
            || ContainerUpdateType.PROMOTE_EXECUTION_TYPE == updateTpe)) {
      return updatedContainers;
    }

    writeLock.lock();
    try {
      Iterator<Map.Entry<ContainerId, RMContainer>> i =
          newlyUpdatedContainers.entrySet().iterator();
      while (i.hasNext()) {
        Map.Entry<ContainerId, RMContainer> entry = i.next();
        ContainerId matchedContainerId = entry.getKey();
        RMContainer tempRMContainer = entry.getValue();

        RMContainer existingRMContainer =
            getRMContainer(matchedContainerId);
        if (existingRMContainer != null) {
          // swap containers
          existingRMContainer = getUpdateContext().swapContainer(
              tempRMContainer, existingRMContainer, updateTpe);
          getUpdateContext().removeFromOutstandingUpdate(
              tempRMContainer.getAllocatedSchedulerKey(),
              existingRMContainer.getContainer());
          Container updatedContainer = updateContainerAndNMToken(
              existingRMContainer, updateTpe);
          updatedContainers.add(updatedContainer);
        }
        tempContainerToKill.add(tempRMContainer);
        i.remove();
      }
      // Release all temporary containers
      Iterator<RMContainer> tempIter = tempContainerToKill.iterator();
      while (tempIter.hasNext()) {
        RMContainer c = tempIter.next();
        // Mark container for release (set RRs to null, so RM does not think
        // it is a recoverable container)
        ((RMContainerImpl) c).setContainerRequest(null);

        // Release this container async-ly so as to prevent
        // 'LeafQueue::completedContainer()' from trying to acquire a lock
        // on the app and queue which can contended for in the reverse order
        // by the Scheduler thread.
        ((AbstractYarnScheduler)rmContext.getScheduler())
            .asyncContainerRelease(c);
        tempIter.remove();
      }
      return updatedContainers;
    } finally {
      writeLock.unlock();
    }
  }

  public List<NMToken> pullUpdatedNMTokens() {
    writeLock.lock();
    try {
      List <NMToken> returnList = new ArrayList<>(updatedNMTokens);
      updatedNMTokens.clear();
      return returnList;
    } finally {
      writeLock.unlock();
    }

  }

  public boolean isWaitingForAMContainer() {
    // The working knowledge is that masterContainer for AM is null as it
    // itself is the master container.
    return (!unmanagedAM && appAttempt.getMasterContainer() == null);
  }

  public void updateBlacklist(List<String> blacklistAdditions,
      List<String> blacklistRemovals) {
    writeLock.lock();
    try {
      if (!isStopped) {
        if (isWaitingForAMContainer()) {
          // The request is for the AM-container, and the AM-container is
          // launched by the system. So, update the places that are blacklisted
          // by system (as opposed to those blacklisted by the application).
          this.appSchedulingInfo.updatePlacesBlacklistedBySystem(
              blacklistAdditions, blacklistRemovals);
        } else{
          this.appSchedulingInfo.updatePlacesBlacklistedByApp(
              blacklistAdditions, blacklistRemovals);
        }
      }
    } finally {
      writeLock.unlock();
    }
  }

  public boolean isPlaceBlacklisted(String resourceName) {
    readLock.lock();
    try {
      boolean forAMContainer = isWaitingForAMContainer();
      return this.appSchedulingInfo.isPlaceBlacklisted(resourceName,
          forAMContainer);
    } finally {
      readLock.unlock();
    }
  }

  public int addMissedNonPartitionedRequestSchedulingOpportunity(
      SchedulerRequestKey schedulerKey) {
    try {
      return missedNonPartitionedReqSchedulingOpportunity.add(
          schedulerKey, 1) + 1;
    } catch (IllegalArgumentException e) {
      // This happens when count = MAX_INT, ignore the exception
      return Integer.MAX_VALUE;
    }
  }

  public void
      resetMissedNonPartitionedRequestSchedulingOpportunity(
      SchedulerRequestKey schedulerKey) {
    missedNonPartitionedReqSchedulingOpportunity.setCount(schedulerKey, 0);
  }

  
  public void addSchedulingOpportunity(
      SchedulerRequestKey schedulerKey) {
    try {
      schedulingOpportunities.add(schedulerKey, 1);
    } catch (IllegalArgumentException e) {
      // This happens when count = MAX_INT, ignore the exception
    }
  }
  
  public void subtractSchedulingOpportunity(
      SchedulerRequestKey schedulerKey) {
    this.schedulingOpportunities.removeExactly(schedulerKey, 1);
  }

  /**
   * Return the number of times the application has been given an opportunity
   * to schedule a task at the given priority since the last time it
   * successfully did so.
   * @param schedulerKey Scheduler Key
   * @return number of scheduling opportunities
   */
  public int getSchedulingOpportunities(
      SchedulerRequestKey schedulerKey) {
    return schedulingOpportunities.count(schedulerKey);
  }
  
  /**
   * Should be called when an application has successfully scheduled a
   * container, or when the scheduling locality threshold is relaxed.
   * Reset various internal counters which affect delay scheduling
   *
   * @param schedulerKey The priority of the container scheduled.
   */
  public void resetSchedulingOpportunities(
      SchedulerRequestKey schedulerKey) {
    resetSchedulingOpportunities(schedulerKey, System.currentTimeMillis());
  }

  // used for continuous scheduling
  public void resetSchedulingOpportunities(SchedulerRequestKey schedulerKey,
      long currentTimeMs) {
    lastScheduledContainer.put(schedulerKey, currentTimeMs);
    schedulingOpportunities.setCount(schedulerKey, 0);
  }

  @VisibleForTesting
  void setSchedulingOpportunities(SchedulerRequestKey schedulerKey, int count) {
    schedulingOpportunities.setCount(schedulerKey, count);
  }

  private AggregateAppResourceUsage getRunningAggregateAppResourceUsage() {
    long currentTimeMillis = System.currentTimeMillis();
    // Don't walk the whole container list if the resources were computed
    // recently.
    if ((currentTimeMillis - lastMemoryAggregateAllocationUpdateTime)
        > MEM_AGGREGATE_ALLOCATION_CACHE_MSECS) {
      Map<String, Long> resourceSecondsMap = new HashMap<>();
      for (RMContainer rmContainer : this.liveContainers.values()) {
        long usedMillis = currentTimeMillis - rmContainer.getCreationTime();
        Resource resource = rmContainer.getContainer().getResource();
        for (ResourceInformation entry : resource.getResources()) {
          long value = RMServerUtils
              .getOrDefault(resourceSecondsMap, entry.getName(), 0L);
          value += entry.getValue() * usedMillis
              / DateUtils.MILLIS_PER_SECOND;
          resourceSecondsMap.put(entry.getName(), value);
        }
      }

      lastMemoryAggregateAllocationUpdateTime = currentTimeMillis;
      lastResourceSecondsMap = resourceSecondsMap;
    }
    return new AggregateAppResourceUsage(lastResourceSecondsMap);
  }

  public ApplicationResourceUsageReport getResourceUsageReport() {
    writeLock.lock();
    try {
      AggregateAppResourceUsage runningResourceUsage =
          getRunningAggregateAppResourceUsage();
      Resource usedResourceClone = Resources.clone(
          attemptResourceUsage.getAllUsed());
      Resource reservedResourceClone =
          Resources.clone(attemptResourceUsage.getAllReserved());
      Resource cluster = rmContext.getScheduler().getClusterResource();
      ResourceCalculator calc =
          rmContext.getScheduler().getResourceCalculator();
      Map<String, Long> preemptedResourceSecondsMaps = new HashMap<>();
      preemptedResourceSecondsMaps
          .put(ResourceInformation.MEMORY_MB.getName(), 0L);
      preemptedResourceSecondsMaps
          .put(ResourceInformation.VCORES.getName(), 0L);
      float queueUsagePerc = 0.0f;
      float clusterUsagePerc = 0.0f;
      if (!calc.isAllInvalidDivisor(cluster)) {
        float queueCapacityPerc = queue.getQueueInfo(false, false)
            .getCapacity();
        queueUsagePerc = calc.divide(cluster, usedResourceClone,
            Resources.multiply(cluster, queueCapacityPerc)) * 100;
        if (Float.isNaN(queueUsagePerc) || Float.isInfinite(queueUsagePerc)) {
          queueUsagePerc = 0.0f;
        }
        clusterUsagePerc =
            calc.divide(cluster, usedResourceClone, cluster) * 100;
      }
      return ApplicationResourceUsageReport
          .newInstance(liveContainers.size(), reservedContainers.size(),
              usedResourceClone, reservedResourceClone,
              Resources.add(usedResourceClone, reservedResourceClone),
              runningResourceUsage.getResourceUsageSecondsMap(), queueUsagePerc,
              clusterUsagePerc, preemptedResourceSecondsMaps);
    } finally {
      writeLock.unlock();
    }
  }

  @VisibleForTesting
  public Map<ContainerId, RMContainer> getLiveContainersMap() {
    return this.liveContainers;
  }

  public Map<SchedulerRequestKey, Long>
      getLastScheduledContainer() {
    return this.lastScheduledContainer;
  }

  public void transferStateFromPreviousAttempt(
      SchedulerApplicationAttempt appAttempt) {
    writeLock.lock();
    try {
      this.liveContainers = appAttempt.getLiveContainersMap();
      // this.reReservations = appAttempt.reReservations;
      this.attemptResourceUsage.copyAllUsed(appAttempt.attemptResourceUsage);
      this.setHeadroom(appAttempt.resourceLimit);
      // this.currentReservation = appAttempt.currentReservation;
      // this.newlyAllocatedContainers = appAttempt.newlyAllocatedContainers;
      // this.schedulingOpportunities = appAttempt.schedulingOpportunities;
      this.lastScheduledContainer = appAttempt.getLastScheduledContainer();
      this.appSchedulingInfo.transferStateFromPreviousAppSchedulingInfo(
          appAttempt.appSchedulingInfo);
    } finally {
      writeLock.unlock();
    }
  }
  
  public void move(Queue newQueue) {
    writeLock.lock();
    try {
      QueueMetrics oldMetrics = queue.getMetrics();
      QueueMetrics newMetrics = newQueue.getMetrics();
      String newQueueName = newQueue instanceof CSQueue ?
              ((CSQueue) newQueue).getQueuePath() : newQueue.getQueueName();
      String user = getUser();

      for (RMContainer liveContainer : liveContainers.values()) {
        Resource resource = liveContainer.getContainer().getResource();
        ((RMContainerImpl) liveContainer).setQueueName(newQueueName);
        oldMetrics.releaseResources(liveContainer.getNodeLabelExpression(),
            user, 1, resource);
        newMetrics.allocateResources(liveContainer.getNodeLabelExpression(),
            user, 1, resource, false);
      }
      for (Map<NodeId, RMContainer> map : reservedContainers.values()) {
        for (RMContainer reservedContainer : map.values()) {
          ((RMContainerImpl) reservedContainer).setQueueName(newQueueName);
          Resource resource = reservedContainer.getReservedResource();
          oldMetrics.unreserveResource(
              reservedContainer.getNodeLabelExpression(), user, resource);
          newMetrics.reserveResource(
              reservedContainer.getNodeLabelExpression(), user, resource);
        }
      }

      if (!isStopped) {
        appSchedulingInfo.move(newQueue);
      }
      this.queue = newQueue;
    } finally {
      writeLock.unlock();
    }
  }

  public boolean recoverContainer(SchedulerNode node,
      RMContainer rmContainer) {
    writeLock.lock();
    try {
      // recover app scheduling info
      appSchedulingInfo.recoverContainer(rmContainer, node.getPartition());

      if (rmContainer.getState().equals(RMContainerState.COMPLETED)) {
        return false;
      }
      LOG.info("SchedulerAttempt " + getApplicationAttemptId()
          + " is recovering container " + rmContainer.getContainerId());
      addRMContainer(rmContainer.getContainerId(), rmContainer);
      if (rmContainer.getExecutionType() == ExecutionType.GUARANTEED) {
        attemptResourceUsage.incUsed(node.getPartition(),
            rmContainer.getContainer().getResource());
      }

      return true;

      // resourceLimit: updated when LeafQueue#recoverContainer#allocateResource
      // is called.
      // newlyAllocatedContainers.add(rmContainer);
      // schedulingOpportunities
      // lastScheduledContainer
    } finally {
      writeLock.unlock();
    }
  }

  public void incNumAllocatedContainers(NodeType containerType,
      NodeType requestType) {
    if (containerType == null || requestType == null) {
      // Sanity check
      return;
    }

    RMApp app = rmContext.getRMApps().get(attemptId.getApplicationId());
    if (app != null) {
      RMAppAttempt attempt = app.getCurrentAppAttempt();
      if (attempt != null) {
        attempt.getRMAppAttemptMetrics()
            .incNumAllocatedContainers(containerType, requestType);
      }
    }
  }

  public void setApplicationHeadroomForMetrics(Resource headroom) {
    RMAppAttempt attempt =
        rmContext.getRMApps().get(attemptId.getApplicationId())
            .getCurrentAppAttempt();
    if (attempt != null) {
      attempt.getRMAppAttemptMetrics().setApplicationAttemptHeadRoom(
          Resources.clone(headroom));
    }
  }

  public void recordContainerRequestTime(long value) {
    firstAllocationRequestSentTime.compareAndSet(0, value);
  }

  public void recordContainerAllocationTime(long value) {
    if (firstContainerAllocatedTime.compareAndSet(0, value)) {
      long timediff = firstContainerAllocatedTime.longValue() -
          firstAllocationRequestSentTime.longValue();
      if (timediff > 0) {
        queue.getMetrics().addAppAttemptFirstContainerAllocationDelay(timediff);
      }
    }
  }

  public Set<String> getBlacklistedNodes() {
    return this.appSchedulingInfo.getBlackListCopy();
  }
  
  @Private
  public boolean hasPendingResourceRequest(String nodePartition,
      SchedulingMode schedulingMode) {
    // We need to consider unconfirmed allocations
    if (schedulingMode == SchedulingMode.IGNORE_PARTITION_EXCLUSIVITY) {
      nodePartition = RMNodeLabelsManager.NO_LABEL;
    }

    Resource pending = attemptResourceUsage.getPending(nodePartition);

    // TODO, need consider node partition here
    // To avoid too many allocation-proposals rejected for non-default
    // partition allocation
    if (StringUtils.equals(nodePartition, RMNodeLabelsManager.NO_LABEL)) {
      pending = Resources.subtractNonNegative(pending, Resources
          .createResource(unconfirmedAllocatedMem.get(),
              unconfirmedAllocatedVcores.get()));
    }

    return !Resources.isNone(pending);
  }

  /*
   * Note that the behavior of appAttemptResourceUsage is different from queue's
   * For queue, used = actual-used + reserved
   * For app, used = actual-used.
   *
   * TODO (wangda): Need to make behaviors of queue/app's resource usage
   * consistent
   */
  @VisibleForTesting
  public ResourceUsage getAppAttemptResourceUsage() {
    return this.attemptResourceUsage;
  }

  @Override
  public Priority getPriority() {
    return appPriority;
  }

  public void setPriority(Priority appPriority) {
    this.appPriority = appPriority;
  }

  @Override
  public String getId() {
    return getApplicationId().toString();
  }
  
  @Override
  public int compareInputOrderTo(SchedulableEntity other) {
    if (other instanceof SchedulerApplicationAttempt) {
      return getApplicationId().compareTo(
        ((SchedulerApplicationAttempt)other).getApplicationId());
    }
    return 1;//let other types go before this, if any
  }
  
  @Override
  public ResourceUsage getSchedulingResourceUsage() {
    return attemptResourceUsage;
  }

  public void setAppAMNodePartitionName(String partitionName) {
    this.appAMNodePartitionName = partitionName;
  }

  public String getAppAMNodePartitionName() {
    return appAMNodePartitionName;
  }

  public void updateAMContainerDiagnostics(AMState state,
      String diagnosticMessage) {
    if (!isWaitingForAMContainer()) {
      return;
    }
    StringBuilder diagnosticMessageBldr = new StringBuilder();
    diagnosticMessageBldr.append("[")
        .append(fdf.format(System.currentTimeMillis()))
        .append("] ");
    switch (state) {
    case INACTIVATED:
      diagnosticMessageBldr.append(state.diagnosticMessage);
      if (diagnosticMessage != null) {
        diagnosticMessageBldr.append(diagnosticMessage);
      }
      getPendingAppDiagnosticMessage(diagnosticMessageBldr);
      break;
    case ACTIVATED:
      diagnosticMessageBldr.append(state.diagnosticMessage);
      if (diagnosticMessage != null) {
        diagnosticMessageBldr.append(diagnosticMessage);
      }
      getActivedAppDiagnosticMessage(diagnosticMessageBldr);
      break;
    default:
      // UNMANAGED , ASSIGNED
      diagnosticMessageBldr.append(state.diagnosticMessage);
      break;
    }
    appAttempt.updateAMLaunchDiagnostics(diagnosticMessageBldr.toString());
  }

  protected void getPendingAppDiagnosticMessage(
      StringBuilder diagnosticMessage) {
    // Give the specific information which might be applicable for the
    // respective scheduler
    // like partitionAMResourcelimit,UserAMResourceLimit, queue'AMResourceLimit
  }

  protected void getActivedAppDiagnosticMessage(
      StringBuilder diagnosticMessage) {
    // Give the specific information which might be applicable for the
    // respective scheduler
    // queue's resource usage for specific partition
  }

  public ReentrantReadWriteLock.WriteLock getWriteLock() {
    return writeLock;
  }

  @Override
  public boolean isRecovering() {
    return isAttemptRecovering;
  }

  protected void setAttemptRecovering(boolean isRecovering) {
    this.isAttemptRecovering = isRecovering;
  }

  public <N extends SchedulerNode> AppPlacementAllocator<N> getAppPlacementAllocator(
      SchedulerRequestKey schedulerRequestKey) {
    return appSchedulingInfo.getAppPlacementAllocator(schedulerRequestKey);
  }
  public void incUnconfirmedRes(Resource res) {
    unconfirmedAllocatedMem.addAndGet(res.getMemorySize());
    unconfirmedAllocatedVcores.addAndGet(res.getVirtualCores());
  }

  public void decUnconfirmedRes(Resource res) {
    unconfirmedAllocatedMem.addAndGet(-res.getMemorySize());
    unconfirmedAllocatedVcores.addAndGet(-res.getVirtualCores());
  }

  @Override
  public int hashCode() {
    return getApplicationAttemptId().hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (! (o instanceof SchedulerApplicationAttempt)) {
      return false;
    }

    SchedulerApplicationAttempt other = (SchedulerApplicationAttempt) o;
    return (this == other ||
        this.getApplicationAttemptId().equals(other.getApplicationAttemptId()));
  }

  /**
   * Different state for Application Master, user can see this state from web UI
   */
  public enum AMState {
    UNMANAGED("User launched the Application Master, since it's unmanaged. "),
    INACTIVATED("Application is added to the scheduler and is not yet activated. "),
    ACTIVATED("Application is Activated, waiting for resources to be assigned for AM. "),
    ASSIGNED("Scheduler has assigned a container for AM, waiting for AM "
        + "container to be launched"),
    LAUNCHED("AM container is launched, waiting for AM container to Register "
        + "with RM")
    ;

    private String diagnosticMessage;

    AMState(String diagnosticMessage) {
      this.diagnosticMessage = diagnosticMessage;
    }

    public String getDiagnosticMessage() {
      return diagnosticMessage;
    }
  }

  public Map<String, String> getApplicationSchedulingEnvs() {
    return this.applicationSchedulingEnvs;
  }

  @Override
  public String getPartition() {
    return nodeLabelExpression == null ? "" : nodeLabelExpression;
  }


  @Override
  public long getStartTime() {
    return startTime;
  }
}
