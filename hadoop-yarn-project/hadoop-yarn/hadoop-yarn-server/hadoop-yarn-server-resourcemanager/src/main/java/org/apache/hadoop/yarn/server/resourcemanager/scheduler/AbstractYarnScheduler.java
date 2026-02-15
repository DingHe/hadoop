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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ContainerUpdateType;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.api.records.UpdateContainerError;
import org.apache.hadoop.yarn.api.records.UpdateContainerRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.InvalidResourceRequestException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SchedulerResourceTypes;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.metrics.OpportunisticSchedulerMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.RMAppManagerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.RMAppManagerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMCriticalThreadUncaughtExceptionHandler;
import org.apache.hadoop.yarn.server.resourcemanager.RMServerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.SchedulingMonitorManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerFinishedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer
    .RMContainerNMDoneChangeResourceEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerRecoverEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeCleanContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeFinishedContainersPulledByAMEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeResourceUpdateEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ContainerRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.QueueEntitlement;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerPreemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ReleaseContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;
import org.apache.hadoop.yarn.server.scheduler.OpportunisticContainerContext;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.server.utils.Lock;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.SettableFuture;

// 旨在为资源调度器提供通用的功能实现，允许具体的调度器（如公平调度器、容量调度器等）继承并扩展它的功能。
// 这个类负责管理 YARN 集群中的节点、应用、容器等资源的调度。它提供了管理应用、容器资源、节点资源、调度活动以及周期性任务的能力
@SuppressWarnings("unchecked")
@Private
@Unstable
public abstract class AbstractYarnScheduler
    <T extends SchedulerApplicationAttempt, N extends SchedulerNode>
    extends AbstractService implements ResourceScheduler {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractYarnScheduler.class);

  private static final Resource ZERO_RESOURCE = Resource.newInstance(0, 0);
  //用于跟踪集群中各个节点的状态和资源信息
  protected final ClusterNodeTracker<N> nodeTracker =
      new ClusterNodeTracker<>();
  //集群中单个容器分配的最小资源要求
  protected Resource minimumAllocation;

  protected volatile RMContext rmContext;
  //集群级别应用的最大优先级，用于调度决策中，控制不同应用的优先级
  private volatile Priority maxClusterLevelAppPriority;
  //用于管理调度活动的管理器。调度活动包括容器请求、资源释放、节点更新等
  protected ActivitiesManager activitiesManager;
  //调度器健康状态的对象，用于监控和报告调度器的健康状况
  protected SchedulerHealth schedulerHealth = new SchedulerHealth();
  //最后一次节点更新的时间戳，用于跟踪节点状态的更新时间
  protected volatile long lastNodeUpdateTime;

  // timeout to join when we stop this service
  //停止服务时等待线程结束的超时时间
  protected final long THREAD_JOIN_TIMEOUT_MS = 1000;

  private volatile Clock clock;

  /**
   * To enable the update thread, subclasses should set updateInterval to a
   * positive value during {@link #serviceInit(Configuration)}.
   */
  //调度器更新的间隔时间。如果设置为正值，调度器会定期更新资源状态
  protected long updateInterval = -1L;
  @VisibleForTesting
  //用于定期更新调度器状态的线程。如果设置了 updateInterval，则会启动此线程
  Thread updateThread;
  //用于同步更新线程的对象，防止多线程竞争
  private final Object updateThreadMonitor = new Object();
  //用于释放缓存的定时任务器，定期清理无用的资源
  private Timer releaseCache;

  /*
   * All schedulers which are inheriting AbstractYarnScheduler should use
   * concurrent version of 'applications' map.
   */
  //存储当前所有应用的信息。每个应用都在调度器中有一个对应的 SchedulerApplication 实例
  protected ConcurrentMap<ApplicationId, SchedulerApplication<T>> applications;
  //节点失效的时间间隔。如果节点长时间没有与资源管理器通信，就会被认为失效
  protected int nmExpireInterval;
  //节点与资源管理器之间的心跳间隔
  protected long nmHeartbeatInterval;
  //跳过节点的时间间隔，某些情况下可能会跳过某些节点的调度
  private long skipNodeInterval;
  //一个空的容器列表，用于表示没有容器的情况
  private final static List<Container> EMPTY_CONTAINER_LIST =
      new ArrayList<Container>();
  //表示没有分配资源的空分配对象
  protected static final Allocation EMPTY_ALLOCATION = new Allocation(
    EMPTY_CONTAINER_LIST, Resources.createResource(0), null, null, null);

  protected final ReentrantReadWriteLock.ReadLock readLock;

  /*
   * Use writeLock for any of operations below:
   * - queue change (hierarchy / configuration / container allocation)
   * - application(add/remove/allocate-container, but not include container
   *   finish)
   * - node (add/remove/change-resource/container-allocation, but not include
   *   container finish)
   */
  protected final ReentrantReadWriteLock.WriteLock writeLock;

  // If set to true, then ALL container updates will be automatically sent to
  // the NM in the next heartbeat.
  //如果设置为 true，则所有容器的更新将在下一次心跳中自动发送到 NodeManager
  private boolean autoUpdateContainers = false;
  //调度监控管理器，用于监控调度器的各种活动
  protected SchedulingMonitorManager schedulingMonitorManager =
      new SchedulingMonitorManager();
  //表示是否启用了调度迁移模式，可能与集群或节点的迁移相关
  private boolean migration;

  /**
   * Construct the service.
   *
   * @param name service name
   */
  public AbstractYarnScheduler(String name) {
    super(name);
    clock = SystemClock.getInstance();
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    migration =
        conf.getBoolean(FairSchedulerConfiguration.MIGRATION_MODE, false);

    nmExpireInterval =
        conf.getInt(YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS,
          YarnConfiguration.DEFAULT_RM_NM_EXPIRY_INTERVAL_MS);
    nmHeartbeatInterval =
        conf.getLong(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS,
            YarnConfiguration.DEFAULT_RM_NM_HEARTBEAT_INTERVAL_MS);
    skipNodeInterval = YarnConfiguration.getSkipNodeInterval(conf);
    long configuredMaximumAllocationWaitTime =
        conf.getLong(YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_SCHEDULING_WAIT_MS,
          YarnConfiguration.DEFAULT_RM_WORK_PRESERVING_RECOVERY_SCHEDULING_WAIT_MS);
    nodeTracker.setConfiguredMaxAllocationWaitTime(
        configuredMaximumAllocationWaitTime);
    maxClusterLevelAppPriority = getMaxPriorityFromConf(conf);
    if (!migration) {
      this.releaseCache = new Timer("Pending Container Clear Timer");
    }

    autoUpdateContainers =
        conf.getBoolean(YarnConfiguration.RM_AUTO_UPDATE_CONTAINERS,
            YarnConfiguration.DEFAULT_RM_AUTO_UPDATE_CONTAINERS);

    if (updateInterval > 0) {
      updateThread = new UpdateThread();
      updateThread.setName("SchedulerUpdateThread");
      updateThread.setUncaughtExceptionHandler(
          new RMCriticalThreadUncaughtExceptionHandler(rmContext));
      updateThread.setDaemon(true);
    }
    super.serviceInit(conf);

  }

  @Override
  protected void serviceStart() throws Exception {
    if (!migration) {
      if (updateThread != null) {
        updateThread.start();
      }
      schedulingMonitorManager.startAll();
      createReleaseCache();
    }

    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (updateThread != null) {
      updateThread.interrupt();
      updateThread.join(THREAD_JOIN_TIMEOUT_MS);
    }

    //Stop Timer
    if (releaseCache != null) {
      releaseCache.cancel();
      releaseCache = null;
    }
    schedulingMonitorManager.stop();
    super.serviceStop();
  }

  @VisibleForTesting
  public ClusterNodeTracker<N> getNodeTracker() {
    return nodeTracker;
  }

  @VisibleForTesting
  public SchedulingMonitorManager getSchedulingMonitorManager() {
    return schedulingMonitorManager;
  }

  /*
   * YARN-3136 removed synchronized lock for this method for performance
   * purposes
   */
  //ApplicationAttemptId currentAttempt：当前应用尝试的 ID
  public List<Container> getTransferredContainers(
      ApplicationAttemptId currentAttempt) {
    //获取 ApplicationId
    ApplicationId appId = currentAttempt.getApplicationId();
    SchedulerApplication<T> app = applications.get(appId);
    List<Container> containerList = new ArrayList<Container>();
    if (app == null) {
      return containerList;
    }
    //获取当前尝试需要转移的容器
    Collection<RMContainer> liveContainers = app.getCurrentAppAttempt()
        .pullContainersToTransfer();
    ContainerId amContainerId = null;
    // For UAM, amContainer would be null
    //获取 Application Master（AM）容器 ID
    if (rmContext.getRMApps().get(appId).getCurrentAppAttempt()
        .getMasterContainer() != null) {
      amContainerId = rmContext.getRMApps().get(appId).getCurrentAppAttempt()
          .getMasterContainer().getId();
    }
    for (RMContainer rmContainer : liveContainers) {
      //如果 rmContainer 是 AM 容器，则跳过
      //否则，将其 Container 添加到 containerList
      if (!rmContainer.getContainerId().equals(amContainerId)) {
        containerList.add(rmContainer.getContainer());
      }
    }
    return containerList;
  }

  public Map<ApplicationId, SchedulerApplication<T>>
      getSchedulerApplications() {
    return applications;
  }

  /**
   * Add blacklisted NodeIds to the list that is passed.
   *
   * @param app application attempt.
   * @return blacklisted NodeIds.
   */
  public List<N> getBlacklistedNodes(final SchedulerApplicationAttempt app) {

    NodeFilter nodeFilter = new NodeFilter() {
      @Override
      public boolean accept(SchedulerNode node) {
        return SchedulerAppUtils.isPlaceBlacklisted(app, node, LOG);
      }
    };
    return nodeTracker.getNodes(nodeFilter);
  }

  public List<N> getNodes(final NodeFilter filter) {
    return nodeTracker.getNodes(filter);
  }

  public boolean shouldContainersBeAutoUpdated() {
    return this.autoUpdateContainers;
  }

  @Override
  public Resource getClusterResource() {
    return nodeTracker.getClusterCapacity();
  }

  @Override
  public Resource getMinimumResourceCapability() {
    return minimumAllocation;
  }

  @Override
  public Resource getMaximumResourceCapability() {
    return nodeTracker.getMaxAllowedAllocation();
  }

  @Override
  public Resource getMaximumResourceCapability(String queueName) {
    return getMaximumResourceCapability();
  }

  protected void initMaximumResourceCapability(Resource maximumAllocation) {
    nodeTracker.setConfiguredMaxAllocation(maximumAllocation);
  }

  public SchedulerHealth getSchedulerHealth() {
    return this.schedulerHealth;
  }

  protected void setLastNodeUpdateTime(long time) {
    this.lastNodeUpdateTime = time;
  }

  public long getLastNodeUpdateTime() {
    return lastNodeUpdateTime;
  }

  public long getSkipNodeInterval(){
    return skipNodeInterval;
  }

  //处理某个 Container 在 SchedulerNode 上成功启动的情况。其主要功能包括：
  //查找该 Container 所属的 ApplicationAttempt（应用尝试）。
  //如果 ApplicationAttempt 不存在，认为是未知应用，并触发清理事件，要求 NodeManager 释放该 Container。
  //如果 ApplicationAttempt 存在，调用 SchedulerApplicationAttempt 和 SchedulerNode 进行状态更新，标记该 Container 已在节点上启动

  protected void containerLaunchedOnNode(
      ContainerId containerId, SchedulerNode node) {
    readLock.lock();
    try {
      // Get the application for the finished container
      //查找 containerId 所属的 ApplicationAttempt（应用尝试）
      SchedulerApplicationAttempt application =
          getCurrentAttemptForContainer(containerId);
      if (application == null) {
        LOG.info("Unknown application " + containerId.getApplicationAttemptId()
            .getApplicationId() + " launched container " + containerId
            + " on node: " + node);
        this.rmContext.getDispatcher().getEventHandler().handle(
            new RMNodeCleanContainerEvent(node.getNodeID(), containerId));
        return;
      }
      //调用 SchedulerApplicationAttempt 的 containerLaunchedOnNode 方法，更新应用尝试的调度状态，记录 Container 已成功在 node 上启动
      application.containerLaunchedOnNode(containerId, node.getNodeID());
      node.containerStarted(containerId);
    } finally {
      readLock.unlock();
    }
  }
  //处理 YARN 容器资源扩展（Container Resource Increase）事件。
  // 当 NodeManager（NM）报告某个 Container 资源已成功增加时，调度器需要：
  //确保该 Container 仍属于有效的 ApplicationAttempt。
  //确保 Container 存在于调度器的管理状态中。
  //更新 RMContainer 的资源信息，使 YARN 资源管理器（RM）的视图与 NodeManager 保持一致。
  //如果 Container 或 ApplicationAttempt 不存在，触发清理逻辑，让 NodeManager 释放该 Container
  //ContainerId containerId：需要更新资源的 Container ID。
  //SchedulerNode node：该 Container 运行的 SchedulerNode，即 YARN 资源调度器管理的物理节点
  //Container increasedContainerReportedByNM：NodeManager（NM）报告的 Container 新资源信息
  protected void containerIncreasedOnNode(ContainerId containerId,
      SchedulerNode node, Container increasedContainerReportedByNM) {
    /*
     * No lock is required, as this method is protected by scheduler's writeLock
     */
    // Get the application for the finished container
    //获取 ApplicationAttempt
    SchedulerApplicationAttempt application = getCurrentAttemptForContainer(
        containerId);
    if (application == null) {
      //如果 ApplicationAttempt 为空，说明 Container 属于未知或已完成的应用，触发清理事件并返回
      LOG.info("Unknown application " + containerId.getApplicationAttemptId()
          .getApplicationId() + " increased container " + containerId
          + " on node: " + node);
      this.rmContext.getDispatcher().getEventHandler().handle(
          new RMNodeCleanContainerEvent(node.getNodeID(), containerId));
      return;
    }

    RMContainer rmContainer = getRMContainer(containerId);
    if (rmContainer == null) {
      // Some unknown container sneaked into the system. Kill it.
      this.rmContext.getDispatcher().getEventHandler().handle(
          new RMNodeCleanContainerEvent(node.getNodeID(), containerId));
      return;
    }
    //通过 RMContainerNMDoneChangeResourceEvent 更新 RMContainer 的资源，使其与 NM 报告的资源一致
    rmContainer.handle(new RMContainerNMDoneChangeResourceEvent(containerId,
        increasedContainerReportedByNM.getResource()));

  }

  // TODO: Rename it to getCurrentApplicationAttempt
  public T getApplicationAttempt(ApplicationAttemptId applicationAttemptId) {
    SchedulerApplication<T> app = applications.get(
        applicationAttemptId.getApplicationId());
    return app == null ? null : app.getCurrentAppAttempt();
  }

  @Override
  public SchedulerAppReport getSchedulerAppInfo(
      ApplicationAttemptId appAttemptId) {
    SchedulerApplicationAttempt attempt = getApplicationAttempt(appAttemptId);
    if (attempt == null) {
      LOG.debug("Request for appInfo of unknown attempt {}", appAttemptId);
      return null;
    }
    return new SchedulerAppReport(attempt);
  }

  @Override
  public ApplicationResourceUsageReport getAppResourceUsageReport(
      ApplicationAttemptId appAttemptId) {
    SchedulerApplicationAttempt attempt = getApplicationAttempt(appAttemptId);
    if (attempt == null) {
      LOG.debug("Request for appInfo of unknown attempt {}", appAttemptId);
      return null;
    }
    return attempt.getResourceUsageReport();
  }

  public T getCurrentAttemptForContainer(ContainerId containerId) {
    return getApplicationAttempt(containerId.getApplicationAttemptId());
  }

  @Override
  public RMContainer getRMContainer(ContainerId containerId) {
    SchedulerApplicationAttempt attempt =
        getCurrentAttemptForContainer(containerId);
    return (attempt == null) ? null : attempt.getRMContainer(containerId);
  }

  @Override
  public SchedulerNodeReport getNodeReport(NodeId nodeId) {
    return nodeTracker.getNodeReport(nodeId);
  }

  @Override
  public String moveApplication(ApplicationId appId, String newQueue)
      throws YarnException {
    throw new YarnException(getClass().getSimpleName()
        + " does not support moving apps between queues");
  }

  @Override
  public void preValidateMoveApplication(ApplicationId appId,
      String newQueue) throws YarnException {
    throw new YarnException(getClass().getSimpleName()
        + " does not support pre-validation of moving apps between queues");
  }

  public void removeQueue(String queueName) throws YarnException {
    throw new YarnException(getClass().getSimpleName()
        + " does not support removing queues");
  }

  @Override
  public void addQueue(Queue newQueue) throws YarnException, IOException {
    throw new YarnException(getClass().getSimpleName()
        + " does not support this operation");
  }

  @Override
  public void setEntitlement(String queue, QueueEntitlement entitlement)
      throws YarnException {
    throw new YarnException(getClass().getSimpleName()
        + " does not support this operation");
  }

  private void killOrphanContainerOnNode(RMNode node,
      NMContainerStatus container) {
    if (!container.getContainerState().equals(ContainerState.COMPLETE)) {
      this.rmContext.getDispatcher().getEventHandler().handle(
        new RMNodeCleanContainerEvent(node.getNodeID(),
          container.getContainerId()));
    }
  }
  // 用于在 ResourceManager（RM）恢复模式 下，从 NodeManager（NM） 端恢复 Container，确保 YARN 集群的 高可用性（HA）。
  // 当 RM 重启时，NodeManager 会报告其当前运行的 Container 状态，RM 需要处理这些信息，决定哪些 Container 可以继续运行，哪些需要被清理
  public void recoverContainersOnNode(List<NMContainerStatus> containerReports,
      RMNode nm) {
    writeLock.lock();
    try {
      //如果 RM 未开启工作保留恢复模式，则跳过恢复
      if (!rmContext.isWorkPreservingRecoveryEnabled()
          || containerReports == null || (containerReports != null
          && containerReports.isEmpty())) {
        return;
      }
      //遍历 NodeManager 上报的 Container
      for (NMContainerStatus container : containerReports) {
        //获取 ApplicationId 并检查应用是否存在
        ApplicationId appId =
            container.getContainerId().getApplicationAttemptId()
                .getApplicationId();
        RMApp rmApp = rmContext.getRMApps().get(appId);
        if (rmApp == null) {
          LOG.error("Skip recovering container " + container
              + " for unknown application.");
          killOrphanContainerOnNode(nm, container);
          continue;
        }
        //检查 SchedulerApplication 是否存在
        SchedulerApplication<T> schedulerApp = applications.get(appId);
        if (schedulerApp == null) {
          LOG.info("Skip recovering container  " + container
              + " for unknown SchedulerApplication. "
              + "Application current state is " + rmApp.getState());
          killOrphanContainerOnNode(nm, container);
          continue;
        }

        LOG.info("Recovering container " + container);
        //获取 SchedulerApplicationAttempt
        SchedulerApplicationAttempt schedulerAttempt =
            schedulerApp.getCurrentAppAttempt();
        //判断是否需要恢复 Container
        if (!rmApp.getApplicationSubmissionContext()
            .getKeepContainersAcrossApplicationAttempts()) {
          // Do not recover containers for stopped attempt or previous attempt.
          if (schedulerAttempt.isStopped() || !schedulerAttempt
              .getApplicationAttemptId().equals(
                  container.getContainerId().getApplicationAttemptId())) {
            LOG.info("Skip recovering container " + container
                + " for already stopped attempt.");
            killOrphanContainerOnNode(nm, container);
            continue;
          }
        }
        //获取 Queue 信息
        Queue queue = schedulerApp.getQueue();
        //To make sure we don't face ambiguity, CS queues should be referenced
        //by their full queue names
        String queueName =  queue instanceof CSQueue ?
            ((CSQueue)queue).getQueuePath() : queue.getQueueName();

        // create container
        //创建 RMContainer，表示恢复的 Container
        RMContainer rmContainer = recoverAndCreateContainer(container, nm,
            queueName);

        // recover RMContainer
        rmContainer.handle(
            new RMContainerRecoverEvent(container.getContainerId(), container));

        // recover scheduler node
        //恢复 Container 在 SchedulerNode 上的调度状态
        SchedulerNode schedulerNode = nodeTracker.getNode(nm.getNodeID());
        schedulerNode.recoverContainer(rmContainer);

        // recover queue: update headroom etc.
        //恢复调度队列
        Queue queueToRecover = schedulerAttempt.getQueue();
        queueToRecover.recoverContainer(getClusterResource(), schedulerAttempt,
            rmContainer);

        // recover scheduler attempt
        final boolean recovered = schedulerAttempt.recoverContainer(
            schedulerNode, rmContainer);

        if (recovered && rmContainer.getExecutionType() ==
            ExecutionType.OPPORTUNISTIC) {
          OpportunisticSchedulerMetrics.getMetrics()
              .incrAllocatedOppContainers(1);
        }
        // set master container for the current running AMContainer for this
        // attempt.
        //标注是否是AM
        RMAppAttempt appAttempt = rmApp.getCurrentAppAttempt();
        if (appAttempt != null) {
          Container masterContainer = appAttempt.getMasterContainer();

          // Mark current running AMContainer's RMContainer based on the master
          // container ID stored in AppAttempt.
          if (masterContainer != null && masterContainer.getId().equals(
              rmContainer.getContainerId())) {
            ((RMContainerImpl) rmContainer).setAMContainer(true);
          }
        }
        //检查 Container 是否已经被 Application 释放
        if (schedulerAttempt.getPendingRelease().remove(
            container.getContainerId())) {
          // release the container
          rmContainer.handle(
              new RMContainerFinishedEvent(container.getContainerId(),
                  SchedulerUtils
                      .createAbnormalContainerStatus(container.getContainerId(),
                          SchedulerUtils.RELEASED_CONTAINER),
                  RMContainerEventType.RELEASED));
          LOG.info(container.getContainerId() + " is released by application.");
        }
      }
    } finally {
      writeLock.unlock();
    }
  }
  //恢复容器
  private RMContainer recoverAndCreateContainer(NMContainerStatus status,
      RMNode node, String queueName) {
    Container container =
        Container.newInstance(status.getContainerId(), node.getNodeID(),
          node.getHttpAddress(), status.getAllocatedResource(),
          status.getPriority(), null);
    container.setVersion(status.getVersion());
    container.setExecutionType(status.getExecutionType());
    container.setAllocationRequestId(status.getAllocationRequestId());
    container.setAllocationTags(status.getAllocationTags());
    ApplicationAttemptId attemptId =
        container.getId().getApplicationAttemptId();
    RMContainer rmContainer = new RMContainerImpl(container,
        SchedulerRequestKey.extractFrom(container), attemptId, node.getNodeID(),
        applications.get(attemptId.getApplicationId()).getUser(), rmContext,
        status.getCreationTime(), status.getNodeLabelExpression());
    ((RMContainerImpl) rmContainer).setQueueName(queueName);
    return rmContainer;
  }

  /**
   * Recover resource request back from RMContainer when a container is
   * preempted before AM pulled the same. If container is pulled by
   * AM, then RMContainer will not have resource request to recover.
   * @param rmContainer rmContainer
   */
  // 恢复 ResourceRequest（资源请求），当 Container 在 AM（Application Master）提取资源之前被抢占（preempted） 时，
  // 调度器会将该资源请求重新添加回 SchedulerApplicationAttempt，以便后续继续调度
  private void recoverResourceRequestForContainer(RMContainer rmContainer) {
    ContainerRequest containerRequest = rmContainer.getContainerRequest();

    // If container state is moved to ACQUIRED, request will be empty.
    if (containerRequest == null) {
      return;
    }

    // Add resource request back to Scheduler ApplicationAttempt.

    // We lookup the application-attempt here again using
    // getCurrentApplicationAttempt() because there is only one app-attempt at
    // any point in the scheduler. But in corner cases, AMs can crash,
    // corresponding containers get killed and recovered to the same-attempt,
    // but because the app-attempt is extinguished right after, the recovered
    // requests don't serve any purpose, but that's okay.
    SchedulerApplicationAttempt schedulerAttempt =
        getCurrentAttemptForContainer(rmContainer.getContainerId());
    if (schedulerAttempt != null) {
      schedulerAttempt.recoverResourceRequestsForContainer(containerRequest);
    }
  }

  protected void createReleaseCache() {
    // Cleanup the cache after nm expire interval.
    releaseCache.schedule(new TimerTask() {
      @Override
      public void run() {
        clearPendingContainerCache();
        LOG.info("Release request cache is cleaned up");
      }
    }, nmExpireInterval);
  }

  @VisibleForTesting
  public void clearPendingContainerCache() {
    for (SchedulerApplication<T> app : applications.values()) {
      T attempt = app.getCurrentAppAttempt();
      if (attempt != null) {
        for (ContainerId containerId : attempt.getPendingRelease()) {
          RMAuditLogger.logFailure(app.getUser(),
              AuditConstants.RELEASE_CONTAINER,
              "Unauthorized access or invalid container", "Scheduler",
              "Trying to release container not owned by app "
                  + "or with invalid id.", attempt.getApplicationId(),
              containerId, null);
        }
        attempt.getPendingRelease().clear();
      }
    }
  }
  //completedContainer 处理 RMContainer 的完成事件。它负责：
  //释放 Container，确保 ResourceManager（RM）更新状态。
  //更新调度信息，释放 SchedulerNode 上的资源。
  //处理 Opportunistic 和 Guaranteed 类型 Container 的不同逻辑。
  //恢复 ResourceRequest，如果 Container 在 ACQUIRED 状态被杀死，需要重新申请资源
  @VisibleForTesting
  @Private
  // clean up a completed container
  public void completedContainer(RMContainer rmContainer,
      ContainerStatus containerStatus, RMContainerEventType event) {
    //处理 RMContainer 为空的情况
    if (rmContainer == null) {
      LOG.info("Container " + containerStatus.getContainerId()
          + " completed with event " + event
          + ", but corresponding RMContainer doesn't exist.");
      return;
    }
    //如果 Container 是 GUARANTEED（保证调度）
    if (rmContainer.getExecutionType() == ExecutionType.GUARANTEED) {
      completedContainerInternal(rmContainer, containerStatus, event);
      completeOustandingUpdatesWhichAreReserved(
          rmContainer, containerStatus, event);
    } else {
      //处理 Opportunistic 类型 Container
      ContainerId containerId = rmContainer.getContainerId();
      // Inform the container
      //触发 RMContainerFinishedEvent
      rmContainer.handle(
          new RMContainerFinishedEvent(containerId, containerStatus, event));
      SchedulerApplicationAttempt schedulerAttempt =
          getCurrentAttemptForContainer(containerId);
      if (schedulerAttempt != null) {
        if (schedulerAttempt.removeRMContainer(containerId)) {
          OpportunisticSchedulerMetrics.getMetrics()
              .incrReleasedOppContainers(1);
        }
      }
      LOG.debug("Completed container: {} in state: {} event:{}",
          rmContainer.getContainerId(), rmContainer.getState(), event);
      //释放 SchedulerNode 资源
      SchedulerNode node = getSchedulerNode(rmContainer.getNodeId());
      if (node != null) {
        node.releaseContainer(rmContainer.getContainerId(), false);
      }
    }

    // If the container is getting killed in ACQUIRED state, the requester (AM
    // for regular containers and RM itself for AM container) will not know what
    // happened. Simply add the ResourceRequest back again so that requester
    // doesn't need to do anything conditionally.
    //处理 ACQUIRED 状态下的 Container
    recoverResourceRequestForContainer(rmContainer);
  }

  // Optimization:
  // Check if there are in-flight container updates and complete the
  // associated temp containers. These are removed when the app completes,
  // but removing them when the actual container completes would allow the
  // scheduler to reallocate those resources sooner.
  //用于 优化调度器的资源回收。
  //当 Container 运行过程中 存在未完成的调度更新，该方法会 检查并回收 这些临时 Container。
  //目的：加速资源回收，使调度器能够 更快重新分配资源，而不是等到整个 Application 结束后才清理这些容器

  private void completeOustandingUpdatesWhichAreReserved(
      RMContainer rmContainer, ContainerStatus containerStatus,
      RMContainerEventType event) {
    //获取 SchedulerNode
    N schedulerNode = getSchedulerNode(rmContainer.getNodeId());
    //检查 SchedulerNode 是否有保留的 Container
    if (schedulerNode != null &&
        schedulerNode.getReservedContainer() != null) {
      RMContainer resContainer = schedulerNode.getReservedContainer();
      //调度键（SchedulerKey） 是调度系统用来识别 Container 的唯一标识
      //如果 SchedulerKey 存在，说明 Container 确实处于等待更新的状态
      if (resContainer.getReservedSchedulerKey() != null) {
        ContainerId containerToUpdate = resContainer
            .getReservedSchedulerKey().getContainerToUpdate();
        //如果 Container 正处于调度更新状态，那么 containerToUpdate 将指向它
        if (containerToUpdate != null &&
            containerToUpdate.equals(containerStatus.getContainerId())) {
          completedContainerInternal(resContainer,
              ContainerStatus.newInstance(resContainer.getContainerId(),
                  containerStatus.getState(), containerStatus
                      .getDiagnostics(),
                  containerStatus.getExitStatus()), event);
        }
      }
    }
  }

  // clean up a completed container
  protected abstract void completedContainerInternal(RMContainer rmContainer,
      ContainerStatus containerStatus, RMContainerEventType event);
  //用于 释放容器。它会检查容器是否存在，并处理释放请求。该方法的主要功能是：
  //如果容器存在，释放该容器并更新其状态。
  //如果容器不存在，它会根据容器是否在恢复过程中来决定是否将容器添加到待处理的释放列表中，或者记录释放失败的日志。
  protected void releaseContainers(List<ContainerId> containers,
      SchedulerApplicationAttempt attempt) {
    //遍历传入的容器ID列表，对每一个容器进行处理
    for (ContainerId containerId : containers) {
      RMContainer rmContainer = getRMContainer(containerId);
      if (rmContainer == null) {
        if (System.currentTimeMillis() - ResourceManager.getClusterTimeStamp()
            < nmExpireInterval) {
          LOG.info(containerId + " doesn't exist. Add the container"
              + " to the release request cache as it maybe on recovery.");
          attempt.getPendingRelease().add(containerId);
        } else {
          RMAuditLogger.logFailure(attempt.getUser(),
            AuditConstants.RELEASE_CONTAINER,
            "Unauthorized access or invalid container", "Scheduler",
            "Trying to release container not owned by app or with invalid id.",
            attempt.getApplicationId(), containerId, null);
        }
      }
      completedContainer(rmContainer,
        SchedulerUtils.createAbnormalContainerStatus(containerId,
          SchedulerUtils.RELEASED_CONTAINER), RMContainerEventType.RELEASED);
    }
  }

  @Override
  public N getSchedulerNode(NodeId nodeId) {
    return nodeTracker.getNode(nodeId);
  }

  @Override
  public void moveAllApps(String sourceQueue, String destQueue)
      throws YarnException {
    writeLock.lock();
    try {
      // check if destination queue is a valid leaf queue
      try {
        getQueueInfo(destQueue, false, false);
      } catch (IOException e) {
        LOG.warn(e.toString());
        throw new YarnException(e);
      }

      // generate move events for each pending/running app
      for (ApplicationAttemptId appAttemptId : getAppsFromQueue(sourceQueue)) {
        this.rmContext.getDispatcher().getEventHandler()
            .handle(new RMAppManagerEvent(appAttemptId.getApplicationId(),
                destQueue, RMAppManagerEventType.APP_MOVE));
      }
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void killAllAppsInQueue(String queueName)
      throws YarnException {
    writeLock.lock();
    try {
      // generate kill events for each pending/running app
      for (ApplicationAttemptId app : getAppsFromQueue(queueName)) {
        this.rmContext.getDispatcher().getEventHandler().handle(
            new RMAppEvent(app.getApplicationId(), RMAppEventType.KILL,
                "Application killed due to expiry of reservation queue "
                    + queueName + "."));
      }
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Process resource update on a node.
   *
   * @param nm RMNode.
   * @param resourceOption resourceOption.
   */
  public void updateNodeResource(RMNode nm,
      ResourceOption resourceOption) {
    writeLock.lock();
    try {
      SchedulerNode node = getSchedulerNode(nm.getNodeID());
      if (node == null) {
        LOG.info("Node: " + nm.getNodeID() + " has already been taken out of " +
            "scheduling. Skip updating its resource");
        return;
      }
      Resource newResource = resourceOption.getResource();
      final int timeout = resourceOption.getOverCommitTimeout();
      Resource oldResource = node.getTotalResource();
      if (!oldResource.equals(newResource)) {
        // Notify NodeLabelsManager about this change
        rmContext.getNodeLabelManager().updateNodeResource(nm.getNodeID(),
            newResource);

        // Log resource change
        LOG.info("Update resource on node: {} from: {}, to: {} in {} ms",
            node.getNodeName(), oldResource, newResource, timeout);

        nodeTracker.removeNode(nm.getNodeID());

        // update resource to node
        node.updateTotalResource(newResource);
        node.setOvercommitTimeOut(timeout);
        signalContainersIfOvercommitted(node, timeout == 0);

        nodeTracker.addNode((N) node);
      } else{
        // Log resource change
        LOG.warn("Update resource on node: " + node.getNodeName()
            + " with the same resource: " + newResource);
      }
    } finally {
      writeLock.unlock();
    }
  }

  /** {@inheritDoc} */
  @Override
  public EnumSet<SchedulerResourceTypes> getSchedulingResourceTypes() {
    return EnumSet.of(SchedulerResourceTypes.MEMORY);
  }

  @Override
  public Set<String> getPlanQueues() throws YarnException {
    throw new YarnException(getClass().getSimpleName()
        + " does not support reservations");
  }

  /**
   * By default placement constraint is disabled. Schedulers which support
   * placement constraint can override this value.
   * @return enabled or not
   */
  public boolean placementConstraintEnabled() {
    return false;
  }

  protected void refreshMaximumAllocation(Resource newMaxAlloc) {
    nodeTracker.setConfiguredMaxAllocation(newMaxAlloc);
  }

  @Override
  public List<ResourceRequest> getPendingResourceRequestsForAttempt(
      ApplicationAttemptId attemptId) {
    SchedulerApplicationAttempt attempt = getApplicationAttempt(attemptId);
    if (attempt != null) {
      return attempt.getAppSchedulingInfo().getAllResourceRequests();
    }
    return null;
  }

  @Override
  public List<SchedulingRequest> getPendingSchedulingRequestsForAttempt(
      ApplicationAttemptId attemptId) {
    SchedulerApplicationAttempt attempt = getApplicationAttempt(attemptId);
    if (attempt != null) {
      return attempt.getAppSchedulingInfo().getAllSchedulingRequests();
    }
    return null;
  }

  @Override
  public Priority checkAndGetApplicationPriority(
          Priority priorityRequestedByApp, UserGroupInformation user,
          String queuePath, ApplicationId applicationId) throws YarnException {
    // Dummy Implementation till Application Priority changes are done in
    // specific scheduler.
    return Priority.newInstance(0);
  }

  @Override
  public Priority updateApplicationPriority(Priority newPriority,
      ApplicationId applicationId, SettableFuture<Object> future,
      UserGroupInformation user)
      throws YarnException {
    // Dummy Implementation till Application Priority changes are done in
    // specific scheduler.
    return Priority.newInstance(0);
  }

  @Override
  public Priority getMaxClusterLevelAppPriority() {
    return maxClusterLevelAppPriority;
  }

  private Priority getMaxPriorityFromConf(Configuration conf) {
    return Priority.newInstance(conf.getInt(
        YarnConfiguration.MAX_CLUSTER_LEVEL_APPLICATION_PRIORITY,
        YarnConfiguration.DEFAULT_CLUSTER_LEVEL_APPLICATION_PRIORITY));
  }

  @Override
  public void setClusterMaxPriority(Configuration conf)
      throws YarnException {
    try {
      maxClusterLevelAppPriority = getMaxPriorityFromConf(conf);
    } catch (NumberFormatException e) {
      throw new YarnException(e);
    }
    LOG.info("Updated the cluste max priority to maxClusterLevelAppPriority = "
        + maxClusterLevelAppPriority);
  }

  /**
   * Sanity check increase/decrease request, and return
   * SchedulerContainerResourceChangeRequest according to given
   * UpdateContainerRequest.
   *
   * <pre>
   * - Returns non-null value means validation succeeded
   * - Throw exception when any other error happens
   * </pre>
   */
  private SchedContainerChangeRequest createSchedContainerChangeRequest(
      UpdateContainerRequest request, boolean increase)
      throws YarnException {
    ContainerId containerId = request.getContainerId();
    RMContainer rmContainer = getRMContainer(containerId);
    if (null == rmContainer) {
      String msg =
          "Failed to get rmContainer for "
              + (increase ? "increase" : "decrease")
              + " request, with container-id=" + containerId;
      throw new InvalidResourceRequestException(msg);
    }
    SchedulerNode schedulerNode =
        getSchedulerNode(rmContainer.getAllocatedNode());
    return new SchedContainerChangeRequest(
        this.rmContext, schedulerNode, rmContainer, request.getCapability());
  }

  protected List<SchedContainerChangeRequest>
      createSchedContainerChangeRequests(
          List<UpdateContainerRequest> changeRequests,
          boolean increase) {
    List<SchedContainerChangeRequest> schedulerChangeRequests =
        new ArrayList<SchedContainerChangeRequest>();
    for (UpdateContainerRequest r : changeRequests) {
      SchedContainerChangeRequest sr = null;
      try {
        sr = createSchedContainerChangeRequest(r, increase);
      } catch (YarnException e) {
        LOG.warn("Error happens when checking increase request, Ignoring.."
            + " exception=", e);
        continue;
      }
      schedulerChangeRequests.add(sr);
    }
    return schedulerChangeRequests;
  }

  public ActivitiesManager getActivitiesManager() {
    return this.activitiesManager;
  }

  public Clock getClock() {
    return clock;
  }

  @VisibleForTesting
  public void setClock(Clock clock) {
    this.clock = clock;
  }

  @Lock(Lock.NoLock.class)
  public SchedulerNode getNode(NodeId nodeId) {
    return nodeTracker.getNode(nodeId);
  }

  /**
   * Get lists of new containers from NodeManager and process them.
   * @param nm The RMNode corresponding to the NodeManager
   * @param schedulerNode schedulerNode
   * @return list of completed containers
   */
  private List<ContainerStatus> updateNewContainerInfo(RMNode nm,
      SchedulerNode schedulerNode) {
    List<UpdatedContainerInfo> containerInfoList = nm.pullContainerUpdates();
    List<ContainerStatus> newlyLaunchedContainers =
        new ArrayList<>();
    List<ContainerStatus> completedContainers =
        new ArrayList<>();
    List<Map.Entry<ApplicationId, ContainerStatus>> updateExistContainers =
        new ArrayList<>();

    for(UpdatedContainerInfo containerInfo : containerInfoList) {
      newlyLaunchedContainers
          .addAll(containerInfo.getNewlyLaunchedContainers());
      completedContainers.addAll(containerInfo.getCompletedContainers());
      updateExistContainers.addAll(containerInfo.getUpdateContainers());
    }

    // Processing the newly launched containers
    for (ContainerStatus launchedContainer : newlyLaunchedContainers) {
      containerLaunchedOnNode(launchedContainer.getContainerId(),
          schedulerNode);
    }

    // Processing the newly increased containers
    List<Container> newlyIncreasedContainers =
        nm.pullNewlyIncreasedContainers();
    for (Container container : newlyIncreasedContainers) {
      containerIncreasedOnNode(container.getId(), schedulerNode, container);
    }

    // Processing the update exist containers
    for (Map.Entry<ApplicationId, ContainerStatus> c : updateExistContainers) {
      SchedulerApplication<T> app = applications.get(c.getKey());
      ContainerId containerId = c.getValue().getContainerId();
      if (app == null || app.getCurrentAppAttempt() == null) {
        continue;
      }
      RMContainer rmContainer
          = app.getCurrentAppAttempt().getRMContainer(containerId);
      if (rmContainer == null) {
        continue;
      }
      // exposed ports are already set for the container, skip
      if (rmContainer.getExposedPorts() != null &&
          rmContainer.getExposedPorts().size() > 0) {
        continue;
      }

      String strExposedPorts = c.getValue().getExposedPorts();
      if (null != strExposedPorts && !strExposedPorts.isEmpty()) {
        Gson gson = new Gson();
        Map<String, List<Map<String, String>>> exposedPorts =
            gson.fromJson(strExposedPorts,
            new TypeToken<Map<String, List<Map<String, String>>>>()
                {}.getType());
        LOG.info("update exist container " + containerId.getContainerId()
            + ", strExposedPorts = " + strExposedPorts);
        rmContainer.setExposedPorts(exposedPorts);
      }
    }

    return completedContainers;
  }

  /**
   * Process completed container list.
   * @param completedContainers Extracted list of completed containers
   * @param releasedResources Reference resource object for completed containers
   * @param nodeId NodeId corresponding to the NodeManager
   * @param schedulerNode schedulerNode
   * @return The total number of released containers
   */
  private int updateCompletedContainers(List<ContainerStatus> completedContainers,
      Resource releasedResources, NodeId nodeId, SchedulerNode schedulerNode) {
    int releasedContainers = 0;
    List<ContainerId> untrackedContainerIdList = new ArrayList<ContainerId>();
    for (ContainerStatus completedContainer : completedContainers) {
      ContainerId containerId = completedContainer.getContainerId();
      LOG.debug("Container FINISHED: {}", containerId);
      RMContainer container = getRMContainer(containerId);
      completedContainer(container,
          completedContainer, RMContainerEventType.FINISHED);
      if (schedulerNode != null) {
        schedulerNode.releaseContainer(containerId, true);
      }

      if (container != null) {
        releasedContainers++;
        Resource ars = container.getAllocatedResource();
        if (ars != null) {
          Resources.addTo(releasedResources, ars);
        }
        Resource rrs = container.getReservedResource();
        if (rrs != null) {
          Resources.addTo(releasedResources, rrs);
        }
      } else {
        // Add containers which are untracked by RM.
        untrackedContainerIdList.add(containerId);
      }
    }

    // Acknowledge NM to remove RM-untracked-containers from NM context.
    if (!untrackedContainerIdList.isEmpty()) {
      this.rmContext.getDispatcher().getEventHandler()
          .handle(new RMNodeFinishedContainersPulledByAMEvent(nodeId,
              untrackedContainerIdList));
    }

    return releasedContainers;
  }

  /**
   * Update schedulerHealth information.
   * @param releasedResources Reference resource object for completed containers
   * @param releasedContainers Count of released containers
   */
  protected void updateSchedulerHealthInformation(Resource releasedResources,
      int releasedContainers) {

    schedulerHealth.updateSchedulerReleaseDetails(getLastNodeUpdateTime(),
        releasedResources);
    schedulerHealth.updateSchedulerReleaseCounts(releasedContainers);
  }

  /**
   * Update container and utilization information on the NodeManager.
   * @param nm The NodeManager to update
   * @param schedulerNode schedulerNode
   */
  protected void updateNodeResourceUtilization(RMNode nm,
      SchedulerNode schedulerNode) {
    // Updating node resource utilization
    schedulerNode.setAggregatedContainersUtilization(
        nm.getAggregatedContainersUtilization());
    schedulerNode.setNodeUtilization(nm.getNodeUtilization());
  }

  /**
   * Process a heartbeat update from a node.
   * @param nm The RMNode corresponding to the NodeManager
   */
  protected void nodeUpdate(RMNode nm) {
    LOG.debug("nodeUpdate: {} cluster capacity: {}",
        nm, getClusterResource());

    // Process new container information
    // NOTICE: it is possible to not find the NodeID as a node can be
    // decommissioned at the same time. Skip updates if node is null.
    //获取调度器中的对应节点信息
    SchedulerNode schedulerNode = getNode(nm.getNodeID());
    //更新新容器的信息，返回已完成的容器状态
    List<ContainerStatus> completedContainers = updateNewContainerInfo(nm,
        schedulerNode);

    // Notify Scheduler Node updated.
    if (schedulerNode != null) {
      schedulerNode.notifyNodeUpdate();
    }

    // Process completed containers
    Resource releasedResources = Resource.newInstance(0, 0);
    int releasedContainers = updateCompletedContainers(completedContainers,
        releasedResources, nm.getNodeID(), schedulerNode);

    // If the node is decommissioning, send an update to have the total
    // resource equal to the used resource, so no available resource to
    // schedule.
    if (nm.getState() == NodeState.DECOMMISSIONING && schedulerNode != null
        && schedulerNode.getTotalResource().compareTo(
            schedulerNode.getAllocatedResource()) != 0) {
      this.rmContext
          .getDispatcher()
          .getEventHandler()
          .handle(
              new RMNodeResourceUpdateEvent(nm.getNodeID(), ResourceOption
                  .newInstance(schedulerNode.getAllocatedResource(), 0)));
    }

    updateSchedulerHealthInformation(releasedResources, releasedContainers);
    if (schedulerNode != null) {
      updateNodeResourceUtilization(nm, schedulerNode);
    }

    if (schedulerNode != null) {
      signalContainersIfOvercommitted(schedulerNode, true);
    }

    // Now node data structures are up-to-date and ready for scheduling.
    if(LOG.isDebugEnabled()) {
      LOG.debug(
          "Node being looked for scheduling " + nm + " availableResource: " +
              (schedulerNode == null ? "unknown (decommissioned)" :
                  schedulerNode.getUnallocatedResource()));
    }
  }

  /**
   * Check if the node is overcommitted and needs to remove containers. If
   * it is overcommitted, it will kill or preempt (notify the AM to stop them)
   * containers. It also takes into account the overcommit timeout. It only
   * notifies the application to preempt a container if the timeout hasn't
   * passed. If the timeout has passed, it tries to kill the containers. If
   * there is no timeout, it doesn't do anything and just prevents new
   * allocations.
   *
   * This action is taken when the change of resources happens (to preempt
   * containers or killing them if specified) or when the node heart beats
   * (for killing only).
   *
   * @param schedulerNode The node to check whether is overcommitted.
   * @param kill If the container should be killed or just notify the AM.
   */
  private void signalContainersIfOvercommitted(
      SchedulerNode schedulerNode, boolean kill) {

    // If there is no time out, we don't do anything
    if (!schedulerNode.isOvercommitTimeOutSet()) {
      return;
    }

    SchedulerEventType eventType =
        SchedulerEventType.MARK_CONTAINER_FOR_PREEMPTION;
    if (kill) {
      eventType = SchedulerEventType.MARK_CONTAINER_FOR_KILLABLE;

      // If it hasn't timed out yet, don't kill
      if (!schedulerNode.isOvercommitTimedOut()) {
        return;
      }
    }

    // Check if the node is overcommitted (negative resources)
    ResourceCalculator rc = getResourceCalculator();
    Resource unallocated = Resource.newInstance(
        schedulerNode.getUnallocatedResource());
    if (Resources.fitsIn(rc, ZERO_RESOURCE, unallocated)) {
      return;
    }

    LOG.info("{} is overcommitted ({}), preempt/kill containers",
        schedulerNode.getNodeID(), unallocated);
    for (RMContainer container : schedulerNode.getContainersToKill()) {
      LOG.info("Send {} to {} to free up {}", eventType,
          container.getContainerId(), container.getAllocatedResource());
      ApplicationAttemptId appId = container.getApplicationAttemptId();
      ContainerPreemptEvent event =
          new ContainerPreemptEvent(appId, container, eventType);
      this.rmContext.getDispatcher().getEventHandler().handle(event);
      Resources.addTo(unallocated, container.getAllocatedResource());

      if (Resources.fitsIn(rc, ZERO_RESOURCE, unallocated)) {
        LOG.debug("Enough unallocated resources {}", unallocated);
        break;
      }
    }
  }

  @Override
  public Resource getNormalizedResource(Resource requestedResource,
                                        Resource maxResourceCapability) {
    return SchedulerUtils.getNormalizedResource(requestedResource,
        getResourceCalculator(),
        getMinimumResourceCapability(),
        maxResourceCapability,
        getMinimumResourceCapability());
  }

  /**
   * Normalize a list of resource requests.
   * 规范化资源请求
   * @param asks resource requests
   */
  protected void normalizeResourceRequests(List<ResourceRequest> asks) {
    normalizeResourceRequests(asks, null);
  }

  /**
   * Normalize a list of resource requests
   * using queue maximum resource allocations.
   * @param asks resource requests
   * @param queueName queue Name.
   */
  protected void normalizeResourceRequests(List<ResourceRequest> asks,
      String queueName) {
    Resource maxAllocation = getMaximumResourceCapability(queueName);
    for (ResourceRequest ask : asks) {
      ask.setCapability(
          getNormalizedResource(ask.getCapability(), maxAllocation));
    }
  }
  //处理应用程序提交的容器更新请求，这些更新包括：
  //提升（Promotion）：从 0 资源增加到目标资源（相当于创建新容器）。
  //资源增加（Increase）：已有容器的资源增加。
  //降级（Demotion）：从原始资源减少到 0（相当于释放容器）。
  //资源减少（Decrease）：已有容器的资源减少。
  //该方法根据更新请求的类型，调用不同的方法进行处理。

  protected void handleContainerUpdates(
      SchedulerApplicationAttempt appAttempt, ContainerUpdates updates) {
    //处理提升（Promotion）请求
    List<UpdateContainerRequest> promotionRequests =
        updates.getPromotionRequests();
    if (promotionRequests != null && !promotionRequests.isEmpty()) {
      LOG.info("Promotion Update requests : " + promotionRequests);
      // Promotion is technically an increase request from
      // 0 resources to target resources.
      handleIncreaseRequests(appAttempt, promotionRequests);
    }
    //处理资源增加（Increase）请求
    List<UpdateContainerRequest> increaseRequests =
        updates.getIncreaseRequests();
    if (increaseRequests != null && !increaseRequests.isEmpty()) {
      LOG.info("Resource increase requests : " + increaseRequests);
      handleIncreaseRequests(appAttempt, increaseRequests);
    }
    //处理降级（Demotion）请求
    List<UpdateContainerRequest> demotionRequests =
        updates.getDemotionRequests();
    if (demotionRequests != null && !demotionRequests.isEmpty()) {
      LOG.info("Demotion Update requests : " + demotionRequests);
      // Demotion is technically a decrease request from initial
      // to 0 resources
      handleDecreaseRequests(appAttempt, demotionRequests);
    }
    //处理资源减少（Decrease）请求
    List<UpdateContainerRequest> decreaseRequests =
        updates.getDecreaseRequests();
    if (decreaseRequests != null && !decreaseRequests.isEmpty()) {
      LOG.info("Resource decrease requests : " + decreaseRequests);
      handleDecreaseRequests(appAttempt, decreaseRequests);
    }
  }
  //处理 资源增加（Increase）请求，即应用程序希望某些正在运行的容器（RMContainer）增加 CPU、内存等资源
  private void handleIncreaseRequests(
      SchedulerApplicationAttempt applicationAttempt,
      List<UpdateContainerRequest> updateContainerRequests) {
    //遍历所有 资源增加请求
    for (UpdateContainerRequest uReq : updateContainerRequests) {
      //获取 对应的容器对象
      RMContainer rmContainer =
          rmContext.getScheduler().getRMContainer(uReq.getContainerId());
      // Check if this is a container update
      // And not in the middle of a Demotion
      if (rmContainer != null) {
        // Check if this is an executionType change request
        // If so, fix the rr to make it look like a normal rr
        // with relaxLocality=false and numContainers=1
        //获取该容器所在的 调度节点（SchedulerNode）
        SchedulerNode schedulerNode = rmContext.getScheduler()
            .getSchedulerNode(rmContainer.getContainer().getNodeId());

        // Add only if no outstanding promote requests exist.
        //检查是否已经有未完成的资源增加请求
        if (!applicationAttempt.getUpdateContext()
            .checkAndAddToOutstandingIncreases(
                rmContainer, schedulerNode, uReq)) { //如果 已经有一个增加请求，则不允许重复请求，返回 false，否则，将该请求加入待处理的增加请求列表，返回 true
          applicationAttempt.addToUpdateContainerErrors(
              UpdateContainerError.newInstance(
              RMServerUtils.UPDATE_OUTSTANDING_ERROR, uReq));
        }
      } else {
        LOG.warn("Cannot promote non-existent (or completed) Container ["
            + uReq.getContainerId() + "]");
      }
    }
  }

  private void handleDecreaseRequests(SchedulerApplicationAttempt appAttempt,
      List<UpdateContainerRequest> demotionRequests) {
    OpportunisticContainerContext oppCntxt =
        appAttempt.getOpportunisticContainerContext();
    for (UpdateContainerRequest uReq : demotionRequests) {
      RMContainer rmContainer =
          rmContext.getScheduler().getRMContainer(uReq.getContainerId());
      if (rmContainer != null) {
        SchedulerNode schedulerNode = rmContext.getScheduler()
            .getSchedulerNode(rmContainer.getContainer().getNodeId());
        if (appAttempt.getUpdateContext()
            .checkAndAddToOutstandingDecreases(uReq, schedulerNode,
                rmContainer.getContainer())) {
          if (ContainerUpdateType.DEMOTE_EXECUTION_TYPE ==
              uReq.getContainerUpdateType()) {
            RMContainer demotedRMContainer =
                createDemotedRMContainer(appAttempt, oppCntxt, rmContainer);
            if (demotedRMContainer != null) {
              OpportunisticSchedulerMetrics.getMetrics()
                  .incrAllocatedOppContainers(1);
              appAttempt.addToNewlyDemotedContainers(
                      uReq.getContainerId(), demotedRMContainer);
            }
          } else {
            RMContainer demotedRMContainer = createDecreasedRMContainer(
                appAttempt, uReq, rmContainer);
            appAttempt.addToNewlyDecreasedContainers(
                uReq.getContainerId(), demotedRMContainer);
          }
        } else {
          appAttempt.addToUpdateContainerErrors(
              UpdateContainerError.newInstance(
              RMServerUtils.UPDATE_OUTSTANDING_ERROR, uReq));
        }
      } else {
        LOG.warn("Cannot demote/decrease non-existent (or completed) " +
            "Container [" + uReq.getContainerId() + "]");
      }
    }
  }

  private RMContainer createDecreasedRMContainer(
      SchedulerApplicationAttempt appAttempt, UpdateContainerRequest uReq,
      RMContainer rmContainer) {
    SchedulerRequestKey sk =
        SchedulerRequestKey.extractFrom(rmContainer.getContainer());
    Container decreasedContainer = BuilderUtils.newContainer(
        ContainerId.newContainerId(appAttempt.getApplicationAttemptId(),
            appAttempt.getNewContainerId()),
        rmContainer.getContainer().getNodeId(),
        rmContainer.getContainer().getNodeHttpAddress(),
        Resources.none(),
        sk.getPriority(), null, rmContainer.getExecutionType(),
        sk.getAllocationRequestId());
    decreasedContainer.setVersion(rmContainer.getContainer().getVersion());
    RMContainer newRmContainer = new RMContainerImpl(decreasedContainer,
        sk, appAttempt.getApplicationAttemptId(),
        decreasedContainer.getNodeId(), appAttempt.getUser(), rmContext,
        rmContainer.isRemotelyAllocated());
    appAttempt.addRMContainer(decreasedContainer.getId(), rmContainer);
    ((AbstractYarnScheduler) rmContext.getScheduler()).getNode(
        decreasedContainer.getNodeId()).allocateContainer(newRmContainer);
    return newRmContainer;
  }

  private RMContainer createDemotedRMContainer(
      SchedulerApplicationAttempt appAttempt,
      OpportunisticContainerContext oppCntxt,
      RMContainer rmContainer) {
    SchedulerRequestKey sk =
        SchedulerRequestKey.extractFrom(rmContainer.getContainer());
    Container demotedContainer = BuilderUtils.newContainer(
        ContainerId.newContainerId(appAttempt.getApplicationAttemptId(),
            oppCntxt.getContainerIdGenerator().generateContainerId()),
        rmContainer.getContainer().getNodeId(),
        rmContainer.getContainer().getNodeHttpAddress(),
        rmContainer.getContainer().getResource(),
        sk.getPriority(), null, ExecutionType.OPPORTUNISTIC,
        sk.getAllocationRequestId());
    demotedContainer.setVersion(rmContainer.getContainer().getVersion());
    return SchedulerUtils.createOpportunisticRmContainer(
        rmContext, demotedContainer, false);
  }

  /**
   * Rollback container update after expiry.
   * @param containerId ContainerId.
   */
  protected void rollbackContainerUpdate(
      ContainerId containerId) {
    RMContainer rmContainer = getRMContainer(containerId);
    if (rmContainer == null) {
      LOG.info("Cannot rollback resource for container " + containerId
          + ". The container does not exist.");
      return;
    }
    T app = getCurrentAttemptForContainer(containerId);
    if (getCurrentAttemptForContainer(containerId) == null) {
      LOG.info("Cannot rollback resource for container " + containerId
          + ". The application that the container "
          + "belongs to does not exist.");
      return;
    }

    if (Resources.fitsIn(rmContainer.getLastConfirmedResource(),
        rmContainer.getContainer().getResource())) {
      LOG.info("Roll back resource for container " + containerId);
      handleDecreaseRequests(app, Arrays.asList(
          UpdateContainerRequest.newInstance(
              rmContainer.getContainer().getVersion(),
              rmContainer.getContainerId(),
              ContainerUpdateType.DECREASE_RESOURCE,
              rmContainer.getLastConfirmedResource(), null)));
    }
  }

  @Override
  public List<NodeId> getNodeIds(String resourceName) {
    return nodeTracker.getNodeIdsByResourceName(resourceName);
  }

  /**
   * To be used to release a container via a Scheduler Event rather than
   * in the same thread.
   * @param container Container.
   */
  public void asyncContainerRelease(RMContainer container) {
    this.rmContext.getDispatcher().getEventHandler().handle(
        new ReleaseContainerEvent(container));
  }

  /*
   * Get a Resource object with for the minimum allocation possible.
   *
   * @return a Resource object with the minimum allocation for the scheduler
   */
  //获取容器的最小分配值
  public Resource getMinimumAllocation() {
    Resource ret = ResourceUtils.getResourceTypesMinimumAllocation();
    LOG.info("Minimum allocation = " + ret);
    return ret;
  }

  /**
   * Get a Resource object with for the maximum allocation possible.
   *
   * @return a Resource object with the maximum allocation for the scheduler
   */
  //获取容器的最大分配值
  public Resource getMaximumAllocation() {
    Resource ret = ResourceUtils.getResourceTypesMaximumAllocation();
    LOG.info("Maximum allocation = " + ret);
    return ret;
  }

  @Override
  public long checkAndGetApplicationLifetime(String queueName, long lifetime) {
    // Lifetime is the application lifetime by default.
    return lifetime;
  }

  @Override
  public long getMaximumApplicationLifetime(String queueName) {
    return -1;
  }

  /**
   * Kill a RMContainer. This is meant to be called in tests only to simulate
   * AM container failures.
   * @param container the container to kill
   */
  @VisibleForTesting
  public abstract void killContainer(RMContainer container);

  /**
   * Update internal state of the scheduler.  This can be useful for scheduler
   * implementations that maintain some state that needs to be periodically
   * updated; for example, metrics or queue resources.  It will be called by the
   * {@link UpdateThread} every {@link #updateInterval}.  By default, it will
   * not run; subclasses should set {@link #updateInterval} to a
   * positive value during {@link #serviceInit(Configuration)} if they want to
   * enable the thread.
   */
  @VisibleForTesting
  public void update() {
    // do nothing by default
  }

  /**
   * Thread which calls {@link #update()} every
   * <code>updateInterval</code> milliseconds.
   */
  private class UpdateThread extends Thread {
    @Override
    public void run() {
      while (!Thread.currentThread().isInterrupted()) {
        try {
          synchronized (updateThreadMonitor) {
            updateThreadMonitor.wait(updateInterval);
          }
          update();
        } catch (InterruptedException ie) {
          LOG.warn("Scheduler UpdateThread interrupted. Exiting.");
          return;
        } catch (Exception e) {
          LOG.error("Exception in scheduler UpdateThread", e);
        }
      }
    }
  }

  /**
   * Allows {@link UpdateThread} to start processing without waiting till
   * {@link #updateInterval}.
   */
  protected void triggerUpdate() {
    synchronized (updateThreadMonitor) {
      updateThreadMonitor.notify();
    }
  }

  @Override
  public void reinitialize(Configuration conf, RMContext rmContext)
      throws IOException {
    try {
      LOG.info("Reinitializing SchedulingMonitorManager ...");
      schedulingMonitorManager.reinitialize(rmContext, conf);
    } catch (YarnException e) {
      throw new IOException(e);
    }
  }

  /**
   * Default implementation. Always returns false.
   * @param appAttempt ApplicationAttempt.
   * @param schedulingRequest SchedulingRequest.
   * @param schedulerNode SchedulerNode.
   * @return Success or not.
   */
  @Override
  public boolean attemptAllocationOnNode(SchedulerApplicationAttempt appAttempt,
      SchedulingRequest schedulingRequest, SchedulerNode schedulerNode) {
    return false;
  }

  @Override
  public void resetSchedulerMetrics() {
    // reset scheduler metrics
  }

  /**
   * Gets the apps from a given queue.
   *
   * Mechanics:
   * 1. Get all {@link ApplicationAttemptId}s in the given queue by
   * {@link #getAppsInQueue(String)} method.
   * 2. Always need to check validity for the given queue by the returned
   * values.
   *
   * @param queueName queue name
   * @return a collection of app attempt ids in the given queue, it maybe empty.
   * @throws YarnException if {@link #getAppsInQueue(String)} return null, will
   * throw this exception.
   */
  private List<ApplicationAttemptId> getAppsFromQueue(String queueName)
      throws YarnException {
    List<ApplicationAttemptId> apps = getAppsInQueue(queueName);
    if (apps == null) {
      throw new YarnException("The specified queue: " + queueName
          + " doesn't exist");
    }
    return apps;
  }
}
