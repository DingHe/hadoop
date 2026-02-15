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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ActivitiesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppActivitiesInfo;
import org.apache.hadoop.yarn.util.SystemClock;

import org.apache.hadoop.classification.VisibleForTesting;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.*;
import java.util.stream.Collectors;

/**
 * A class to store node or application allocations.
 * It mainly contains operations for allocation start, add, update and finish.
 */
//主要用于存储和管理 YARN 资源管理器（ResourceManager）在调度过程中产生的节点或应用程序的资源分配活动。它负责：
//记录 资源分配过程中的详细信息，包括节点资源分配 (NodeAllocation) 和应用资源分配 (AppAllocation)。
//提供查询接口，让外部组件（如 Web UI）可以获取调度活动的历史信息。
//清理历史记录，防止资源占用过多。
public class ActivitiesManager extends AbstractService {
  private static final Logger LOG =
      LoggerFactory.getLogger(ActivitiesManager.class);
  // An empty node ID, we use this variable as a placeholder
  // in the activity records when recording multiple nodes assignments.
  //用于表示多节点分配时的空 ID
  public static final NodeId EMPTY_NODE_ID = NodeId.newInstance("", 0);
  //诊断信息的分隔符（换行符 \n）
  public static final char DIAGNOSTICS_DETAILS_SEPARATOR = '\n';
  //空字符串，用作默认诊断信息
  public static final String EMPTY_DIAGNOSTICS = "";
  //线程本地变量，存储当前线程正在记录的节点资源分配情况
  private ThreadLocal<Map<NodeId, List<NodeAllocation>>>
      recordingNodesAllocation;
  //存储已完成的节点资源分配记录
  @VisibleForTesting
  ConcurrentMap<NodeId, List<NodeAllocation>> completedNodeAllocations;
  //当前活跃的节点集合，表示哪些节点正在记录资源分配信息
  private Set<NodeId> activeRecordedNodes;
  //记录哪些应用的资源分配活动应持续记录到特定时间
  private ConcurrentMap<ApplicationId, Long>
      recordingAppActivitiesUntilSpecifiedTime;
  //线程本地变量，存储当前线程正在记录的应用程序资源分配情况
  private ThreadLocal<Map<ApplicationId, AppAllocation>>
      appsAllocation;
  //存储已完成的应用程序资源分配记录
  @VisibleForTesting
  ConcurrentMap<ApplicationId, Queue<AppAllocation>> completedAppAllocations;
  //记录活动日志的计数器，用于管理日志的数量
  private AtomicInteger recordCount = new AtomicInteger(0);
  //最后一次可用的节点分配活动记录，用于获取最近的调度活动
  private List<NodeAllocation> lastAvailableNodeActivities = null;
  //后台清理线程，负责定期清理旧的资源分配记录
  private Thread cleanUpThread;
  //清理间隔时间（毫秒）
  private long activitiesCleanupIntervalMs;
  //调度器活动的生存时间（TTL）
  private long schedulerActivitiesTTL;
  //应用程序活动的生存时间（TTL）
  private long appActivitiesTTL;
  //应用程序活动的最大队列长度，控制存储的历史记录条数
  private volatile int appActivitiesMaxQueueLength;
  //从 YARN 配置中读取的最大队列长度
  private int configuredAppActivitiesMaxQueueLength;
  private final RMContext rmContext;
  private volatile boolean stopped;
  //线程本地变量，用于管理调度诊断信息的收集
  private ThreadLocal<DiagnosticsCollectorManager> diagnosticCollectorManager;
  //存储最近的 N 次调度活动记录，便于快速查询最新的调度历史
  private volatile ConcurrentLinkedDeque<Pair<NodeId, List<NodeAllocation>>>
      lastNActivities;

  public ActivitiesManager(RMContext rmContext) {
    super(ActivitiesManager.class.getName());
    recordingNodesAllocation = ThreadLocal.withInitial(() -> new HashMap());
    completedNodeAllocations = new ConcurrentHashMap<>();
    appsAllocation = ThreadLocal.withInitial(() -> new HashMap());
    completedAppAllocations = new ConcurrentHashMap<>();
    activeRecordedNodes = Collections.newSetFromMap(new ConcurrentHashMap<>());
    recordingAppActivitiesUntilSpecifiedTime = new ConcurrentHashMap<>();
    diagnosticCollectorManager = ThreadLocal.withInitial(
        () -> new DiagnosticsCollectorManager(
            new GenericDiagnosticsCollector()));
    this.rmContext = rmContext;
    if (rmContext.getYarnConfiguration() != null) {
      setupConfForCleanup(rmContext.getYarnConfiguration());
    }
    lastNActivities = new ConcurrentLinkedDeque<>();
  }
  //设置清理配置
  private void setupConfForCleanup(Configuration conf) {
    activitiesCleanupIntervalMs = conf.getLong(
        YarnConfiguration.RM_ACTIVITIES_MANAGER_CLEANUP_INTERVAL_MS,
        YarnConfiguration.
            DEFAULT_RM_ACTIVITIES_MANAGER_CLEANUP_INTERVAL_MS);
    schedulerActivitiesTTL = conf.getLong(
        YarnConfiguration.RM_ACTIVITIES_MANAGER_SCHEDULER_ACTIVITIES_TTL_MS,
        YarnConfiguration.
            DEFAULT_RM_ACTIVITIES_MANAGER_SCHEDULER_ACTIVITIES_TTL_MS);
    appActivitiesTTL = conf.getLong(
        YarnConfiguration.RM_ACTIVITIES_MANAGER_APP_ACTIVITIES_TTL_MS,
        YarnConfiguration.
            DEFAULT_RM_ACTIVITIES_MANAGER_APP_ACTIVITIES_TTL_MS);
    configuredAppActivitiesMaxQueueLength = conf.getInt(YarnConfiguration.
            RM_ACTIVITIES_MANAGER_APP_ACTIVITIES_MAX_QUEUE_LENGTH,
        YarnConfiguration.
            DEFAULT_RM_ACTIVITIES_MANAGER_APP_ACTIVITIES_MAX_QUEUE_LENGTH);
    appActivitiesMaxQueueLength = configuredAppActivitiesMaxQueueLength;
  }
  //获取指定 ApplicationId 对应的应用调度活动信息，包括资源分配尝试的详细情况。支持按优先级、请求 ID 进行筛选，并可对结果进行汇总（summarize）
  public AppActivitiesInfo getAppActivitiesInfo(ApplicationId applicationId, //要查询的应用 ID
      Set<Integer> requestPriorities, //需要过滤的请求优先级集合
      Set<Long> allocationRequestIds,  //需要过滤的资源分配请求 ID 集合
      RMWSConsts.ActivitiesGroupBy groupBy, //结果分组方式
      int limit, //限制返回的记录数量
      boolean summarize, //是否对记录进行汇总
      double maxTimeInSeconds) { //汇总时的时间窗口
    //通过 applicationId 从 rmContext 获取 RMApp（资源管理器中的应用）
    //如果应用存在且仍处于运行状态（即 FinalApplicationStatus.UNDEFINED），继续执行，否则返回失败信息
    RMApp app = rmContext.getRMApps().get(applicationId);
    if (app != null && app.getFinalApplicationStatus()
        == FinalApplicationStatus.UNDEFINED) {
      //获取应用的已完成分配记录
      Queue<AppAllocation> curAllocations =
          completedAppAllocations.get(applicationId);
      List<AppAllocation> allocations = null;
      if (curAllocations != null) {
        //筛选符合条件的分配记录
        if (CollectionUtils.isNotEmpty(requestPriorities) || CollectionUtils
            .isNotEmpty(allocationRequestIds)) {
          allocations = curAllocations.stream().map(e -> e
              .filterAllocationAttempts(requestPriorities,
                  allocationRequestIds))
              .filter(e -> !e.getAllocationAttempts().isEmpty())
              .collect(Collectors.toList());
        } else {
          allocations = new ArrayList(curAllocations);
        }
      }
      //对分配记录进行汇总
      if (summarize && allocations != null) {
        AppAllocation summaryAppAllocation =
            getSummarizedAppAllocation(allocations, maxTimeInSeconds);
        if (summaryAppAllocation != null) {
          allocations = Lists.newArrayList(summaryAppAllocation);
        }
      }
      if (allocations != null && limit > 0 && limit < allocations.size()) {
        allocations =
            allocations.subList(allocations.size() - limit, allocations.size());
      }
      return new AppActivitiesInfo(allocations, applicationId, groupBy);
    } else {
      return new AppActivitiesInfo(
          "fail to get application activities after finished",
          applicationId.toString());
    }
  }

  /**
   * Get summarized app allocation from multiple allocations as follows:
   * 1. Collect latest allocation attempts on nodes to construct an allocation
   *    summary on nodes from multiple app allocations which are recorded a few
   *    seconds before the last allocation.
   * 2. Copy other fields from the last allocation.
   */
  //作用是从多个 AppAllocation 对象中提取最新的分配尝试（allocation attempts），
  // 并生成一个总结后的 AppAllocation 对象
  private AppAllocation getSummarizedAppAllocation(
      List<AppAllocation> allocations, //一组应用的分配记录，按时间顺序存储（时间越大的索引越靠后）
      double maxTimeInSeconds) { //时间窗口（秒），只保留该时间段内的 allocationAttempts
    if (allocations == null || allocations.isEmpty()) {
      return null;
    }
    //只保留 startTime 之后的调度尝试
    long startTime = allocations.get(allocations.size() - 1).getTime()
        - (long) (maxTimeInSeconds * 1000);
    Map<String, ActivityNode> nodeActivities = new HashMap<>();
    for (int i = allocations.size() - 1; i >= 0; i--) {
      //如果当前 AppAllocation 早于 startTime，停止遍历，避免不必要的数据处理
      AppAllocation appAllocation = allocations.get(i);
      if (startTime > appAllocation.getTime()) {
        break;
      }
      //提取当前 AppAllocation 的 allocationAttempts（表示当前分配尝试
      List<ActivityNode> activityNodes = appAllocation.getAllocationAttempts();
      for (ActivityNode an : activityNodes) {
        //唯一键requestPriority + allocationRequestId + nodeId 作为 key，确保相同的 ActivityNode 只存储最新的
        nodeActivities.putIfAbsent(
            an.getRequestPriority() + "_" + an.getAllocationRequestId() + "_"
                + an.getNodeId(), an);
      }
    }
    //最新的 AppAllocation 记录（allocations 最后一条数据）
    AppAllocation lastAppAllocation = allocations.get(allocations.size() - 1);
    AppAllocation summarizedAppAllocation =
        new AppAllocation(lastAppAllocation.getPriority(), null,
            lastAppAllocation.getQueueName());
    summarizedAppAllocation.updateAppContainerStateAndTime(null,
        lastAppAllocation.getActivityState(), lastAppAllocation.getTime(),
        lastAppAllocation.getDiagnostic());
    //只包含最新的调度尝试
    summarizedAppAllocation
        .setAllocationAttempts(new ArrayList<>(nodeActivities.values()));
    return summarizedAppAllocation;
  }
  //用于获取指定节点的活动信息 (ActivitiesInfo)，以便用于资源管理、调度分析和 UI 展示。
  //如果 nodeId 为空，则获取 所有可用节点的最新活动信息（lastAvailableNodeActivities）。
  //如果 nodeId 不为空，则获取 该节点的已完成分配信息（completedNodeAllocations）。
  //最终返回 ActivitiesInfo 对象，包含选定的分配信息，并支持按 groupBy 进行分类。
  public ActivitiesInfo getActivitiesInfo(String nodeId,
      RMWSConsts.ActivitiesGroupBy groupBy) {
    List<NodeAllocation> allocations;
    if (nodeId == null) {
      //如果 nodeId 为空，获取所有可用节点的最新活动信息（lastAvailableNodeActivities）
      allocations = lastAvailableNodeActivities;
    } else {
      //如果 nodeId 不为空，从 completedNodeAllocations（已完成的节点分配映射表）中查找 nodeId 对应的 NodeAllocation 记录
      allocations = completedNodeAllocations.get(NodeId.fromString(nodeId));
    }
    return new ActivitiesInfo(allocations, nodeId, groupBy);
  }

  //等待一定数量的调度活动 (activitiesCount) 记录完成
  public List<ActivitiesInfo> recordAndGetBulkActivitiesInfo(
      int activitiesCount, RMWSConsts.ActivitiesGroupBy groupBy)
      throws InterruptedException {
    //recordCount 是一个原子变量，代表需要等待多少个调度活动被记录
    recordCount.set(activitiesCount);
    while (recordCount.get() > 0) {
      Thread.sleep(1);
    }
    Iterator<Pair<NodeId, List<NodeAllocation>>> ite =
        lastNActivities.iterator();
    List<ActivitiesInfo> outList = new ArrayList<>();
    while (ite.hasNext()) {
      Pair<NodeId, List<NodeAllocation>> pair = ite.next();
      outList.add(new ActivitiesInfo(pair.getRight(),
          pair.getLeft().toString(), groupBy));
    }
    // reset with new activities
    //清空 lastNActivities，以便存储新的活动记录，防止数据污染
    lastNActivities = new ConcurrentLinkedDeque<>();
    return outList;
  }
  //用于记录下一个节点的调度活动，并标记 recordCount
  public void recordNextNodeUpdateActivities(String nodeId) {
    if (nodeId == null) {
      recordCount.compareAndSet(0, 1);
    } else {
      activeRecordedNodes.add(NodeId.fromString(nodeId));
    }
  }
  //该方法 开启对特定应用 (ApplicationId) 的调度活动记录，并设定记录的截止时间
  public void turnOnAppActivitiesRecording(ApplicationId applicationId,
      double maxTime) {
    long startTS = SystemClock.getInstance().getTime();
    long endTS = startTS + (long) (maxTime * 1000);
    recordingAppActivitiesUntilSpecifiedTime.put(applicationId, endTS);
  }
  // 该方法动态调整应用调度活动 (app activities) 的最大队列长度 (appActivitiesMaxQueueLength)，
  // 以适应当前集群规模 (numNodes) 和调度器线程数 (numAsyncSchedulerThreads)
  private void dynamicallyUpdateAppActivitiesMaxQueueLengthIfNeeded() {
    if (rmContext.getRMNodes() == null) {
      return;
    }
    //检查调度器类型是否为 CapacityScheduler
    if (rmContext.getScheduler() instanceof CapacityScheduler) {
      CapacityScheduler cs = (CapacityScheduler) rmContext.getScheduler();
      //若未启用多节点调度，计算新的 appActivitiesMaxQueueLength
      if (!cs.isMultiNodePlacementEnabled()) {
        //获取当前节点数和异步调度线程数
        int numNodes = rmContext.getRMNodes().size();
        int newAppActivitiesMaxQueueLength;
        int numAsyncSchedulerThreads = cs.getNumAsyncSchedulerThreads();
        if (numAsyncSchedulerThreads > 0) {
          newAppActivitiesMaxQueueLength =
              Math.max(configuredAppActivitiesMaxQueueLength,
                  numNodes * numAsyncSchedulerThreads);
        } else {
          newAppActivitiesMaxQueueLength =
              Math.max(configuredAppActivitiesMaxQueueLength,
                  (int) (numNodes * 1.2));
        }
        if (appActivitiesMaxQueueLength != newAppActivitiesMaxQueueLength) {
          LOG.info("Update max queue length of app activities from {} to {},"
                  + " configured={}, numNodes={}, numAsyncSchedulerThreads={}"
                  + " when multi-node placement disabled.",
              appActivitiesMaxQueueLength, newAppActivitiesMaxQueueLength,
              configuredAppActivitiesMaxQueueLength, numNodes,
              numAsyncSchedulerThreads);
          appActivitiesMaxQueueLength = newAppActivitiesMaxQueueLength;
        }
      } else if (appActivitiesMaxQueueLength
          != configuredAppActivitiesMaxQueueLength) {
        LOG.info("Update max queue length of app activities from {} to {}"
                + " when multi-node placement enabled.",
            appActivitiesMaxQueueLength, configuredAppActivitiesMaxQueueLength);
        appActivitiesMaxQueueLength = configuredAppActivitiesMaxQueueLength;
      }
    }
  }
  // 启动一个后台线程 (cleanUpThread)，用于定期清理过期的调度活动数据 (NodeAllocation 和 AppAllocation)，
  // 同时动态调整app activities队列最大长度
  @Override
  protected void serviceStart() throws Exception {
    cleanUpThread = new Thread(new Runnable() {
      @Override
      public void run() {
        while (!stopped && !Thread.currentThread().isInterrupted()) {
          Iterator<Map.Entry<NodeId, List<NodeAllocation>>> ite =
              completedNodeAllocations.entrySet().iterator();
          long curTS = SystemClock.getInstance().getTime();
          while (ite.hasNext()) {
            Map.Entry<NodeId, List<NodeAllocation>> nodeAllocation = ite.next();
            List<NodeAllocation> allocations = nodeAllocation.getValue();
            if (allocations.size() > 0
                && curTS - allocations.get(0).getTimestamp()
                > schedulerActivitiesTTL) {  //清理 已经超过 schedulerActivitiesTTL 的调度记录
              ite.remove();
            }
          }
          //清理 completedAppAllocations 中过期的 AppAllocation
          Iterator<Map.Entry<ApplicationId, Queue<AppAllocation>>> iteApp =
              completedAppAllocations.entrySet().iterator();
          while (iteApp.hasNext()) {
            Map.Entry<ApplicationId, Queue<AppAllocation>> appAllocation =
                iteApp.next();
            RMApp rmApp = rmContext.getRMApps().get(appAllocation.getKey());
            //若 RMApp 为空或已完成，则删除整个应用的 AppAllocation
            if (rmApp == null || rmApp.getFinalApplicationStatus()
                != FinalApplicationStatus.UNDEFINED) {
              iteApp.remove();
            } else {
              Iterator<AppAllocation> appActivitiesIt =
                  appAllocation.getValue().iterator();
              while (appActivitiesIt.hasNext()) {
                //否则，逐步清理过期的 AppAllocation
                if (curTS - appActivitiesIt.next().getTime()
                    > appActivitiesTTL) {
                  appActivitiesIt.remove();
                } else {
                  break;
                }
              }
              if (appAllocation.getValue().isEmpty()) {
                iteApp.remove();
                LOG.debug("Removed all expired activities from cache for {}.",
                    rmApp.getApplicationId());
              }
            }
          }

          LOG.debug("Remaining apps in app activities cache: {}",
              completedAppAllocations.keySet());
          // dynamically update max queue length of app activities if needed
          dynamicallyUpdateAppActivitiesMaxQueueLengthIfNeeded();
          try {
            Thread.sleep(activitiesCleanupIntervalMs);
          } catch (InterruptedException e) {
            LOG.info(getName() + " thread interrupted");
            break;
          }
        }
      }
    });
    cleanUpThread.setName("ActivitiesManager thread.");
    cleanUpThread.start();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    stopped = true;
    if (cleanUpThread != null) {
      cleanUpThread.interrupt();
      try {
        cleanUpThread.join();
      } catch (InterruptedException ie) {
        LOG.warn("Interrupted Exception while stopping", ie);
      }
    }
    super.serviceStop();
  }

  void startNodeUpdateRecording(NodeId nodeID) {
    if (recordCount.get() > 0) {
      recordNextNodeUpdateActivities(nodeID.toString());
    }
    // Removing from activeRecordedNodes immediately is to ensure that
    // activities will be recorded just once in multiple threads.
    if (activeRecordedNodes.remove(nodeID)) {
      List<NodeAllocation> nodeAllocation = new ArrayList<>();
      recordingNodesAllocation.get().put(nodeID, nodeAllocation);
      // enable diagnostic collector
      diagnosticCollectorManager.get().enable();
    }
  }

  void startAppAllocationRecording(NodeId nodeID, long currTS,
      SchedulerApplicationAttempt application) {
    ApplicationId applicationId = application.getApplicationId();

    Long turnOffTimestamp =
        recordingAppActivitiesUntilSpecifiedTime.get(applicationId);
    if (turnOffTimestamp != null) {
      if (turnOffTimestamp > currTS) {
        appsAllocation.get().put(applicationId,
            new AppAllocation(application.getPriority(), nodeID,
                application.getQueueName()));
        // enable diagnostic collector
        diagnosticCollectorManager.get().enable();
      } else {
        turnOffActivityMonitoringForApp(applicationId);
      }
    }
  }

  // Add queue, application or container activity into specific node allocation.
  void addSchedulingActivityForNode(NodeId nodeId, String parentName,
      String childName, Integer priority, ActivityState state,
      String diagnostic, ActivityLevel level, Long allocationRequestId) {
    if (shouldRecordThisNode(nodeId)) {
      NodeAllocation nodeAllocation = getCurrentNodeAllocation(nodeId);

      ResourceScheduler scheduler = this.rmContext.getScheduler();
      //Sorry about this :( Making sure CS short queue references are normalized
      if (scheduler instanceof CapacityScheduler) {
        CapacityScheduler cs = (CapacityScheduler)this.rmContext.getScheduler();
        parentName = cs.normalizeQueueName(parentName);
        childName  = cs.normalizeQueueName(childName);
      }

      nodeAllocation.addAllocationActivity(parentName, childName, priority,
          state, diagnostic, level, nodeId, allocationRequestId);
    }
  }

  // Add queue, application or container activity into specific application
  // allocation.
  void addSchedulingActivityForApp(ApplicationId applicationId,
      ContainerId containerId, Integer priority, ActivityState state,
      String diagnostic, ActivityLevel level, NodeId nodeId,
      Long allocationRequestId) {
    if (shouldRecordThisApp(applicationId)) {
      AppAllocation appAllocation = appsAllocation.get().get(applicationId);
      appAllocation.addAppAllocationActivity(containerId == null ?
          "Container-Id-Not-Assigned" :
          containerId.toString(), priority, state, diagnostic, level, nodeId,
          allocationRequestId);
    }
  }

  // Update container allocation meta status for this node allocation.
  // It updates general container status but not the detailed activity state
  // in updateActivityState.
  void updateAllocationFinalState(NodeId nodeID, ContainerId containerId,
      AllocationState containerState) {
    if (shouldRecordThisNode(nodeID)) {
      NodeAllocation nodeAllocation = getCurrentNodeAllocation(nodeID);
      nodeAllocation.updateContainerState(containerId, containerState);
    }
  }

  void finishAppAllocationRecording(ApplicationId applicationId,
      ContainerId containerId, ActivityState appState, String diagnostic) {
    if (shouldRecordThisApp(applicationId)) {
      long currTS = SystemClock.getInstance().getTime();
      AppAllocation appAllocation = appsAllocation.get().remove(applicationId);
      appAllocation.updateAppContainerStateAndTime(containerId, appState,
          currTS, diagnostic);

      Queue<AppAllocation> appAllocations =
          completedAppAllocations.get(applicationId);
      if (appAllocations == null) {
        appAllocations = new ConcurrentLinkedQueue<>();
        Queue<AppAllocation> curAppAllocations =
            completedAppAllocations.putIfAbsent(applicationId, appAllocations);
        if (curAppAllocations != null) {
          appAllocations = curAppAllocations;
        }
      }
      int curQueueLength = appAllocations.size();
      while (curQueueLength >= appActivitiesMaxQueueLength) {
        appAllocations.poll();
        --curQueueLength;
      }
      appAllocations.add(appAllocation);
      Long stopTime =
          recordingAppActivitiesUntilSpecifiedTime.get(applicationId);
      if (stopTime != null && stopTime <= currTS) {
        turnOffActivityMonitoringForApp(applicationId);
      }
    }
  }

  void finishNodeUpdateRecording(NodeId nodeID, String partition) {
    List<NodeAllocation> value = recordingNodesAllocation.get().get(nodeID);
    long timestamp = SystemClock.getInstance().getTime();

    if (value != null) {
      if (value.size() > 0) {
        lastAvailableNodeActivities = value;
        for (NodeAllocation allocation : lastAvailableNodeActivities) {
          allocation.transformToTree();
          allocation.setTimestamp(timestamp);
          allocation.setPartition(partition);
        }
        if (recordCount.get() > 0) {
          recordCount.getAndDecrement();
        }
      }

      if (shouldRecordThisNode(nodeID)) {
        recordingNodesAllocation.get().remove(nodeID);
        completedNodeAllocations.put(nodeID, value);
        if (recordCount.get() >= 0) {
          lastNActivities.add(Pair.of(nodeID, value));
        }
      }
    }
    // disable diagnostic collector
    diagnosticCollectorManager.get().disable();
  }

  boolean shouldRecordThisApp(ApplicationId applicationId) {
    if (recordingAppActivitiesUntilSpecifiedTime.isEmpty()
        || appsAllocation.get().isEmpty()) {
      return false;
    }
    return recordingAppActivitiesUntilSpecifiedTime.containsKey(applicationId)
        && appsAllocation.get().containsKey(applicationId);
  }

  boolean shouldRecordThisNode(NodeId nodeID) {
    return isRecordingMultiNodes() || recordingNodesAllocation.get()
        .containsKey(nodeID);
  }

  private NodeAllocation getCurrentNodeAllocation(NodeId nodeID) {
    NodeId recordingKey =
        isRecordingMultiNodes() ? EMPTY_NODE_ID : nodeID;
    List<NodeAllocation> nodeAllocations =
        recordingNodesAllocation.get().get(recordingKey);
    NodeAllocation nodeAllocation;
    // When this node has already stored allocation activities, get the
    // last allocation for this node.
    if (nodeAllocations.size() != 0) {
      nodeAllocation = nodeAllocations.get(nodeAllocations.size() - 1);
      // When final state in last allocation is not DEFAULT, it means
      // last allocation has finished. Create a new allocation for this node,
      // and add it to the allocation list. Return this new allocation.
      //
      // When final state in last allocation is DEFAULT,
      // it means last allocation has not finished. Just get last allocation.
      if (nodeAllocation.getFinalAllocationState() != AllocationState.DEFAULT) {
        nodeAllocation = new NodeAllocation(nodeID);
        nodeAllocations.add(nodeAllocation);
      }
    }
    // When this node has not stored allocation activities,
    // create a new allocation for this node, and add it to the allocation list.
    // Return this new allocation.
    else {
      nodeAllocation = new NodeAllocation(nodeID);
      nodeAllocations.add(nodeAllocation);
    }
    return nodeAllocation;
  }

  private void turnOffActivityMonitoringForApp(ApplicationId applicationId) {
    recordingAppActivitiesUntilSpecifiedTime.remove(applicationId);
  }

  public boolean isRecordingMultiNodes() {
    return recordingNodesAllocation.get().containsKey(EMPTY_NODE_ID);
  }

  /**
   * Get recording node id:
   * 1. node id of the input node if it is not null.
   * 2. EMPTY_NODE_ID if input node is null and activities manager is
   *    recording multi-nodes.
   * 3. null otherwise.
   * @param node - input node
   * @return recording nodeId
   */
  public NodeId getRecordingNodeId(SchedulerNode node) {
    if (node != null) {
      return node.getNodeID();
    } else if (isRecordingMultiNodes()) {
      return ActivitiesManager.EMPTY_NODE_ID;
    }
    return null;
  }

  /**
   * Class to manage the diagnostics collector.
   */
  public static class DiagnosticsCollectorManager {
    private boolean enabled = false;
    private DiagnosticsCollector gdc;

    public boolean isEnabled() {
      return enabled;
    }

    public void enable() {
      this.enabled = true;
    }

    public void disable() {
      this.enabled = false;
    }

    public DiagnosticsCollectorManager(DiagnosticsCollector gdc) {
      this.gdc = gdc;
    }

    public Optional<DiagnosticsCollector> getOptionalDiagnosticsCollector() {
      if (enabled) {
        return Optional.of(gdc);
      } else {
        return Optional.empty();
      }
    }
  }

  public Optional<DiagnosticsCollector> getOptionalDiagnosticsCollector() {
    return diagnosticCollectorManager.get().getOptionalDiagnosticsCollector();
  }

  public String getResourceDiagnostics(ResourceCalculator rc, Resource required,
      Resource available) {
    Optional<DiagnosticsCollector> dcOpt = getOptionalDiagnosticsCollector();
    if (dcOpt.isPresent()) {
      dcOpt.get().collectResourceDiagnostics(rc, required, available);
      return getDiagnostics(dcOpt.get());
    }
    return EMPTY_DIAGNOSTICS;
  }

  public static String getDiagnostics(Optional<DiagnosticsCollector> dcOpt) {
    if (dcOpt != null && dcOpt.isPresent()) {
      DiagnosticsCollector dc = dcOpt.get();
      if (dc != null && dc.getDiagnostics() != null) {
        return getDiagnostics(dc);
      }
    }
    return EMPTY_DIAGNOSTICS;
  }

  private static String getDiagnostics(DiagnosticsCollector dc) {
    StringBuilder sb = new StringBuilder();
    sb.append(", ").append(dc.getDiagnostics());
    if (dc.getDetails() != null) {
      sb.append(DIAGNOSTICS_DETAILS_SEPARATOR).append(dc.getDetails());
    }
    return sb.toString();
  }

  @VisibleForTesting
  public int getAppActivitiesMaxQueueLength() {
    return appActivitiesMaxQueueLength;
  }
}
