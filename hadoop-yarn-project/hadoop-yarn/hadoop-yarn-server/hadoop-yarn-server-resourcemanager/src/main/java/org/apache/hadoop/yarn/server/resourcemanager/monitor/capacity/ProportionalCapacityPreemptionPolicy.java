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

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractParentQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.SchedulingEditPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueResourceQuotas;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity
    .ManagedParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacities;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.preemption.PreemptableQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerPreemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;

/**
 * This class implement a {@link SchedulingEditPolicy} that is designed to be
 * paired with the {@code CapacityScheduler}. At every invocation of {@code
 * editSchedule()} it computes the ideal amount of resources assigned to each
 * queue (for each queue in the hierarchy), and determines whether preemption
 * is needed. Overcapacity is distributed among queues in a weighted fair manner,
 * where the weight is the amount of guaranteed capacity for the queue.
 * Based on this ideal assignment it determines whether preemption is required
 * and select a set of containers from each application that would be killed if
 * the corresponding amount of resources is not freed up by the application.
 *
 * If not in {@code observeOnly} mode, it triggers preemption requests via a
 * {@link ContainerPreemptEvent} that the {@code ResourceManager} will ensure
 * to deliver to the application (or to execute).
 *
 * If the deficit of resources is persistent over a long enough period of time
 * this policy will trigger forced termination of containers (again by generating
 * {@link ContainerPreemptEvent}).
 */

//用于管理 CapacityScheduler 预抢占的策略类。它实现了 SchedulingEditPolicy 和 CapacitySchedulerPreemptionContext，主要功能包括：
//计算队列理想的资源分配情况，确保资源公平分配
//识别是否需要进行资源抢占（preemption）
//选择需要被抢占的容器，并触发 ContainerPreemptEvent
//处理跨队列和队列内部的资源抢占
//采用比例抢占策略，按照各队列的保证容量（guaranteed capacity）进行公平分配
//维护抢占相关的策略，例如保守抢占 (conservative-drf)、懒抢占 (lazy preemption) 及优先级抢占
//如果抢占模式未处于 observeOnly（仅观察）模式，ResourceManager 会根据 ContainerPreemptEvent 进行容器杀死（Kill）操作，确保资源重新分配给有更高需求的队列或应用

public class ProportionalCapacityPreemptionPolicy
    implements SchedulingEditPolicy, CapacitySchedulerPreemptionContext {

  /**
   * IntraQueuePreemptionOrder will be used to define various priority orders
   * which could be configured by admin.
   */
  @Unstable
  public enum IntraQueuePreemptionOrderPolicy {
    PRIORITY_FIRST, USERLIMIT_FIRST;
  }

  private static final Logger LOG =
      LoggerFactory.getLogger(ProportionalCapacityPreemptionPolicy.class);

  private final Clock clock;

  // Configurable fields
  //允许的最大超量容量，超过此值时将触发资源抢占
  private double maxIgnoredOverCapacity;
  //容器被标记为可抢占后，等待多长时间再真正执行抢占
  private long maxWaitTime;
  //抢占策略执行的时间间隔，决定多久计算一次抢占决策
  private long monitoringInterval;
  //每轮抢占过程中，整个集群可被抢占的最大资源比例
  private float percentageClusterPreemptionAllowed;
  //应用任务自然终止的因子，如果资源长期短缺，则会触发更强制的抢占
  private double naturalTerminationFactor;
  //是否只观察抢占建议而不实际执行抢占操作
  private boolean observeOnly;
  //是否启用懒抢占，即如果某些任务即将完成，则避免强制杀死容器
  private boolean lazyPreempionEnabled;
  //队列内部抢占的最大允许限制
  private float maxAllowableLimitForIntraQueuePreemption;
  //队列内部抢占的最小触发阈值
  private float minimumThresholdForIntraQueuePreemption;
  //定义队列内部抢占时的优先级（支持 PRIORITY_FIRST 和 USERLIMIT_FIRST）
  private IntraQueuePreemptionOrderPolicy intraQueuePreemptionOrderPolicy;
  //是否启用跨队列抢占的保守策略，防止过度抢占
  private boolean crossQueuePreemptionConservativeDRF;
  //是否启用队列内抢占的保守策略，防止任务被过度抢占
  private boolean inQueuePreemptionConservativeDRF;

  // Current configuration
  //CapacityScheduler 的配置信息，提供抢占相关的参数
  private CapacitySchedulerConfiguration csConfig;

  // Pointer to other RM components
  //资源管理器 (ResourceManager) 的上下文，用于访问 YARN 其他组件
  private RMContext rmContext;
  //资源计算器，用于比较和计算资源需求
  private ResourceCalculator rc;
  private CapacityScheduler scheduler;
  //节点标签管理器 (RMNodeLabelsManager)，支持按标签进行资源抢占
  private RMNodeLabelsManager nlm;

  // Internal properties to make decisions of what to preempt
  //存储待抢占的 RMContainer 容器及其标记时间
  private final Map<RMContainer,Long> preemptionCandidates =
    new HashMap<>();
  //按队列和分区存储 TempQueuePerPartition（临时队列）
  private Map<String, Map<String, TempQueuePerPartition>> queueToPartitions =
      new HashMap<>();
  //存储资源不足的队列集合
  private Map<String, LinkedHashSet<String>> partitionToUnderServedQueues =
      new HashMap<String, LinkedHashSet<String>>();
  //存储不同的候选容器选择策略
  private List<PreemptionCandidatesSelector> candidatesSelectionPolicies;
  //所有的资源分区集合
  private Set<String> allPartitions;
  //所有叶子队列的集合
  private Set<String> leafQueueNames;
  //抢占候选容器映射表，按照 ApplicationAttemptId 组织
  Map<PreemptionCandidatesSelector, Map<ApplicationAttemptId,
      Set<RMContainer>>> pcsMap;

  // Preemptable Entities, synced from scheduler at every run
  //当前可抢占的队列信息
  private Map<String, PreemptableQueue> preemptableQueues;
  //存储可被终止的容器 ID
  private Set<ContainerId> killableContainers;

  public ProportionalCapacityPreemptionPolicy() {
    clock = SystemClock.getInstance();
    allPartitions = Collections.emptySet();
    leafQueueNames = Collections.emptySet();
    preemptableQueues = Collections.emptyMap();
  }

  @VisibleForTesting
  public ProportionalCapacityPreemptionPolicy(RMContext context,
      CapacityScheduler scheduler, Clock clock) {
    init(context.getYarnConfiguration(), context, scheduler);
    this.clock = clock;
    allPartitions = Collections.emptySet();
    leafQueueNames = Collections.emptySet();
    preemptableQueues = Collections.emptyMap();
  }

  public void init(Configuration config, RMContext context,
      ResourceScheduler sched) {
    LOG.info("Preemption monitor:" + this.getClass().getCanonicalName());
    assert null == scheduler : "Unexpected duplicate call to init";
    if (!(sched instanceof CapacityScheduler)) {
      throw new YarnRuntimeException("Class " +
          sched.getClass().getCanonicalName() + " not instance of " +
          CapacityScheduler.class.getCanonicalName());
    }
    rmContext = context;
    scheduler = (CapacityScheduler) sched;
    rc = scheduler.getResourceCalculator();
    nlm = scheduler.getRMContext().getNodeLabelManager();
    updateConfigIfNeeded();
  }
  // 在检测到调度器配置 (csConfig) 发生变化时，更新与抢占（Preemption）相关的参数。
  // 它主要用于 Hadoop 的 CapacityScheduler（容量调度器），确保抢占策略与最新的调度配置保持一致
  private void updateConfigIfNeeded() {
    CapacitySchedulerConfiguration config = scheduler.getConfiguration();
    if (config == csConfig) {
      return;
    }
    //允许的最大超量容量，超过此值时将触发资源抢占
    maxIgnoredOverCapacity = config.getDouble(
        CapacitySchedulerConfiguration.PREEMPTION_MAX_IGNORED_OVER_CAPACITY,
        CapacitySchedulerConfiguration.DEFAULT_PREEMPTION_MAX_IGNORED_OVER_CAPACITY);
    //应用任务自然终止的因子，如果资源长期短缺，则会触发更强制的抢占
    naturalTerminationFactor = config.getDouble(
        CapacitySchedulerConfiguration.PREEMPTION_NATURAL_TERMINATION_FACTOR,
        CapacitySchedulerConfiguration.DEFAULT_PREEMPTION_NATURAL_TERMINATION_FACTOR);

    maxWaitTime = config.getLong(
        CapacitySchedulerConfiguration.PREEMPTION_WAIT_TIME_BEFORE_KILL,
        CapacitySchedulerConfiguration.DEFAULT_PREEMPTION_WAIT_TIME_BEFORE_KILL);

    monitoringInterval = config.getLong(
        CapacitySchedulerConfiguration.PREEMPTION_MONITORING_INTERVAL,
        CapacitySchedulerConfiguration.DEFAULT_PREEMPTION_MONITORING_INTERVAL);

    percentageClusterPreemptionAllowed = config.getFloat(
        CapacitySchedulerConfiguration.TOTAL_PREEMPTION_PER_ROUND,
        CapacitySchedulerConfiguration.DEFAULT_TOTAL_PREEMPTION_PER_ROUND);

    observeOnly = config.getBoolean(
        CapacitySchedulerConfiguration.PREEMPTION_OBSERVE_ONLY,
        CapacitySchedulerConfiguration.DEFAULT_PREEMPTION_OBSERVE_ONLY);

    lazyPreempionEnabled = config.getBoolean(
        CapacitySchedulerConfiguration.LAZY_PREEMPTION_ENABLED,
        CapacitySchedulerConfiguration.DEFAULT_LAZY_PREEMPTION_ENABLED);

    maxAllowableLimitForIntraQueuePreemption = config.getFloat(
        CapacitySchedulerConfiguration.
        INTRAQUEUE_PREEMPTION_MAX_ALLOWABLE_LIMIT,
        CapacitySchedulerConfiguration.
        DEFAULT_INTRAQUEUE_PREEMPTION_MAX_ALLOWABLE_LIMIT);

    minimumThresholdForIntraQueuePreemption = config.getFloat(
        CapacitySchedulerConfiguration.
        INTRAQUEUE_PREEMPTION_MINIMUM_THRESHOLD,
        CapacitySchedulerConfiguration.
        DEFAULT_INTRAQUEUE_PREEMPTION_MINIMUM_THRESHOLD);

    intraQueuePreemptionOrderPolicy = IntraQueuePreemptionOrderPolicy
        .valueOf(config
            .get(
                CapacitySchedulerConfiguration.INTRAQUEUE_PREEMPTION_ORDER_POLICY,
                CapacitySchedulerConfiguration.DEFAULT_INTRAQUEUE_PREEMPTION_ORDER_POLICY)
            .toUpperCase());

    crossQueuePreemptionConservativeDRF =  config.getBoolean(
        CapacitySchedulerConfiguration.
        CROSS_QUEUE_PREEMPTION_CONSERVATIVE_DRF,
        CapacitySchedulerConfiguration.
        DEFAULT_CROSS_QUEUE_PREEMPTION_CONSERVATIVE_DRF);

    inQueuePreemptionConservativeDRF =  config.getBoolean(
        CapacitySchedulerConfiguration.
        IN_QUEUE_PREEMPTION_CONSERVATIVE_DRF,
        CapacitySchedulerConfiguration.
        DEFAULT_IN_QUEUE_PREEMPTION_CONSERVATIVE_DRF);

    candidatesSelectionPolicies = new ArrayList<>();

    // Do we need white queue-priority preemption policy?
    boolean isQueuePriorityPreemptionEnabled =
        config.getPUOrderingPolicyUnderUtilizedPreemptionEnabled();
    if (isQueuePriorityPreemptionEnabled) {
      candidatesSelectionPolicies.add(
          new QueuePriorityContainerCandidateSelector(this));
    }

    // Do we need to specially consider reserved containers?
    boolean selectCandidatesForResevedContainers = config.getBoolean(
        CapacitySchedulerConfiguration.
        PREEMPTION_SELECT_CANDIDATES_FOR_RESERVED_CONTAINERS,
        CapacitySchedulerConfiguration.
        DEFAULT_PREEMPTION_SELECT_CANDIDATES_FOR_RESERVED_CONTAINERS);
    if (selectCandidatesForResevedContainers) {
      candidatesSelectionPolicies
          .add(new ReservedContainerCandidatesSelector(this));
    }

    boolean additionalPreemptionBasedOnReservedResource = config.getBoolean(
        CapacitySchedulerConfiguration.ADDITIONAL_RESOURCE_BALANCE_BASED_ON_RESERVED_CONTAINERS,
        CapacitySchedulerConfiguration.DEFAULT_ADDITIONAL_RESOURCE_BALANCE_BASED_ON_RESERVED_CONTAINERS);

    // initialize candidates preemption selection policies
    candidatesSelectionPolicies.add(new FifoCandidatesSelector(this,
        additionalPreemptionBasedOnReservedResource, false));

    // Do we need to do preemption to balance queue even after queues get satisfied?
    boolean isPreemptionToBalanceRequired = config.getBoolean(
        CapacitySchedulerConfiguration.PREEMPTION_TO_BALANCE_QUEUES_BEYOND_GUARANTEED,
        CapacitySchedulerConfiguration.DEFAULT_PREEMPTION_TO_BALANCE_QUEUES_BEYOND_GUARANTEED);
    long maximumKillWaitTimeForPreemptionToQueueBalance = config.getLong(
        CapacitySchedulerConfiguration.MAX_WAIT_BEFORE_KILL_FOR_QUEUE_BALANCE_PREEMPTION,
        CapacitySchedulerConfiguration.DEFAULT_MAX_WAIT_BEFORE_KILL_FOR_QUEUE_BALANCE_PREEMPTION);
    if (isPreemptionToBalanceRequired) {
      PreemptionCandidatesSelector selector = new FifoCandidatesSelector(this,
          false, true);
      selector.setMaximumKillWaitTime(maximumKillWaitTimeForPreemptionToQueueBalance);
      candidatesSelectionPolicies.add(selector);
    }

    // Do we need to specially consider intra queue
    boolean isIntraQueuePreemptionEnabled = config.getBoolean(
        CapacitySchedulerConfiguration.INTRAQUEUE_PREEMPTION_ENABLED,
        CapacitySchedulerConfiguration.DEFAULT_INTRAQUEUE_PREEMPTION_ENABLED);
    if (isIntraQueuePreemptionEnabled) {
      candidatesSelectionPolicies.add(new IntraQueueCandidatesSelector(this));
    }

    LOG.info("Capacity Scheduler configuration changed, updated preemption " +
        "properties to:\n" +
        "max_ignored_over_capacity = " + maxIgnoredOverCapacity + "\n" +
        "natural_termination_factor = " + naturalTerminationFactor + "\n" +
        "max_wait_before_kill = " + maxWaitTime + "\n" +
        "monitoring_interval = " + monitoringInterval + "\n" +
        "total_preemption_per_round = " + percentageClusterPreemptionAllowed +
          "\n" +
        "observe_only = " + observeOnly + "\n" +
        "lazy-preemption-enabled = " + lazyPreempionEnabled + "\n" +
        "intra-queue-preemption.enabled = " + isIntraQueuePreemptionEnabled +
          "\n" +
        "intra-queue-preemption.max-allowable-limit = " +
          maxAllowableLimitForIntraQueuePreemption + "\n" +
        "intra-queue-preemption.minimum-threshold = " +
          minimumThresholdForIntraQueuePreemption + "\n" +
        "intra-queue-preemption.preemption-order-policy = " +
          intraQueuePreemptionOrderPolicy + "\n" +
        "priority-utilization.underutilized-preemption.enabled = " +
          isQueuePriorityPreemptionEnabled + "\n" +
        "select_based_on_reserved_containers = " +
          selectCandidatesForResevedContainers + "\n" +
        "additional_res_balance_based_on_reserved_containers = " +
          additionalPreemptionBasedOnReservedResource + "\n" +
        "Preemption-to-balance-queue-enabled = " +
          isPreemptionToBalanceRequired + "\n" +
        "cross-queue-preemption.conservative-drf = " +
          crossQueuePreemptionConservativeDRF + "\n" +
        "in-queue-preemption.conservative-drf = " +
          inQueuePreemptionConservativeDRF);

    csConfig = config;
  }

  @Override
  public ResourceCalculator getResourceCalculator() {
    return rc;
  }

  //该方法 editSchedule() 主要用于 Hadoop YARN 的 CapacityScheduler（容量调度器），它的作用是：
  //更新抢占配置（如果有变更）。
  //执行抢占或终止任务，确保资源调度合理。
  //记录执行时间（用于调试）
  @Override
  public synchronized void editSchedule() {
    //更新抢占相关的调度配置
    updateConfigIfNeeded();

    long startTs = clock.getTime();
    //获取 调度器的根队列（CSQueue）
    CSQueue root = scheduler.getRootQueue();
    //获取当前集群的资源状态
    Resource clusterResources = Resources.clone(scheduler.getClusterResource());
    //执行基于容器的抢占或终止
    containerBasedPreemptOrKill(root, clusterResources);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Total time used=" + (clock.getTime() - startTs) + " ms.");
    }
  }
  //根据选择的抢占候选容器，决定是否继续等待、发送抢占请求，或者直接杀掉容器。
  // 如果某个容器 超过了允许的最大等待时间，则会将其标记为 KILLABLE，否则仅标记为 PREEMPTION，等待一段时间后再决定是否真正杀死该容器
  //toPreemptPerSelector：一个 Map，表示由不同的 抢占策略（PreemptionCandidatesSelector） 选出的需要抢占的容器
  //currentTime：当前时间戳，用于判断是否超过 最大等待时间（maxWaitTime）
  private void preemptOrkillSelectedContainerAfterWait(
      Map<PreemptionCandidatesSelector, Map<ApplicationAttemptId,
          Set<RMContainer>>> toPreemptPerSelector, long currentTime) {
    //统计需要抢占的容器总数
    int toPreemptCount = 0;
    for (Map<ApplicationAttemptId, Set<RMContainer>> containers :
        toPreemptPerSelector.values()) {
      toPreemptCount += containers.size();
    }
    LOG.debug(
        "Starting to preempt containers for selectedCandidates and size:{}",
        toPreemptCount);

    // preempt (or kill) the selected containers
    // We need toPreemptPerSelector here to match list of containers to
    // its selector so that we can get custom timeout per selector when
    // checking if current container should be killed or not
    //遍历所有抢占策略及其选出的容器
    for (Map.Entry<PreemptionCandidatesSelector, Map<ApplicationAttemptId,
        Set<RMContainer>>> pc : toPreemptPerSelector
        .entrySet()) {
      Map<ApplicationAttemptId, Set<RMContainer>> cMap = pc.getValue();
      if (cMap.size() > 0) {
        //遍历应用尝试及其容器
        for (Map.Entry<ApplicationAttemptId,
            Set<RMContainer>> e : cMap.entrySet()) {
          ApplicationAttemptId appAttemptId = e.getKey();
          if (LOG.isDebugEnabled()) {
            LOG.debug("Send to scheduler: in app=" + appAttemptId
                + " #containers-to-be-preemptionCandidates=" + e.getValue().size());
          }
          //处理每个容器：判断是否超过最大等待时间
          for (RMContainer container : e.getValue()) {
            // if we tried to preempt this for more than maxWaitTime, this
            // should be based on custom timeout per container per selector
            if (preemptionCandidates.get(container) != null
                && preemptionCandidates.get(container)
                + pc.getKey().getMaximumKillWaitTimeMs() <= currentTime) {//超过了该策略允许的最大等待时间
              // kill it 直接杀掉该容器
              rmContext.getDispatcher().getEventHandler().handle(
                  new ContainerPreemptEvent(appAttemptId, container,
                      SchedulerEventType.MARK_CONTAINER_FOR_KILLABLE));
              preemptionCandidates.remove(container);
            } else {
              if (preemptionCandidates.get(container) != null) {
                // We already updated the information to scheduler earlier, we need
                // not have to raise another event.
                continue;
              }
              //通知调度器该容器 可能需要被抢占，但不立即杀死它
              //otherwise just send preemption events
              rmContext.getDispatcher().getEventHandler().handle(
                  new ContainerPreemptEvent(appAttemptId, container,
                      SchedulerEventType.MARK_CONTAINER_FOR_PREEMPTION));
              preemptionCandidates.put(container, currentTime);
            }
          }
        }
      }
    }
  }
  // 该方法主要用于 同步调度器中的“可终止（Killable）容器”，
  // 确保在 懒惰抢占（Lazy Preemption） 模式下，调度器不会立即杀死容器，而是优先标记它们为 可终止（Killable）
  private void syncKillableContainersFromScheduler() {
    // sync preemptable entities from scheduler
    preemptableQueues =
        scheduler.getPreemptionManager().getShallowCopyOfPreemptableQueues();

    killableContainers = new HashSet<>();
    //收集所有可终止（Killable）的容器
    for (Map.Entry<String, PreemptableQueue> entry : preemptableQueues
        .entrySet()) {
      PreemptableQueue entity = entry.getValue();
      for (Map<ContainerId, RMContainer> map : entity.getKillableContainers()
          .values()) {
        killableContainers.addAll(map.keySet());
      }
    }
  }

  private void cleanupStaledPreemptionCandidates(long currentTime) {
    // Keep the preemptionCandidates list clean
    // garbage collect containers that are irrelevant for preemption
    // And avoid preempt selected containers for *this execution*
    // or within 1 ms
    preemptionCandidates.entrySet()
        .removeIf(candidate ->
            candidate.getValue() + 2 * maxWaitTime < currentTime);
  }
  //判断队列石佛是叶子分区
  private Set<String> getLeafQueueNames(TempQueuePerPartition q) {
    // Also exclude ParentQueues, which might be without children
    if (CollectionUtils.isEmpty(q.children)
        && !(q.parentQueue instanceof ManagedParentQueue)
        && (q.parentQueue == null
        || !q.parentQueue.isEligibleForAutoQueueCreation())) {
      return ImmutableSet.of(q.queueName);
    }

    Set<String> leafQueueNames = new HashSet<>();
    for (TempQueuePerPartition child : q.children) {
      leafQueueNames.addAll(getLeafQueueNames(child));
    }

    return leafQueueNames;
  }

  /**
   * This method selects and tracks containers to be preemptionCandidates. If a container
   * is in the target list for more than maxWaitTime it is killed.
   *
   * @param root the root of the CapacityScheduler queue hierarchy
   * @param clusterResources the total amount of resources in the cluster
   */
  //收集可抢占的容器（Containers）
  //根据策略选择要抢占或终止的任务
  //确保资源公平分配，避免某些队列长期占用资源
  private void containerBasedPreemptOrKill(CSQueue root,
      Resource clusterResources) {
    // Sync killable containers from scheduler when lazy preemption enabled
    //如果启用了“懒惰抢占”（Lazy Preemption），同步可终止的容器
    if (lazyPreempionEnabled) {
      syncKillableContainersFromScheduler();
    }

    // All partitions to look at
    //获取集群中所有的节点标签，并将其加入 partitions
    //NO_LABEL 表示无标签节点，即默认资源池
    Set<String> partitions = new HashSet<>();
    partitions.addAll(scheduler.getRMContext()
        .getNodeLabelManager().getClusterNodeLabelNames());
    partitions.add(RMNodeLabelsManager.NO_LABEL);
    this.allPartitions = ImmutableSet.copyOf(partitions);

    // extract a summary of the queues from scheduler
    synchronized (scheduler) {
      queueToPartitions.clear();
      //递归克隆调度队列，获取每个分区的资源使用情况
      for (String partitionToLookAt : allPartitions) {
        cloneQueues(root, Resources
                .clone(nlm.getResourceByLabel(partitionToLookAt, clusterResources)),
            partitionToLookAt);
      }

      // Update effective priority of queues
    }
    //更新叶子队列名称
    this.leafQueueNames = ImmutableSet.copyOf(getLeafQueueNames(
        getQueueByPartition(CapacitySchedulerConfiguration.ROOT,
            RMNodeLabelsManager.NO_LABEL)));

    // compute total preemption allowed
    //计算出最多可以抢占多少资源，避免过度抢占影响系统稳定性
    Resource totalPreemptionAllowed = Resources.multiply(clusterResources,
        percentageClusterPreemptionAllowed);

    //clear under served queues for every run
    partitionToUnderServedQueues.clear();

    // based on ideal allocation select containers to be preemptionCandidates from each
    // queue and each application
    //选择需要抢占的容器
    Map<ApplicationAttemptId, Set<RMContainer>> toPreempt =
        new HashMap<>();
    Map<PreemptionCandidatesSelector, Map<ApplicationAttemptId,
        Set<RMContainer>>> toPreemptPerSelector =  new HashMap<>();
    for (PreemptionCandidatesSelector selector :
        candidatesSelectionPolicies) {
      //策略选择器 PreemptionCandidatesSelector 负责选出要抢占的容器
      long startTime = 0;
      if (LOG.isDebugEnabled()) {
        LOG.debug(MessageFormat
            .format("Trying to use {0} to select preemption candidates",
                selector.getClass().getName()));
        startTime = clock.getTime();
      }
      Map<ApplicationAttemptId, Set<RMContainer>> curCandidates =
          selector.selectCandidates(toPreempt, clusterResources,
              totalPreemptionAllowed);
      //记录每个策略选出的容器，便于分析抢占决策
      toPreemptPerSelector.putIfAbsent(selector, curCandidates);

      if (LOG.isDebugEnabled()) {
        LOG.debug(MessageFormat
            .format("{0} uses {1} millisecond to run",
                selector.getClass().getName(), clock.getTime() - startTime));
        int totalSelected = 0;
        int curSelected = 0;
        for (Set<RMContainer> set : toPreempt.values()) {
          totalSelected += set.size();
        }
        for (Set<RMContainer> set : curCandidates.values()) {
          curSelected += set.size();
        }
        LOG.debug(MessageFormat
            .format("So far, total {0} containers selected to be preempted, {1}"
                    + " containers selected this round\n",
                totalSelected, curSelected));
      }
    }

    if (LOG.isDebugEnabled()) {
      logToCSV(new ArrayList<>(leafQueueNames));
    }

    // if we are in observeOnly mode return before any action is taken
    //如果启用了“观察模式”（Observe Only），则跳过抢占
    if (observeOnly) {
      return;
    }

    // TODO: need consider revert killable containers when no more demandings.
    // Since we could have several selectors to make decisions concurrently.
    // So computed ideal-allocation varies between different selectors.
    //
    // We may need to "score" killable containers and revert the most preferred
    // containers. The bottom line is, we shouldn't preempt a queue which is already
    // below its guaranteed resource.

    long currentTime = clock.getTime();

    pcsMap = toPreemptPerSelector;

    // preempt (or kill) the selected containers
    //执行抢占或终止
    preemptOrkillSelectedContainerAfterWait(toPreemptPerSelector, currentTime);

    // cleanup staled preemption candidates
    //清理过时的抢占候选项
    cleanupStaledPreemptionCandidates(currentTime);
  }

  @Override
  public long getMonitoringInterval() {
    return monitoringInterval;
  }

  @Override
  public String getPolicyName() {
    return "ProportionalCapacityPreemptionPolicy";
  }

  @VisibleForTesting
  public Map<RMContainer, Long> getToPreemptContainers() {
    return preemptionCandidates;
  }

  /**
   * This method walks a tree of CSQueue and clones the portion of the state
   * relevant for preemption in TempQueue(s). It also maintains a pointer to
   * the leaves. Finally it aggregates pending resources in each queue and rolls
   * it up to higher levels.
   *
   * @param curQueue current queue which I'm looking at now
   * @param partitionResource the total amount of resources in the cluster
   * @return the root of the cloned queue hierarchy
   */
  //主要用于 克隆队列层级结构（CSQueue），并处理与抢占相关的状态数据，生成 临时队列（TempQueuePerPartition）。
  // 它支持 递归克隆队列，并维护每个队列的资源使用情况，同时还会将数据汇总到父队列中
  //CSQueue curQueue: 当前正在处理的队列（CSQueue）。它可能是一个父队列或叶子队列
  //Resource partitionResource: 集群资源的总量，可能根据分区而有所不同
  //String partitionToLookAt: 当前需要查看的分区标签
  private TempQueuePerPartition cloneQueues(CSQueue curQueue,
      Resource partitionResource, String partitionToLookAt) {
    TempQueuePerPartition ret;
    ReadLock readLock = curQueue.getReadLock();
    // Acquire a read lock from Parent/LeafQueue.
    readLock.lock();
    try {
      //队列的路径，通常是队列在调度器中的唯一标识
      String queuePath = curQueue.getQueuePath();
      // 队列的容量设置，包括 绝对容量 (absCap) 和 最大容量 (absMaxCap)
      QueueCapacities qc = curQueue.getQueueCapacities();
      float absCap = qc.getAbsoluteCapacity(partitionToLookAt);
      float absMaxCap = qc.getAbsoluteMaximumCapacity(partitionToLookAt);
      //标识当前队列是否禁用抢占
      boolean preemptionDisabled = curQueue.getPreemptionDisabled();
      //队列的资源配额，确定队列在给定分区中的最小资源和最大资源
      QueueResourceQuotas queueResourceQuotas = curQueue
          .getQueueResourceQuotas();
      Resource effMinRes = queueResourceQuotas
          .getEffectiveMinResource(partitionToLookAt);
      Resource effMaxRes = queueResourceQuotas
          .getEffectiveMaxResource(partitionToLookAt);
      //克隆当前队列在该分区上使用的资源（已分配的资源）
      Resource current = Resources
          .clone(curQueue.getQueueResourceUsage().getUsed(partitionToLookAt));
      //初始化为空，稍后会通过 preemptableQueues 更新它为可终止（killable）的资源
      Resource killable = Resources.none();
      //克隆当前队列在该分区的保留资源
      Resource reserved = Resources.clone(
          curQueue.getQueueResourceUsage().getReserved(partitionToLookAt));
      //如果队列路径存在于 preemptableQueues 中，那么当前队列的资源会被标记为 可终止（killable）资源，这些容器可以被抢占
      if (null != preemptableQueues.get(queuePath)) {
        killable = Resources.clone(preemptableQueues.get(queuePath)
            .getKillableResource(partitionToLookAt));
      }

      // when partition is a non-exclusive partition, the actual maxCapacity
      // could more than specified maxCapacity
      //如果分区不是 独占分区（Exclusive Node Label），则其最大容量会被设置为 100%（1.0f），允许队列使用更多的资源
      try {
        if (!scheduler.getRMContext().getNodeLabelManager()
            .isExclusiveNodeLabel(partitionToLookAt)) {
          absMaxCap = 1.0f;
        }
      } catch (IOException e) {
        // This may cause by partition removed when running capacity monitor,
        // just ignore the error, this will be corrected when doing next check.
      }

      ret = new TempQueuePerPartition(queuePath, current, preemptionDisabled,
          partitionToLookAt, killable, absCap, absMaxCap, partitionResource,
          reserved, curQueue, effMinRes, effMaxRes);
      //如果当前队列是 父队列（AbstractParentQueue），则递归地克隆它的子队列，并将子队列添加到当前队列的子队列列表中
      if (curQueue instanceof AbstractParentQueue) {
        String configuredOrderingPolicy =
            ((AbstractParentQueue) curQueue).getQueueOrderingPolicy().getConfigName();

        // Recursively add children
        for (CSQueue c : curQueue.getChildQueues()) {
          TempQueuePerPartition subq = cloneQueues(c, partitionResource,
              partitionToLookAt);

          // If we respect priority
          if (StringUtils.equals(
              CapacitySchedulerConfiguration.QUEUE_PRIORITY_UTILIZATION_ORDERING_POLICY,
              configuredOrderingPolicy)) {
            subq.relativePriority = c.getPriority().getPriority();
          }
          ret.addChild(subq);
          subq.parent = ret;
        }
      }
    } finally {
      readLock.unlock();
    }

    addTempQueuePartition(ret);
    return ret;
  }

  // simple printout function that reports internal queue state (useful for
  // plotting)
  private void logToCSV(List<String> leafQueueNames){
    Collections.sort(leafQueueNames);
    String queueState = " QUEUESTATE: " + clock.getTime();
    StringBuilder sb = new StringBuilder();
    sb.append(queueState);

    for (String queueName : leafQueueNames) {
      TempQueuePerPartition tq =
          getQueueByPartition(queueName, RMNodeLabelsManager.NO_LABEL);
      sb.append(", ");
      tq.appendLogString(sb);
    }
    LOG.debug(sb.toString());
  }
  //这个方法的作用是 将 TempQueuePerPartition 对象存储到 queueToPartitions 这个映射（Map）中，
  // 以便在后续的调度或抢占过程中能够快速查找特定队列及其分区的状态
  private void addTempQueuePartition(TempQueuePerPartition queuePartition) {
    String queueName = queuePartition.queueName;

    Map<String, TempQueuePerPartition> queuePartitions;
    if (null == (queuePartitions = queueToPartitions.get(queueName))) {
      queuePartitions = new HashMap<>();
      queueToPartitions.put(queueName, queuePartitions);
    }
    queuePartitions.put(queuePartition.partition, queuePartition);
  }

  /**
   * Get queue partition by given queueName and partitionName
   */
  //根据队列名称和分区获取队列临时分区
  @Override
  public TempQueuePerPartition getQueueByPartition(String queueName,
      String partition) {
    Map<String, TempQueuePerPartition> partitionToQueues;
    if (null == (partitionToQueues = queueToPartitions.get(queueName))) {
      throw new YarnRuntimeException("This shouldn't happen, cannot find "
          + "TempQueuePerPartition for queueName=" + queueName);
    }
    return partitionToQueues.get(partition);
  }

  /**
   * Get all queue partitions by given queueName
   */
  @Override
  public Collection<TempQueuePerPartition> getQueuePartitions(String queueName) {
    if (!queueToPartitions.containsKey(queueName)) {
      throw new YarnRuntimeException("This shouldn't happen, cannot find "
          + "TempQueuePerPartition collection for queueName=" + queueName);
    }
    return queueToPartitions.get(queueName).values();
  }

  @Override
  public CapacityScheduler getScheduler() {
    return scheduler;
  }

  @Override
  public RMContext getRMContext() {
    return rmContext;
  }

  @Override
  public boolean isObserveOnly() {
    return observeOnly;
  }

  @Override
  public Set<ContainerId> getKillableContainers() {
    return killableContainers;
  }

  @Override
  public double getMaxIgnoreOverCapacity() {
    return maxIgnoredOverCapacity;
  }

  @Override
  public double getNaturalTerminationFactor() {
    return naturalTerminationFactor;
  }

  @Override
  public Set<String> getLeafQueueNames() {
    return leafQueueNames;
  }

  @Override
  public Set<String> getAllPartitions() {
    return allPartitions;
  }

  @VisibleForTesting
  Map<String, Map<String, TempQueuePerPartition>> getQueuePartitions() {
    return queueToPartitions;
  }

  @VisibleForTesting
  Map<PreemptionCandidatesSelector, Map<ApplicationAttemptId,
      Set<RMContainer>>> getToPreemptCandidatesPerSelector() {
    return pcsMap;
  }

  @Override
  public int getClusterMaxApplicationPriority() {
    return scheduler.getMaxClusterLevelAppPriority().getPriority();
  }

  @Override
  public float getMaxAllowableLimitForIntraQueuePreemption() {
    return maxAllowableLimitForIntraQueuePreemption;
  }

  @Override
  public float getMinimumThresholdForIntraQueuePreemption() {
    return minimumThresholdForIntraQueuePreemption;
  }

  @Override
  public Resource getPartitionResource(String partition) {
    return Resources.clone(nlm.getResourceByLabel(partition,
        Resources.clone(scheduler.getClusterResource())));
  }

  public LinkedHashSet<String> getUnderServedQueuesPerPartition(
      String partition) {
    return partitionToUnderServedQueues.get(partition);
  }

  public void addPartitionToUnderServedQueues(String queueName,
      String partition) {
    LinkedHashSet<String> underServedQueues = partitionToUnderServedQueues
        .get(partition);
    if (null == underServedQueues) {
      underServedQueues = new LinkedHashSet<String>();
      partitionToUnderServedQueues.put(partition, underServedQueues);
    }
    underServedQueues.add(queueName);
  }

  @Override
  public IntraQueuePreemptionOrderPolicy getIntraQueuePreemptionOrderPolicy() {
    return intraQueuePreemptionOrderPolicy;
  }

  @Override
  public boolean getCrossQueuePreemptionConservativeDRF() {
    return crossQueuePreemptionConservativeDRF;
  }

  @Override
  public boolean getInQueuePreemptionConservativeDRF() {
    return inQueuePreemptionConservativeDRF;
  }

  @Override
  public long getDefaultMaximumKillWaitTimeout() {
    return maxWaitTime;
  }
}
