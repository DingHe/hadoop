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
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.QueueEntitlement;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SchedulerResourceTypes;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.SettableFuture;

/**
 * This interface is used by the components to talk to the
 * scheduler for allocating of resources, cleaning up resources.
 *
 */
// YarnScheduler 负责管理 YARN 集群中的资源分配，提供应用程序调度功能。
// 它接收 ApplicationMaster 的资源请求，并根据集群资源情况和调度策略进行资源分配，同时支持队列管理、权限控制、容器分配与回收等功能。
//不同的调度器（如 CapacityScheduler 和 FairScheduler）都会实现这个接口，以提供具体的资源管理和调度策略
public interface YarnScheduler extends EventHandler<SchedulerEvent> {

  /**
   * Get queue information.
   *
   * @param queueName queue name  队列名称
   * @param includeChildQueues include child queues? 是否包含子队列的信息
   * @param recursive get children queues? 是否递归获取子队列信息
   * @return queue information
   * @throws IOException an I/O exception has occurred.
   */
  //获取指定名称的队列信息
  @Public
  @Stable
  public QueueInfo getQueueInfo(String queueName, boolean includeChildQueues,
      boolean recursive) throws IOException;

  /**获取当前用户在各个队列中的权限信息
   * Get acls for queues for current user.
   * @return acls for queues for current user
   */
  @Public
  @Stable
  public List<QueueUserACLInfo> getQueueUserAclInfo();

  /** 获取整个 YARN 集群的资源总量
   * Get the whole resource capacity of the cluster.
   * @return the whole resource capacity of the cluster.
   */
  @LimitedPrivate("yarn")
  @Unstable
  public Resource getClusterResource();

  /**获取最小可分配资源单位
   * Get minimum allocatable {@link Resource}.
   * @return minimum allocatable resource
   */
  @Public
  @Stable
  public Resource getMinimumResourceCapability();
  
  /** 获取最大可分配资源单位
   * Get maximum allocatable {@link Resource} at the cluster level.
   * @return maximum allocatable resource
   */
  @Public
  @Stable
  public Resource getMaximumResourceCapability();

  /**获取特定队列的最大可分配资源
   * Get maximum allocatable {@link Resource} for the queue specified.
   * @param queueName queue name
   * @return maximum allocatable resource
   */
  @Public
  @Stable
  public Resource getMaximumResourceCapability(String queueName);
  //获取资源计算器
  @LimitedPrivate("yarn")
  @Evolving
  ResourceCalculator getResourceCalculator();

  /**获取集群中的节点数
   * Get the number of nodes available in the cluster.
   * @return the number of available nodes.
   */
  @Public
  @Stable
  public int getNumClusterNodes();
  
  /**
   * The main API between the ApplicationMaster and the Scheduler.
   * The ApplicationMaster may request/update container resources,
   * number of containers, node/rack preference for allocations etc.
   * to the Scheduler.
   * @param appAttemptId the id of the application attempt.
   * @param ask the request made by an application to obtain various allocations
   * like host/rack, resource, number of containers, relaxLocality etc.,
   * see {@link ResourceRequest}.
   * @param schedulingRequests similar to ask, but with added ability to specify
   * allocation tags etc., see {@link SchedulingRequest}.
   * @param release the list of containers to be released.
   * @param blacklistAdditions places (node/rack) to be added to the blacklist.
   * @param blacklistRemovals places (node/rack) to be removed from the
   * blacklist.
   * @param updateRequests container promotion/demotion updates.
   * @return the {@link Allocation} for the application.
   */
  //allocate 方法是 ApplicationMaster（AM） 与 YARN 调度器（Scheduler） 之间的主要交互接口。
  //它允许 AM 向调度器请求和更新资源，包括：
  //申请新的容器（CPU、内存等资源）
  //释放不再需要的容器
  //设定资源分配的节点/机架偏好
  //处理黑名单（不希望在哪些节点或机架上运行任务）
  //容器的晋升（promotion）和降级（demotion）
  @Public
  @Stable
  Allocation allocate(ApplicationAttemptId appAttemptId,//应用尝试 ID，标识当前 AM 代表的应用尝试（Application Attempt）
      List<ResourceRequest> ask,  //资源请求列表，描述应用所需的资源，如 CPU、内存、节点位置、任务优先级等
      List<SchedulingRequest> schedulingRequests, //调度请求列表，类似 ask，但可以指定 分配标签（allocation tags）等高级调度参数
      List<ContainerId> release, //待释放的容器 ID 列表，表示应用不再需要这些容器
      List<String> blacklistAdditions,//新增黑名单列表，指定应用不希望在这些节点或机架上运行
      List<String> blacklistRemovals,//移除黑名单列表，从黑名单中去除的节点或机架，使其重新可用
      ContainerUpdates updateRequests);//容器更新请求，用于调整已分配容器的规格，如晋升（promotion）或降级（demotion）

  /**
   * Get node resource usage report.
   *  获取指定节点的资源使用报告
   * @param nodeId nodeId.
   * @return the {@link SchedulerNodeReport} for the node or null
   * if nodeId does not point to a defined node.
   */
  @LimitedPrivate("yarn")
  @Stable
  public SchedulerNodeReport getNodeReport(NodeId nodeId);
  
  /** 根据应用尝试 ID 获取调度器应用信息
   * Get the Scheduler app for a given app attempt Id.
   * @param appAttemptId the id of the application attempt
   * @return SchedulerApp for this given attempt.
   */
  @LimitedPrivate("yarn")
  @Stable
  SchedulerAppReport getSchedulerAppInfo(ApplicationAttemptId appAttemptId);

  /** 获取与给定应用尝试 ID 相关的资源使用报告
   * Get a resource usage report from a given app attempt ID.
   * @param appAttemptId the id of the application attempt
   * @return resource usage report for this given attempt
   */
  @LimitedPrivate("yarn")
  @Evolving
  ApplicationResourceUsageReport getAppResourceUsageReport(
      ApplicationAttemptId appAttemptId);
  
  /** 获取调度器的根队列的队列指标
   * Get the root queue for the scheduler.
   * @return the root queue for the scheduler.
   */
  @LimitedPrivate("yarn")
  @Evolving
  QueueMetrics getRootQueueMetrics();

  /** 检查用户是否有权限执行某项操作
   * Check if the user has permission to perform the operation.
   * If the user has {@link QueueACL#ADMINISTER_QUEUE} permission,
   * this user can view/modify the applications in this queue.
   *
   * @param callerUGI caller UserGroupInformation.
   * @param acl queue ACL.
   * @param queueName queue Name.
   * @return <code>true</code> if the user has the permission,
   *         <code>false</code> otherwise
   */
  boolean checkAccess(UserGroupInformation callerUGI,
      QueueACL acl, String queueName);
  
  /** 获取指定队列中的应用列表
   * Gets the apps under a given queue
   * @param queueName the name of the queue.
   * @return a collection of app attempt ids in the given queue.
   */
  @LimitedPrivate("yarn")
  @Stable
  public List<ApplicationAttemptId> getAppsInQueue(String queueName);

  /** 获取指定容器的资源管理器容器（RMContainer）对象
   * Get the container for the given containerId.
   *
   * @param containerId the given containerId.
   * @return the container for the given containerId.
   */
  @LimitedPrivate("yarn")
  @Unstable
  public RMContainer getRMContainer(ContainerId containerId);

  /** 将指定应用移动到新的队列中
   * Moves the given application to the given queue.
   * @param appId application Id
   * @param newQueue the given queue.
   * @return the name of the queue the application was placed into
   * @throws YarnException if the move cannot be carried out
   */
  @LimitedPrivate("yarn")
  @Evolving
  public String moveApplication(ApplicationId appId, String newQueue)
      throws YarnException;

  /**
   * 在应用移动到目标队列之前进行预验证，确保可以进行移动
   * @param appId Application ID
   * @param newQueue Target QueueName
   * @throws YarnException if the pre-validation for move cannot be carried out
   */
  @LimitedPrivate("yarn")
  @Evolving
  public void preValidateMoveApplication(ApplicationId appId,
      String newQueue) throws YarnException;

  /** 将源队列中的所有应用移动到目标队列
   * Completely drain sourceQueue of applications, by moving all of them to
   * destQueue.
   *
   * @param sourceQueue sourceQueue.
   * @param destQueue destQueue.
   * @throws YarnException when yarn exception occur.
   */
  void moveAllApps(String sourceQueue, String destQueue) throws YarnException;

  /** 终止指定队列中的所有应用
   * Terminate all applications in the specified queue.
   *
   * @param queueName the name of queue to be drained
   * @throws YarnException when yarn exception occur.
   */
  void killAllAppsInQueue(String queueName) throws YarnException;

  /**
   * Remove an existing queue. Implementations might limit when a queue could be
   * removed (e.g., must have zero entitlement, and no applications running, or
   * must be a leaf, etc..).
   * 移除指定队列。实现可能会限制何时可以移除队列，例如队列必须没有应用在运行或队列必须是叶子队列等
   * @param queueName name of the queue to remove
   * @throws YarnException when yarn exception occur.
   */
  void removeQueue(String queueName) throws YarnException;

  /**
   * Add to the scheduler a new Queue. Implementations might limit what type of
   * queues can be dynamically added (e.g., Queue must be a leaf, must be
   * attached to existing parent, must have zero entitlement).
   * 向调度器添加一个新的队列。实现可能会限制可以动态添加哪些类型的队列，例如队列必须是叶子队列、必须附加到现有父队列、必须没有负载等
   * @param newQueue the queue being added.
   * @throws YarnException when yarn exception occur.
   * @throws IOException when io exception occur.
   */
  void addQueue(Queue newQueue) throws YarnException, IOException;

  /**
   * This method increase the entitlement for current queue (must respect
   * invariants, e.g., no overcommit of parents, non negative, etc.).
   * Entitlement is a general term for weights in FairScheduler, capacity for
   * the CapacityScheduler, etc.
   * 为当前队列设置新的资源配额（即 entitlement），例如设置 FairScheduler 中的权重或 CapacityScheduler 中的容量
   * @param queue the queue for which we change entitlement
   * @param entitlement the new entitlement for the queue (capacity,
   *              maxCapacity, etc..)
   * @throws YarnException when yarn exception occur.
   */
  void setEntitlement(String queue, QueueEntitlement entitlement)
      throws YarnException;

  /** 获取受预留系统管理的队列列表。这些队列支持预留
   * Gets the list of names for queues managed by the Reservation System.
   * @return the list of queues which support reservations
   * @throws YarnException when yarn exception occur.
   */
  public Set<String> getPlanQueues() throws YarnException;  

  /**获取调度时考虑的资源类型
   * Return a collection of the resource types that are considered when
   * scheduling
   *
   * @return an EnumSet containing the resource types
   */
  public EnumSet<SchedulerResourceTypes> getSchedulingResourceTypes();

  /**
   *
   * Verify whether a submitted application priority is valid as per configured
   * Queue
   * 验证并获取应用程序的优先级
   * @param priorityRequestedByApp
   *          Submitted Application priority.
   * @param user
   *          User who submitted the Application
   * @param queuePath
   *          Name of the Queue
   * @param applicationId
   *          Application ID
   * @return Updated Priority from scheduler
   * @throws YarnException when yarn exception occur.
   */
  public Priority checkAndGetApplicationPriority(Priority priorityRequestedByApp,
      UserGroupInformation user, String queuePath, ApplicationId applicationId)
      throws YarnException;

  /**
   * 在运行时更改已提交应用程序的优先级
   * Change application priority of a submitted application at runtime
   *
   * @param newPriority Submitted Application priority.
   *
   * @param applicationId Application ID
   *
   * @param future Sets any type of exception happened from StateStore
   * @param user who submitted the application
   *
   * @return updated priority
   * @throws YarnException when yarn exception occur.
   */
  public Priority updateApplicationPriority(Priority newPriority,
      ApplicationId applicationId, SettableFuture<Object> future,
      UserGroupInformation user) throws YarnException;

  /**
   * 获取前次尝试的活跃容器，用于工作保留的 AM 重启
   * Get previous attempts' live containers for work-preserving AM restart.
   *
   * @param appAttemptId the id of the application attempt
   *
   * @return list of live containers for the given attempt
   */
  List<Container> getTransferredContainers(ApplicationAttemptId appAttemptId);

  /**
   * Set the cluster max priority.
   *  设置集群的最大优先级
   * @param conf Configuration.
   * @throws YarnException when yarn exception occur.
   */
  void setClusterMaxPriority(Configuration conf) throws YarnException;

  /** 获取指定应用程序尝试的待处理资源请求
   * Get pending resource request for specified application attempt.
   *
   * @param attemptId the id of the application attempt
   * @return pending resource requests.
   */
  List<ResourceRequest> getPendingResourceRequestsForAttempt(
      ApplicationAttemptId attemptId);

  /** 获取指定应用程序尝试的待处理调度请求
   * Get pending scheduling request for specified application attempt.
   *
   * @param attemptId the id of the application attempt
   *
   * @return pending scheduling requests
   */
  List<SchedulingRequest> getPendingSchedulingRequestsForAttempt(
      ApplicationAttemptId attemptId);

  /** 获取集群级别的最大应用程序优先级
   * Get cluster max priority.
   * 
   * @return maximum priority of cluster
   */
  Priority getMaxClusterLevelAppPriority();

  /** 根据节点 ID 获取对应的调度器节点信息
   * Get SchedulerNode corresponds to nodeId.
   *
   * @param nodeId the node id of RMNode
   *
   * @return SchedulerNode corresponds to nodeId
   */
  SchedulerNode getSchedulerNode(NodeId nodeId);

  /**
   * Normalize a resource request using scheduler level maximum resource or
   * queue based maximum resource.
   * 根据调度器级别最大资源或队列级别最大资源，规范化资源请求
   * @param requestedResource the resource to be normalized
   * @param maxResourceCapability Maximum container allocation value, if null or
   *          empty scheduler level maximum container allocation value will be
   *          used
   * @return the normalized resource
   */
  Resource getNormalizedResource(Resource requestedResource,
      Resource maxResourceCapability);

  /** 验证并获取应用程序生命周期是否符合队列配置的生命周期
   * Verify whether a submitted application lifetime is valid as per configured
   * Queue lifetime.
   * @param queueName Name of the Queue
   * @param lifetime configured application lifetime
   * @return valid lifetime as per queue
   */
  @Public
  @Evolving
  long checkAndGetApplicationLifetime(String queueName, long lifetime);

  /** 获取队列的最大应用程序生命周期
   * Get maximum lifetime for a queue.
   * @param queueName to get lifetime
   * @return maximum lifetime in seconds
   */
  @Public
  @Evolving
  long getMaximumApplicationLifetime(String queueName);
}
