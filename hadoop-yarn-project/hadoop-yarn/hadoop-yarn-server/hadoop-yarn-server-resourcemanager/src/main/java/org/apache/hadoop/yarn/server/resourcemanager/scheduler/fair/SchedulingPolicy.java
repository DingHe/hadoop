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

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.DominantResourceFairnessPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.FairSharePolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.FifoPolicy;


import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The SchedulingPolicy is used by the fair scheduler mainly to determine
 * what a queue's fair share and steady fair share should be as well as
 * calculating available headroom. This determines how resources can be
 * shared between running applications within a queue.
 * <p>
 * Every queue has a policy, including parents and children. If a child
 * queue doesn't specify one, it inherits the parent's policy.
 * The policy for a child queue must be compatible with the policy of
 * the parent queue; there are some combinations that aren't allowed.
 * See {@link SchedulingPolicy#isChildPolicyAllowed(SchedulingPolicy)}.
 * The policy for a queue is specified by setting property
 * <i>schedulingPolicy</i> in the fair scheduler configuration file.
 * The default policy is {@link FairSharePolicy} if not specified.
 */
//SchedulingPolicy 类是 Apache Hadoop YARN 中公平调度器（Fair Scheduler）的核心类之一，
// 主要用于决定队列的公平资源分配、计算可用资源以及为调度决策提供依据。
// 它定义了资源调度的策略，每个队列都有一个与之关联的调度策略，包括父队列和子队列。
// 如果子队列未指定策略，则继承父队列的策略。
// 该类通过方法计算队列的资源配额、调度排序、头部资源等，确保多个应用程序能够公平共享集群资源
@Public
@Evolving
public abstract class SchedulingPolicy {
  //用于存储不同类型调度策略的实例。通过这个缓存，可以确保每种调度策略类只有一个实例，避免重复创建
  private static final ConcurrentHashMap<Class<? extends SchedulingPolicy>, SchedulingPolicy> instances =
      new ConcurrentHashMap<Class<? extends SchedulingPolicy>, SchedulingPolicy>();
  //默认的调度策略，默认为 FairSharePolicy 类的实例。如果没有在配置文件中指定其他调度策略，将使用此策略
  public static final SchedulingPolicy DEFAULT_POLICY =
      getInstance(FairSharePolicy.class);

  /**
   * Returns a {@link SchedulingPolicy} instance corresponding
   * to the passed clazz.
   *
   * @param clazz a class that extends {@link SchedulingPolicy}
   * @return a {@link SchedulingPolicy} instance
   */
  public static SchedulingPolicy getInstance(
      Class<? extends SchedulingPolicy> clazz) {
    SchedulingPolicy policy = ReflectionUtils.newInstance(clazz, null);
    SchedulingPolicy policyRet = instances.putIfAbsent(clazz, policy);
    if(policyRet != null) {
      return policyRet;
    }
    return policy;
  }

  /**
   * Returns {@link SchedulingPolicy} instance corresponding to the
   * {@link SchedulingPolicy} passed as a string. The policy can be "fair" for
   * FairSharePolicy, "fifo" for FifoPolicy, or "drf" for
   * DominantResourceFairnessPolicy. For a custom
   * {@link SchedulingPolicy}s in the RM classpath, the policy should be
   * canonical class name of the {@link SchedulingPolicy}.
   * 
   * @param policy canonical class name or "drf" or "fair" or "fifo"
   * @return a {@link SchedulingPolicy} instance parsed from given policy
   * @throws AllocationConfigurationException for any errors.
   *
   */
  //policy 是一个字符串，表示要解析的调度策略。它可以是 "fair"、"fifo"、"drf" 等，或者是自定义调度策略的全类名
  @SuppressWarnings("unchecked")
  public static SchedulingPolicy parse(String policy)
      throws AllocationConfigurationException {
    @SuppressWarnings("rawtypes")
    Class clazz;
    String text = StringUtils.toLowerCase(policy);
    if (text.equalsIgnoreCase(FairSharePolicy.NAME)) {
      clazz = FairSharePolicy.class;
    } else if (text.equalsIgnoreCase(FifoPolicy.NAME)) {
      clazz = FifoPolicy.class;
    } else if (text.equalsIgnoreCase(DominantResourceFairnessPolicy.NAME)) {
      clazz = DominantResourceFairnessPolicy.class;
    } else {
      try {
        clazz = Class.forName(policy);
      } catch (ClassNotFoundException cnfe) {
        throw new AllocationConfigurationException(policy
            + " SchedulingPolicy class not found!");
      }
    }
    if (!SchedulingPolicy.class.isAssignableFrom(clazz)) {
      throw new AllocationConfigurationException(policy
          + " does not extend SchedulingPolicy");
    }
    return getInstance(clazz);
  }

  /**
   * Initialize the scheduling policy with cluster resources.
   * @deprecated Since it doesn't track cluster resource changes, replaced by
   * {@link #initialize(FSContext)}.
   *
   * @param clusterCapacity cluster resources
   */
  //clusterCapacity 是集群的资源容量（如 CPU、内存等
  @Deprecated
  public void initialize(Resource clusterCapacity) {}

  /**
   * Initialize the scheduling policy with a {@link FSContext} object, which has
   * a pointer to the cluster resources among other information.
   *
   * @param fsContext a {@link FSContext} object which has a pointer to the
   *                  cluster resources
   */
  public void initialize(FSContext fsContext) {}

  /**
   * The {@link ResourceCalculator} returned by this method should be used
   * for any calculations involving resources.
   *
   * @return ResourceCalculator instance to use
   */
  public abstract ResourceCalculator getResourceCalculator();

  /**
   * @return returns the name of {@link SchedulingPolicy}
   */
  public abstract String getName();

  /**
   * The comparator returned by this method is to be used for sorting the
   * {@link Schedulable}s in that queue.
   * 
   * @return the comparator to sort by
   */
  public abstract Comparator<Schedulable> getComparator();

  /**
   * Computes and updates the shares of {@link Schedulable}s as per
   * the {@link SchedulingPolicy}, to be used later for scheduling decisions.
   * The shares computed are instantaneous and only consider queues with
   * running applications.
   * 
   * @param schedulables {@link Schedulable}s whose shares are to be updated
   * @param totalResources Total {@link Resource}s in the cluster
   */
  //schedulables 是一个包含多个 Schedulable 对象的集合，表示需要计算资源配额的对象；
  // totalResources 是集群中的总资源
  //该方法计算并更新每个可调度对象（如队列、任务）的资源配额。配额是瞬时的，仅考虑当前正在运行的应用程序
  public abstract void computeShares(
      Collection<? extends Schedulable> schedulables, Resource totalResources);

  /**
   * Computes and updates the steady shares of {@link FSQueue}s as per the
   * {@link SchedulingPolicy}. The steady share does not differentiate
   * between queues with and without running applications under them. The
   * steady share is not used for scheduling, it is displayed on the Web UI
   * for better visibility.
   *
   * @param queues {@link FSQueue}s whose shares are to be updated
   * @param totalResources Total {@link Resource}s in the cluster
   */
  //queues 是一个包含多个 FSQueue 对象的集合，表示需要计算的队列；
  // totalResources 是集群中的总资源
  // 该方法计算并更新每个队列的稳定资源配额。
  // 稳定配额不区分队列下是否有正在运行的应用程序，它主要用于显示在 Web UI 上，以便查看资源分配情况
  public abstract void computeSteadyShares(
      Collection<? extends FSQueue> queues, Resource totalResources);

  /**
   * Check if the resource usage is over the fair share under this policy.
   *
   * @param usage {@link Resource} the resource usage
   * @param fairShare {@link Resource} the fair share
   * @return true if check passes (is over) or false otherwise
   */
  //usage 是队列使用的资源量；
  // fairShare 是队列的公平配额
  //检查队列的资源使用量是否超出了其公平配额。如果超出，返回 true，否则返回 false
  public abstract boolean checkIfUsageOverFairShare(
      Resource usage, Resource fairShare);

  /**
   * Get headroom by calculating the min of {@code clusterAvailable} and
   * ({@code queueFairShare} - {@code queueUsage}) resources that are
   * applicable to this policy. For eg if only memory then leave other
   * resources such as CPU to same as {@code clusterAvailable}.
   *
   * @param queueFairShare fairshare in the queue
   * @param queueUsage resources used in the queue
   * @param maxAvailable available resource in cluster for this queue
   * @return calculated headroom
   */
  //queueFairShare 是队列的公平配额；
  // queueUsage 是队列当前使用的资源量；
  // maxAvailable 是集群中该队列可用的最大资源
  //该方法计算队列的头部资源（即可以分配的资源）。
  // 计算方式是取 queueFairShare - queueUsage 和 maxAvailable 的最小值，表示队列还可以使用多少资源
  public abstract Resource getHeadroom(Resource queueFairShare,
      Resource queueUsage, Resource maxAvailable);

  /**
   * Check whether the policy of a child queue is allowed.
   *
   * @param childPolicy the policy of child queue
   * @return true if the child policy is allowed; false otherwise
   */
  //childPolicy 是子队列的调度策略
  //检查子队列的策略是否被允许。默认情况下，子队列的策略总是允许（返回 true）。但某些策略组合可能不被允许，具体实现可以在子类中覆盖此方法
  public boolean isChildPolicyAllowed(SchedulingPolicy childPolicy) {
    return true;
  }
}
