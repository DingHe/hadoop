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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.Schedulable;

import static java.lang.Math.addExact;

/**
 * Contains logic for computing the fair shares. A {@link Schedulable}'s fair
 * share is {@link Resource} it is entitled to, independent of the current
 * demands and allocations on the cluster. A {@link Schedulable} whose resource
 * consumption lies at or below its fair share will never have its containers
 * preempted.
 */
public final class ComputeFairShares {
  
  private static final int COMPUTE_FAIR_SHARES_ITERATIONS = 25;

  private ComputeFairShares() {
  }

  /**
   * Compute fair share of the given schedulables.Fair share is an allocation of
   * shares considering only active schedulables ie schedulables which have
   * running apps.
   * 
   * @param schedulables given schedulables.
   * @param totalResources totalResources.
   * @param type type of the resource.
   */
  public static void computeShares(
      Collection<? extends Schedulable> schedulables, Resource totalResources,
      String type) {
    computeSharesInternal(schedulables, totalResources, type, false);
  }

  /**
   * Compute the steady fair share of the given queues. The steady fair
   * share is an allocation of shares considering all queues, i.e.,
   * active and inactive.
   *
   * @param queues {@link FSQueue}s whose shares are to be updated.
   * @param totalResources totalResources.
   * @param type type of the resource.
   */
  public static void computeSteadyShares(
      Collection<? extends FSQueue> queues, Resource totalResources,
      String type) {
    computeSharesInternal(queues, totalResources, type, true);
  }

  /**
   * Given a set of Schedulables and a number of slots, compute their weighted
   * fair shares. The min and max shares and of the Schedulables are assumed to
   * be set beforehand. We compute the fairest possible allocation of shares to
   * the Schedulables that respects their min and max shares.
   * <p>
   * To understand what this method does, we must first define what weighted
   * fair sharing means in the presence of min and max shares. If there
   * were no minimum or maximum shares, then weighted fair sharing would be
   * achieved if the ratio of slotsAssigned / weight was equal for each
   * Schedulable and all slots were assigned. Minimum and maximum shares add a
   * further twist - Some Schedulables may have a min share higher than their
   * assigned share or a max share lower than their assigned share.
   * <p>
   * To deal with these possibilities, we define an assignment of slots as being
   * fair if there exists a ratio R such that: Schedulables S where S.minShare
   * {@literal >} R * S.weight are given share S.minShare - Schedulables S
   * where S.maxShare {@literal <} R * S.weight are given S.maxShare -
   * All other Schedulables S are assigned share R * S.weight -
   * The sum of all the shares is totalSlots.
   * <p>
   * We call R the weight-to-slots ratio because it converts a Schedulable's
   * weight to the number of slots it is assigned.
   * <p>
   * We compute a fair allocation by finding a suitable weight-to-slot ratio R.
   * To do this, we use binary search. Given a ratio R, we compute the number of
   * slots that would be used in total with this ratio (the sum of the shares
   * computed using the conditions above). If this number of slots is less than
   * totalSlots, then R is too small and more slots could be assigned. If the
   * number of slots is more than totalSlots, then R is too large.
   * <p>
   * We begin the binary search with a lower bound on R of 0 (which means that
   * all Schedulables are only given their minShare) and an upper bound computed
   * to be large enough that too many slots are given (by doubling R until we
   * use more than totalResources resources). The helper method
   * resourceUsedWithWeightToResourceRatio computes the total resources used
   * with a given value of R.
   * <p>
   * The running time of this algorithm is linear in the number of Schedulables,
   * because resourceUsedWithWeightToResourceRatio is linear-time and the
   * number of iterations of binary search is a constant (dependent on desired
   * precision).
   */
  // 用于计算 Schedulable 任务的公平资源分配。
  // 它的主要目标是在满足任务的最小（minShare）和最大（maxShare）资源限制的前提下，基于权重实现公平分配。
  // 当没有 minShare 和 maxShare 限制时，公平分配意味着所有 Schedulable 的 (分配的资源 / 权重) 之比相等。
  // 该方法通过二分查找来确定合适的权重到资源比率 (weight-to-resource ratio, R)，以确保所有任务的资源分配既公平又符合约束
  private static void computeSharesInternal(
      Collection<? extends Schedulable> allSchedulables,
      Resource totalResources, String type, boolean isSteadyShare) {

    Collection<Schedulable> schedulables = new ArrayList<>(); //非固定份额调度实体
    //筛选出需要进行公平计算的任务，并返回固定份额的资源总量
    long takenResources = handleFixedFairShares(
        allSchedulables, schedulables, isSteadyShare, type);

    if (schedulables.isEmpty()) {
      return;
    }
    // Find an upper bound on R that we can use in our binary search. We start
    // at R = 1 and double it until we have either used all the resources or we
    // have met all Schedulables' max shares.
    //计算 schedulables 任务集合中，所有任务 maxShare 的总和，确保分配不会超过任务的最大限制
    long totalMaxShare = 0;
    for (Schedulable sched : schedulables) {
      long maxShare = sched.getMaxShare().getResourceValue(type);
      totalMaxShare = safeAdd(maxShare, totalMaxShare);
      if (totalMaxShare == Long.MAX_VALUE) {
        break;
      }
    }
    //计算可用于公平分配的 totalResource
    long totalResource = Math.max((totalResources.getResourceValue(type) -
        takenResources), 0);
    totalResource = Math.min(totalMaxShare, totalResource);
    //计算二分查找的上界rMax
    double rMax = 1.0;
    while (resourceUsedWithWeightToResourceRatio(rMax, schedulables, type)
        < totalResource) {
      rMax *= 2.0;
    }
    // Perform the binary search for up to COMPUTE_FAIR_SHARES_ITERATIONS steps
    //采用 二分查找 方式寻找最合适的 R
    double left = 0;
    double right = rMax;
    for (int i = 0; i < COMPUTE_FAIR_SHARES_ITERATIONS; i++) {
      double mid = (left + right) / 2.0;
      long plannedResourceUsed = resourceUsedWithWeightToResourceRatio(
          mid, schedulables, type);
      if (plannedResourceUsed == totalResource) {
        right = mid;
        break;
      } else if (plannedResourceUsed < totalResource) {
        left = mid;
      } else {
        right = mid;
      }
    }
    //计算并设置最终的公平分配
    // Set the fair shares based on the value of R we've converged to
    for (Schedulable sched : schedulables) {
      Resource target;

      if (isSteadyShare) {
        target = ((FSQueue) sched).getSteadyFairShare();
      } else {
        target = sched.getFairShare();
      }

      target.setResourceValue(type, computeShare(sched, right, type));
    }
  }

  /**
   * Compute the resources that would be used given a weight-to-resource ratio
   * w2rRatio, for use in the computeFairShares algorithm as described in
   * {@link #computeSharesInternal}.
   */
  //计算在给定的权重到资源比率 (w2rRatio) 下，所有 Schedulable 任务的 总资源使用量
  //w2rRatio	权重到资源的比率 (weight-to-resource ratio)，决定如何将 Schedulable 的 weight 转换为资源
  //schedulables	需要计算资源分配的 Schedulable 任务集合
  //type	资源类型（如 CPU、内存等）
  private static long resourceUsedWithWeightToResourceRatio(double w2rRatio,
      Collection<? extends Schedulable> schedulables, String type) {
    long resourcesTaken = 0;
    for (Schedulable sched : schedulables) {
      //计算当前 sched 在 w2rRatio 下应该分配的资源 (computeShare)
      long share = computeShare(sched, w2rRatio, type);
      resourcesTaken = safeAdd(resourcesTaken, share);
      if (resourcesTaken == Long.MAX_VALUE) {
        break;
      }
    }
    return resourcesTaken;
  }

  /**
   * Compute the resources assigned to a Schedulable given a particular
   * weight-to-resource ratio w2rRatio.
   */
  //计算单个 Schedulable 在 w2rRatio 下的公平份额
  //确保计算出的资源分配量不会低于 minShare，不会超过 maxShare
  //w2rRatio 是当前二分搜索的 R 值
  private static long computeShare(Schedulable sched, double w2rRatio,
      String type) {
    double share = sched.getWeight() * w2rRatio;
    share = Math.max(share, sched.getMinShare().getResourceValue(type));
    share = Math.min(share, sched.getMaxShare().getResourceValue(type));
    return (long) share;
  }

  /**
   * Helper method to handle Schedulabes with fixed fairshares.
   * Returns the resources taken by fixed fairshare schedulables,
   * and adds the remaining to the passed nonFixedSchedulables.
   */
  //处理具有固定公平份额（fixed fair share） 的 Schedulable 任务，并计算它们已占用的资源总量
  //将非固定份额 (fixedShare < 0) 的 Schedulable 任务添加到 nonFixedSchedulables 列表，以便后续的公平份额计算
  //返回已占用的资源总量 totalResource
  private static long handleFixedFairShares(
      Collection<? extends Schedulable> schedulables,// 所有待调度任务
      Collection<Schedulable> nonFixedSchedulables, // 非固定公平份额的任务集合
      boolean isSteadyShare, String type) {
    long totalResource = 0;  // 用于记录 具有固定公平份额的任务所占用的总资源量

    for (Schedulable sched : schedulables) {
      //判断该任务是否有固定公平份额
      long fixedShare = getFairShareIfFixed(sched, isSteadyShare, type);
      if (fixedShare < 0) {
        //如果 fixedShare < 0，表示该任务没有固定公平份额，后续需要进行公平分配
        nonFixedSchedulables.add(sched);
      } else {
        //如果 fixedShare >= 0，表示该任务具有固定公平份额，其资源值等于 fixedShare
        Resource target;

        if (isSteadyShare) {
          //获取 sched 的 稳定公平份额
          target = ((FSQueue)sched).getSteadyFairShare();
        } else {
          //获取 sched 的 当前公平份额 (getFairShare())
          target = sched.getFairShare();
        }

        target.setResourceValue(type, fixedShare);
        totalResource = safeAdd(totalResource, fixedShare);
      }
    }
    //返回已分配的固定公平份额资源总量
    return totalResource;
  }

  /**
   * Get the fairshare for the {@link Schedulable} if it is fixed,
   * -1 otherwise.
   *
   * The fairshare is fixed if either the maxShare is 0, weight is 0,
   * or the Schedulable is not active for instantaneous fairshare.
   */
  //用于判断某个Schedulable任务是否具有固定公平份额 (fixed fair share)
  //sched	需要检查的 Schedulable 任务
  //isSteadyShare	是否计算 稳定公平份额 (steady fair share)，如果为 false，则计算 瞬时公平份额 (instantaneous fair share)
  //type	资源类型（如 CPU、内存）
  private static long getFairShareIfFixed(Schedulable sched,
      boolean isSteadyShare, String type) {

    // Check if maxShare is 0
    //如果sched任务的最大公平份额 (maxShare) 为 0，表示该任务不能分配任何资源，直接返回0，即固定公平份额为0
    if (sched.getMaxShare().getResourceValue(type) <= 0) {
      return 0;
    }

    // For instantaneous fairshares, check if queue is active
    //计算瞬时公平份额时，
    //如果sched是 FSQueue（公平调度的队列）类型的队列，并且 不活跃 (isActive() == false)，说明它不应该获得任何资源，直接返回 0
    if (!isSteadyShare &&
        (sched instanceof FSQueue) && !((FSQueue)sched).isActive()) {
      return 0;
    }

    // Check if weight is 0
    if (sched.getWeight() <= 0) {
      //任务没有分配权重，通常意味着它不会参与公平资源计算
      //但如果它的 最小公平份额 (minShare) > 0，则返回 minShare，否则返回 0
      long minShare = sched.getMinShare().getResourceValue(type);
      return (minShare <= 0) ? 0 : minShare;
    }
    //如果以上所有条件都 不满足，说明该 sched 没有固定公平份额，需要参与公平分配计算，返回 -1
    return -1;
  }

  /**
   * Safely add two long values. The result will always be a valid long value.
   * If the addition caused an overflow the return value will be set to
   * <code>Long.MAX_VALUE</code>.
   * @param a first long to add
   * @param b second long to add
   * @return result of the addition
   */
  private static long safeAdd(long a, long b) {
    try {
      return addExact(a, b);
    } catch (ArithmeticException ae) {
      return Long.MAX_VALUE;
    }
  }
}
