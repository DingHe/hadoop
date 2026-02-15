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

import java.io.Serializable;
import java.util.Collection;
import java.util.Comparator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.Schedulable;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.SchedulingPolicy;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * Makes scheduling decisions by trying to equalize shares of memory.
 */
//FairSharePolicy 类是 Apache YARN 中的一个调度策略类，用于实现内存资源的公平调度。
// 它的主要目标是使得调度系统能够尽量公平地分配内存资源给不同的作业或队列，从而避免某些作业占用过多资源导致其他作业饥饿。
// 这个类通过一系列的比较逻辑来决定哪些作业应当优先得到资源，并通过权重、资源需求和公平资源使用情况来进行调度
@Private
@Unstable
public class FairSharePolicy extends SchedulingPolicy {
  @VisibleForTesting
  public static final String NAME = "fair";
  private static final Logger LOG =
      LoggerFactory.getLogger(FairSharePolicy.class);
  //表示内存资源的名称
  private static final String MEMORY = ResourceInformation.MEMORY_MB.getName();
  //一个资源计算器，用于计算资源的使用情况，特别是内存资源的使用
  private static final DefaultResourceCalculator RESOURCE_CALCULATOR =
      new DefaultResourceCalculator();
  //一个比较器 (FairShareComparator)，用于根据公平共享的原则对可调度的任务（Schedulable）进行排序
  private static final FairShareComparator COMPARATOR =
          new FairShareComparator();

  @Override
  public String getName() {
    return NAME;
  }

  /**
   * Compare Schedulables mainly via fair share usage to meet fairness.
   * Specifically, it goes through following four steps.
   *
   * 1. Compare demands. Schedulables without resource demand get lower priority
   * than ones who have demands.
   * 
   * 2. Compare min share usage. Schedulables below their min share are compared
   * by how far below it they are as a ratio. For example, if job A has 8 out
   * of a min share of 10 tasks and job B has 50 out of a min share of 100,
   * then job B is scheduled next, because B is at 50% of its min share and A
   * is at 80% of its min share.
   * 
   * 3. Compare fair share usage. Schedulables above their min share are
   * compared by fair share usage by checking (resource usage / weight).
   * If all weights are equal, slots are given to the job with the fewest tasks;
   * otherwise, jobs with more weight get proportionally more slots. If weight
   * equals to 0, we can't compare Schedulables by (resource usage/weight).
   * There are two situations: 1)All weights equal to 0, slots are given
   * to one with less resource usage. 2)Only one of weight equals to 0, slots
   * are given to the one with non-zero weight.
   *
   * 4. Break the tie by compare submit time and job name.
   */
  //目标是通过公平分享的资源使用情况对 Schedulable（可以调度的任务或作业）进行排序。它通过以下四个步骤来实现公平调度：
  //比较需求：没有资源需求的任务优先级较低。
  //比较最小共享资源的使用情况：低于最小共享资源的任务按差距比例进行比较，差距小的任务优先。
  //比较公平共享资源的使用情况：对于已超过最小共享资源的任务，比较资源使用量与权重的比例，权重较大的任务按比例分配更多资源。
  //通过提交时间和作业名称打破平局：如果仍然平局，通过提交时间和作业名称来决定优先级。
  private static class FairShareComparator implements Comparator<Schedulable>,
      Serializable {
    private static final long serialVersionUID = 5564969375856699313L;

    @Override
    public int compare(Schedulable s1, Schedulable s2) {
      //1、比较需求：没有资源需求的任务优先级较低。
      int res = compareDemand(s1, s2);

      // Share resource usages to avoid duplicate calculation
      Resource resourceUsage1 = null;
      Resource resourceUsage2 = null;
      //2、如果资源需求相同，则通过 compareMinShareUsage 方法比较最小共享资源的使用情况。资源使用低于最小共享的任务优先级较高
      if (res == 0) {
        resourceUsage1 = s1.getResourceUsage();
        resourceUsage2 = s2.getResourceUsage();
        res = compareMinShareUsage(s1, s2, resourceUsage1, resourceUsage2);
      }
     //3、如果最小共享资源使用相同，则比较公平共享资源的使用情况，使用权重进行排序
      if (res == 0) {
        res = compareFairShareUsage(s1, s2, resourceUsage1, resourceUsage2);
      }

      // Break the tie by submit time
      //如果仍然平局，通过任务的提交时间和作业名称进行最终排序
      if (res == 0) {
        res = (int) Math.signum(s1.getStartTime() - s2.getStartTime());
      }

      // Break the tie by job name
      if (res == 0) {
        res = s1.getName().compareTo(s2.getName());
      }

      return res;
    }
    //比较两个任务的资源需求
    private int compareDemand(Schedulable s1, Schedulable s2) {
      int res = 0;
      long demand1 = s1.getDemand().getMemorySize();
      long demand2 = s2.getDemand().getMemorySize();
      //如果 s1 没有资源需求而 s2 有需求，则 s1 的优先级较低（返回 1）
      if ((demand1 == 0) && (demand2 > 0)) {
        res = 1;
        //如果 s2 没有资源需求而 s1 有需求，则 s1 的优先级较高（返回 -1）
      } else if ((demand2 == 0) && (demand1 > 0)) {
        res = -1;
      }
      //如果两者都有或没有需求，则返回 0，表示需求相同
      return res;
    }
    //比较两个任务在最小共享资源下的使用情况
    private int compareMinShareUsage(Schedulable s1, Schedulable s2,
        Resource resourceUsage1, Resource resourceUsage2) {
      int res;
      //minShare1 和 minShare2 是任务 s1 和 s2 最小共享内存和需求内存的较小值
      long minShare1 = Math.min(s1.getMinShare().getMemorySize(),
          s1.getDemand().getMemorySize());
      long minShare2 = Math.min(s2.getMinShare().getMemorySize(),
          s2.getDemand().getMemorySize());
      //s1和s2资源使用是否低于最小共享内存
      boolean s1Needy = resourceUsage1.getMemorySize() < minShare1;
      boolean s2Needy = resourceUsage2.getMemorySize() < minShare2;
      //如果 s1 的资源使用低于其最小共享资源，而 s2 的资源使用不低于其最小共享资源，则 s1 的优先级较高
      if (s1Needy && !s2Needy) {
        res = -1;
        //反之，亦然
      } else if (s2Needy && !s1Needy) {
        res = 1;
      } else if (s1Needy && s2Needy) {
        //如果两个任务都低于最小共享资源，比较它们的资源使用与最小共享资源的比例，比例低的任务优先级较高
        double minShareRatio1 = (double) resourceUsage1.getMemorySize();
        double minShareRatio2 = (double) resourceUsage2.getMemorySize();

        if (minShare1 > 1) {
          minShareRatio1 /= minShare1;
        }

        if (minShare2 > 1) {
          minShareRatio2 /= minShare2;
        }

        res = (int) Math.signum(minShareRatio1 - minShareRatio2);
      } else {
        //如果两个任务都不低于最小共享资源，则返回 0
        res = 0;
      }

      return res;
    }

    /**
     * To simplify computation, use weights instead of fair shares to calculate
     * fair share usage.
     */
    //比较两个任务的公平共享资源使用情况
    private int compareFairShareUsage(Schedulable s1, Schedulable s2,
        Resource resourceUsage1, Resource resourceUsage2) {
      //使用任务的权重来简化计算，weight1 和 weight2 是任务 s1 和 s2 的权重
      double weight1 = s1.getWeight();
      double weight2 = s2.getWeight();
      double useToWeightRatio1;
      double useToWeightRatio2;
      //如果两个任务的权重都大于 0，比较资源使用量与权重的比例（useToWeightRatio）
      if (weight1 > 0.0 && weight2 > 0.0) {
        useToWeightRatio1 = resourceUsage1.getMemorySize() / weight1;
        useToWeightRatio2 = resourceUsage2.getMemorySize() / weight2;
      } else if (weight1 == weight2) { // Either weight1 or weight2 equals to 0
        // If they have same weight, just compare usage
        //如果权重相同，则直接比较资源使用量
        useToWeightRatio1 = resourceUsage1.getMemorySize();
        useToWeightRatio2 = resourceUsage2.getMemorySize();
      } else {
        // By setting useToWeightRatios to negative weights, we give the
        // zero-weight one less priority, so the non-zero weight one will
        // be given slots.
        //如果有任务的权重为 0，则给没有权重的任务较低的优先级，确保有权重的任务优先获得资源
        useToWeightRatio1 = -weight1;
        useToWeightRatio2 = -weight2;
      }

      return (int) Math.signum(useToWeightRatio1 - useToWeightRatio2);
    }
  }

  @Override
  public Comparator<Schedulable> getComparator() {
    return COMPARATOR;
  }

  @Override
  public ResourceCalculator getResourceCalculator() {
    return RESOURCE_CALCULATOR;
  }
  //作用是计算一个队列的头部资源（headroom）。
  // 头部资源是指当前队列可以分配的资源量，
  // 通常用于调度决策中，表示队列在公平调度下，还能获得多少资源
  // 通过计算队列的剩余资源（可用内存）与系统的可用资源之间的关系来实现这一目标。
  // 该方法确保队列不会分配超过系统实际可用资源的内存，同时也考虑了队列在公平共享策略下应该获得的资源量
  @Override
  public Resource getHeadroom(Resource queueFairShare,
                              Resource queueUsage, Resource maxAvailable) {
    long queueAvailableMemory = Math.max(
        queueFairShare.getMemorySize() - queueUsage.getMemorySize(), 0);
    Resource headroom = Resources.createResource(
        Math.min(maxAvailable.getMemorySize(), queueAvailableMemory),
        maxAvailable.getVirtualCores());
    return headroom;
  }
  //计算可调度对象（Schedulable）的公平共享资源
  //总资源扣除固定份额的调度实体，然后按照权重分配给非固定份额的调度实体，主要是分配内存资源。
  @Override
  public void computeShares(Collection<? extends Schedulable> schedulables,
      Resource totalResources) {
    ComputeFairShares.computeShares(schedulables, totalResources, MEMORY);
  }
  //计算 长期稳定的公平共享资源（Steady Fair Share），即 各队列在长期运行中应当分配的资源。
  //queues：可调度的 公平调度队列集合（FSQueue）
  //totalResources： 可用的总资源（Resource）
  @Override
  public void computeSteadyShares(Collection<? extends FSQueue> queues,
      Resource totalResources) {
    ComputeFairShares.computeSteadyShares(queues, totalResources, MEMORY);
  }

  @Override
  public boolean checkIfUsageOverFairShare(Resource usage, Resource fairShare) {
    return usage.getMemorySize() > fairShare.getMemorySize();
  }

  @Override
  public boolean isChildPolicyAllowed(SchedulingPolicy childPolicy) {
    if (childPolicy instanceof DominantResourceFairnessPolicy) {
      LOG.error("Queue policy can't be " + DominantResourceFairnessPolicy.NAME
          + " if the parent policy is " + getName() + ". Choose " +
          getName() + " or " + FifoPolicy.NAME + " for child queues instead."
          + " Please note that " + FifoPolicy.NAME
          + " is only for leaf queues.");
      return false;
    }
    return true;
  }
}
