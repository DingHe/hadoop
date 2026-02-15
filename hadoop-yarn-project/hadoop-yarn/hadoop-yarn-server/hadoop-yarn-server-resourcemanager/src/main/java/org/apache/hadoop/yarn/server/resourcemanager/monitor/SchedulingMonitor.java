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
package org.apache.hadoop.yarn.server.resourcemanager.monitor;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;

import org.apache.hadoop.classification.VisibleForTesting;

// SchedulingMonitor 是 YARN 资源管理器（ResourceManager, RM）中的一个调度监视器，
// 它的主要功能是定期执行调度策略（SchedulingEditPolicy），以便动态调整资源分配策略，如抢占（Preemption）、预留（Reservation）等。
// 具体而言，它的作用包括：
// 初始化和管理调度策略，确保调度策略在 YARN 运行期间能够正确执行。
// 定期执行调度检查（PreemptionChecker），在指定的时间间隔内执行 SchedulingEditPolicy 的 editSchedule() 方法。
// 支持动态调整调度监控的执行频率，如果调度策略的监控间隔发生变化，则自动调整执行频率。
// 通过 ScheduledExecutorService 实现周期性任务调度，确保调度策略能够在后台持续运行
public class SchedulingMonitor extends AbstractService {
  //调度策略实例，由外部传入，实现了 SchedulingEditPolicy 接口
  private final SchedulingEditPolicy scheduleEditPolicy;
  private static final Logger LOG =
      LoggerFactory.getLogger(SchedulingMonitor.class);

  // ScheduledExecutorService which schedules the PreemptionChecker to run
  // periodically.
  // 任务调度线程池，用于周期性地执行调度检查任务
  private ScheduledExecutorService ses;
  private ScheduledFuture<?> handler;
  //表示调度监控器是否已停止，用于防止在已停止的状态下重复启动
  private volatile boolean stopped;
  //监控时间间隔，即多久执行一次调度策略。
  private long monitorInterval;
  private RMContext rmContext;

  public SchedulingMonitor(RMContext rmContext,
      SchedulingEditPolicy scheduleEditPolicy) {
    super("SchedulingMonitor (" + scheduleEditPolicy.getPolicyName() + ")");
    this.scheduleEditPolicy = scheduleEditPolicy;
    this.rmContext = rmContext;
  }

  @VisibleForTesting
  public synchronized SchedulingEditPolicy getSchedulingEditPolicy() {
    return scheduleEditPolicy;
  }

  public void serviceInit(Configuration conf) throws Exception {
    LOG.info("Initializing SchedulingMonitor=" + getName());
    scheduleEditPolicy.init(conf, rmContext, rmContext.getScheduler());
    this.monitorInterval = scheduleEditPolicy.getMonitoringInterval();
    super.serviceInit(conf);
  }

  @Override
  public void serviceStart() throws Exception {
    LOG.info("Starting SchedulingMonitor=" + getName());
    assert !stopped : "starting when already stopped";
    ses = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
      public Thread newThread(Runnable r) {
        Thread t = new Thread(r);
        t.setName(getName());
        return t;
      }
    });
    schedulePreemptionChecker();
    super.serviceStart();
  }

  private void schedulePreemptionChecker() {
    handler = ses.scheduleAtFixedRate(new PolicyInvoker(),
        0, monitorInterval, TimeUnit.MILLISECONDS);
  }

  @Override
  public void serviceStop() throws Exception {
    stopped = true;
    if (handler != null) {
      LOG.info("Stop " + getName());
      handler.cancel(true);
      ses.shutdown();
    }
    super.serviceStop();
  }

  @VisibleForTesting
  public void invokePolicy(){
    scheduleEditPolicy.editSchedule();
  }

  private class PolicyInvoker implements Runnable {
    @Override
    public void run() {
      try {
        if (monitorInterval != scheduleEditPolicy.getMonitoringInterval()) {
          handler.cancel(true);
          monitorInterval = scheduleEditPolicy.getMonitoringInterval();
          schedulePreemptionChecker();
        } else {
          invokePolicy();
        }
      } catch (Throwable t) {
        // The preemption monitor does not alter structures nor do structures
        // persist across invocations. Therefore, log, skip, and retry.
        LOG.error("Exception raised while executing preemption"
            + " checker, skip this run..., exception=", t);
      }
    }
  }
}
