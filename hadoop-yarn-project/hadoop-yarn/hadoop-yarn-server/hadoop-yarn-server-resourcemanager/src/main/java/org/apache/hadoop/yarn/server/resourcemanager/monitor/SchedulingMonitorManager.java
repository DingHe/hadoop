/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.monitor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

//负责管理 YARN 资源管理器（RM）的调度监控器（SchedulingMonitor）。它的主要功能包括：
//初始化、重新初始化和管理调度监控器，用于控制调度策略的应用
//根据配置文件动态添加、移除或更新调度监控器，支持热更新调度策略
//提供启动、停止、查询等管理功能，确保调度监控器的状态符合当前 YARN 运行时的配置
//调度监控器通常用于监控和调整YARN资源分配策略，比如按比例预占（Preemption），以确保公平性或容量调度策略的执行。
/**
 * Manages scheduling monitors.
 */
public class SchedulingMonitorManager {
  private static final Logger LOG = LoggerFactory.getLogger(
      SchedulingMonitorManager.class);

  private Map<String, SchedulingMonitor> runningSchedulingMonitors =
      new HashMap<>();
  private RMContext rmContext;

  private void updateSchedulingMonitors(Configuration conf,
      boolean startImmediately) throws YarnException {
    boolean monitorsEnabled = conf.getBoolean(
        YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_ENABLE_MONITORS);

    if (!monitorsEnabled) {
      if (!runningSchedulingMonitors.isEmpty()) {
        // If monitors disabled while we have some running monitors, we should
        // stop them.
        LOG.info("Scheduling Monitor disabled, stopping all services");
        stopAndRemoveAll();
      }

      return;
    }

    // When monitor is enabled, loading policies
    String[] configuredPolicies = conf.getTrimmedStrings(
        YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES);
    if (configuredPolicies == null || configuredPolicies.length == 0) {
      return;
    }

    Set<String> configurePoliciesSet = new HashSet<>();
    for (String s : configuredPolicies) {
      configurePoliciesSet.add(s);
    }

    // Add new monitor when needed
    for (String s : configurePoliciesSet) {
      if (!runningSchedulingMonitors.containsKey(s)) {
        Class<?> policyClass;
        try {
          policyClass = Class.forName(s);
        } catch (ClassNotFoundException e) {
          String message = "Failed to find class of specified policy=" + s;
          LOG.warn(message);
          throw new YarnException(message);
        }

        if (SchedulingEditPolicy.class.isAssignableFrom(policyClass)) {
          SchedulingEditPolicy policyInstance =
              (SchedulingEditPolicy) ReflectionUtils.newInstance(policyClass,
                  null);
          SchedulingMonitor mon = new SchedulingMonitor(rmContext,
              policyInstance);
          mon.init(conf);
          if (startImmediately) {
            mon.start();
          }
          runningSchedulingMonitors.put(s, mon);
        } else {
          String message =
              "Specified policy=" + s + " is not a SchedulingEditPolicy class.";
          LOG.warn(message);
          throw new YarnException(message);
        }
      }
    }

    // Stop monitor when needed.
    Set<String> disabledPolicies = Sets.difference(
        runningSchedulingMonitors.keySet(), configurePoliciesSet);
    for (String disabledPolicy : disabledPolicies) {
      LOG.info("SchedulingEditPolicy=" + disabledPolicy
          + " removed, stopping it now ...");
      silentlyStopSchedulingMonitor(disabledPolicy);
      runningSchedulingMonitors.remove(disabledPolicy);
    }
  }

  public synchronized void initialize(RMContext rmContext,
      Configuration configuration) throws YarnException {
    this.rmContext = rmContext;
    stopAndRemoveAll();

    updateSchedulingMonitors(configuration, false);
  }

  public synchronized void reinitialize(RMContext rmContext,
      Configuration configuration) throws YarnException {
    this.rmContext = rmContext;

    updateSchedulingMonitors(configuration, true);
  }

  public synchronized void startAll() {
    for (SchedulingMonitor schedulingMonitor : runningSchedulingMonitors
        .values()) {
      schedulingMonitor.start();
    }
  }

  private void silentlyStopSchedulingMonitor(String name) {
    SchedulingMonitor mon = runningSchedulingMonitors.get(name);
    try {
      mon.stop();
      LOG.info("Sucessfully stopped monitor=" + mon.getName());
    } catch (Exception e) {
      LOG.warn("Exception while stopping monitor=" + mon.getName(), e);
    }
  }

  private void stopAndRemoveAll() {
    if (!runningSchedulingMonitors.isEmpty()) {
      for (String schedulingMonitorName : runningSchedulingMonitors
          .keySet()) {
        silentlyStopSchedulingMonitor(schedulingMonitorName);
      }
      runningSchedulingMonitors.clear();
    }
  }

  public boolean isRSMEmpty() {
    return runningSchedulingMonitors.isEmpty();
  }

  public boolean isSameConfiguredPolicies(Set<String> configurePoliciesSet) {
    return configurePoliciesSet.equals(runningSchedulingMonitors.keySet());
  }

  public SchedulingMonitor getAvailableSchedulingMonitor() {
    if (isRSMEmpty()) {
      return null;
    }
    for (SchedulingMonitor smon : runningSchedulingMonitors.values()) {
      if (smon.getSchedulingEditPolicy()
          instanceof ProportionalCapacityPreemptionPolicy) {
        return smon;
      }
    }
    return null;
  }

  public synchronized void stop() throws YarnException {
    stopAndRemoveAll();
  }
}
