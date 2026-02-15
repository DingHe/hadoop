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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;

//监控模块 (monitor) 的一个接口，它定义了一组调度编辑策略的行为。主要作用如下：
//定期执行调度调整：该接口的实现类会在固定的时间间隔内调整调度策略，以优化资源分配。
//与 ResourceScheduler 交互：它可以跟踪容器使用情况，并通过 ResourceScheduler 影响调度行为。
//提供策略名称和监控间隔：可以获取当前策略的名称 (getPolicyName) 和执行的时间间隔 (getMonitoringInterval)。
//初始化方法：提供 init 方法用于初始化策略，实现类可根据 Configuration 进行参数配置

public interface SchedulingEditPolicy {

  void init(Configuration config, RMContext context,
      ResourceScheduler scheduler);

  /**
   * This method is invoked at regular intervals. Internally the policy is
   * allowed to track containers and affect the scheduler. The "actions"
   * performed are passed back through an EventHandler.
   */
  void editSchedule();

  long getMonitoringInterval();

  String getPolicyName();

}
