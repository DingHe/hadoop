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

package org.apache.hadoop.yarn.server.resourcemanager.rmcontainer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerExpiredSchedulerEvent;
import org.apache.hadoop.yarn.util.AbstractLivelinessMonitor;
//活跃性监控器，用于监控分配但未被使用的容器（Container）的超时
//在 YARN 资源管理调度过程中，一个容器可能会被分配给应用程序，但如果应用程序长时间未使用该容器，则该容器可能会被回收
@SuppressWarnings({"unchecked", "rawtypes"})
public class ContainerAllocationExpirer extends
    AbstractLivelinessMonitor<AllocationExpirationInfo> {

  private EventHandler dispatcher;

  public ContainerAllocationExpirer(Dispatcher d) {
    super(ContainerAllocationExpirer.class.getName());
    this.dispatcher = d.getEventHandler();
  }

  public void serviceInit(Configuration conf) throws Exception {
    //从配置 YarnConfiguration.RM_CONTAINER_ALLOC_EXPIRY_INTERVAL_MS 读取容器分配的超时时间（默认值 DEFAULT_RM_CONTAINER_ALLOC_EXPIRY_INTERVAL_MS）
    int expireIntvl = conf.getInt(
            YarnConfiguration.RM_CONTAINER_ALLOC_EXPIRY_INTERVAL_MS,
            YarnConfiguration.DEFAULT_RM_CONTAINER_ALLOC_EXPIRY_INTERVAL_MS);
    //设置过期时间间隔 (expireInterval)，即容器最长未被使用的时间
    setExpireInterval(expireIntvl);
    //设定监控间隔 (monitorInterval)，一般是 expireInterval 的三分之一，用于周期性检查是否有超时的容器
    setMonitorInterval(expireIntvl/3);
    super.serviceInit(conf);
  }

  @Override
  protected void expire(AllocationExpirationInfo allocationExpirationInfo) {
    dispatcher.handle(new ContainerExpiredSchedulerEvent(
        allocationExpirationInfo.getContainerId(),
            allocationExpirationInfo.isIncrease()));
  }
}
