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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.AbstractLivelinessMonitor;
import org.apache.hadoop.yarn.util.Clock;

import java.util.concurrent.TimeUnit;
//监控YARN资源管理器中应用程序尝试（Application Attempt）的活跃状态。
// 该类会定期检查应用程序尝试（ApplicationAttemptId）的心跳状态，
// 若在指定的时间内未收到心跳，则会触发过期事件并通过事件分发器（dispatcher）发送相应的过期事件。
// 主要目的是确保资源管理器能够及时处理未响应的应用程序尝试
public class AMLivelinessMonitor extends AbstractLivelinessMonitor<ApplicationAttemptId> {

  private EventHandler<Event> dispatcher;
  
  public AMLivelinessMonitor(Dispatcher d) {
    super("AMLivelinessMonitor");
    this.dispatcher = d.getEventHandler();
  }

  public AMLivelinessMonitor(Dispatcher d, Clock clock) {
    super("AMLivelinessMonitor", clock);
    this.dispatcher = d.getEventHandler();
  }

  public void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    long expireIntvl;
    //从配置中获取过期时间（RM_AM_EXPIRY_INTERVAL_MS），并根据该配置设置expireInterval（过期时间）和monitorInterval（监控间隔）
    String rmAmExpiryIntervalMS = conf.get(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS);
    if (NumberUtils.isDigits(rmAmExpiryIntervalMS)) {
      expireIntvl = conf.getLong(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS,
          YarnConfiguration.DEFAULT_RM_AM_EXPIRY_INTERVAL_MS);
    } else {
      expireIntvl = conf.getTimeDuration(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS,
          YarnConfiguration.DEFAULT_RM_AM_EXPIRY_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }
    setExpireInterval(expireIntvl);
    setMonitorInterval(expireIntvl/3);
  }

  @Override
  protected void expire(ApplicationAttemptId id) {
    dispatcher.handle(
        new RMAppAttemptEvent(id, RMAppAttemptEventType.EXPIRE));
  }
}