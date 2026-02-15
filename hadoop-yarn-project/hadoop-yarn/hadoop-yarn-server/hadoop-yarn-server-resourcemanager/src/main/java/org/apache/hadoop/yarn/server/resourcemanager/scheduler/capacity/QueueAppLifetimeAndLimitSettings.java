/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

/**
 * This class determines application lifetime and max parallel apps settings based on the
 * {@link CapacitySchedulerConfiguration} and other queue
 * properties.
 **/
//管理 YARN 容量调度器（CapacityScheduler）中队列的应用程序生命周期 (Application Lifetime) 和最大并行应用数 (Max Parallel Apps)。
// 它基于 CapacitySchedulerConfiguration 配置以及队列的继承关系来确定
public class QueueAppLifetimeAndLimitSettings {
  // -1 indicates lifetime is disabled
  //	该队列的最大应用生命周期，单位通常是秒，-1 表示未启用生命周期限制
  private final long maxApplicationLifetime;
  //该队列的默认应用生命周期。如果用户提交应用时未指定生命周期，则使用该值
  private final long defaultApplicationLifetime;

  // Indicates if this queue's default lifetime was set by a config property,
  // either at this level or anywhere in the queue's hierarchy.
  //标识默认应用生命周期是否由配置文件指定，或者是从父队列继承
  private boolean defaultAppLifetimeWasSpecifiedInConfig = false;
  //队列中同时可运行的最大应用数量。
  private int maxParallelApps;

  public QueueAppLifetimeAndLimitSettings(CapacitySchedulerConfiguration configuration,
      AbstractCSQueue q, QueuePath queuePath) {
    // Store max parallel apps property
    this.maxParallelApps = configuration.getMaxParallelAppsForQueue(queuePath.getFullPath());
    this.maxApplicationLifetime = getInheritedMaxAppLifetime(q, configuration);
    this.defaultApplicationLifetime = setupInheritedDefaultAppLifetime(q, queuePath, configuration,
        maxApplicationLifetime);
  }

  private long getInheritedMaxAppLifetime(CSQueue q, CapacitySchedulerConfiguration conf) {
    CSQueue parentQ = q.getParent();
    long maxAppLifetime = conf.getMaximumLifetimePerQueue(q.getQueuePath());

    // If q is the root queue, then get max app lifetime from conf.
    if (q.getQueuePathObject().isRoot()) {
      return maxAppLifetime;
    }

    // If this is not the root queue, get this queue's max app lifetime
    // from the conf. The parent's max app lifetime will be used if it's
    // not set for this queue.
    // A value of 0 will override the parent's value and means no max lifetime.
    // A negative value means that the parent's max should be used.
    long parentsMaxAppLifetime = parentQ.getMaximumApplicationLifetime();
    return (maxAppLifetime >= 0) ? maxAppLifetime : parentsMaxAppLifetime;
  }

  private long setupInheritedDefaultAppLifetime(CSQueue q,
      QueuePath queuePath, CapacitySchedulerConfiguration conf, long myMaxAppLifetime) {
    CSQueue parentQ = q.getParent();
    long defaultAppLifetime = conf.getDefaultLifetimePerQueue(queuePath.getFullPath());
    defaultAppLifetimeWasSpecifiedInConfig =
        (defaultAppLifetime >= 0
            || (!queuePath.isRoot() &&
            parentQ.getDefaultAppLifetimeWasSpecifiedInConfig()));

    // If q is the root queue, then get default app lifetime from conf.
    if (queuePath.isRoot()) {
      return defaultAppLifetime;
    }

    // If this is not the root queue, get the parent's default app lifetime. The
    // parent's default app lifetime will be used if not set for this queue.
    long parentsDefaultAppLifetime = parentQ.getDefaultApplicationLifetime();

    // Negative value indicates default lifetime was not set at this level.
    // If default lifetime was not set at this level, calculate it based on
    // parent's default lifetime or current queue's max lifetime.
    if (defaultAppLifetime < 0) {
      // If default lifetime was not set at this level but was set somewhere in
      // the parent's hierarchy, set default lifetime to parent queue's default
      // only if parent queue's lifetime is less than current queue's max
      // lifetime. Otherwise, use current queue's max lifetime value for its
      // default lifetime.
      if (defaultAppLifetimeWasSpecifiedInConfig) {
        defaultAppLifetime =
            Math.min(parentsDefaultAppLifetime, myMaxAppLifetime);
      } else {
        // Default app lifetime value was not set anywhere in this queue's
        // hierarchy. Use current queue's max lifetime as its default.
        defaultAppLifetime = myMaxAppLifetime;
      }
    } // else if >= 0, default lifetime was set at this level. Just use it.

    if (myMaxAppLifetime > 0 && defaultAppLifetime > myMaxAppLifetime) {
      throw new YarnRuntimeException(
          "Default lifetime " + defaultAppLifetime
              + " can't exceed maximum lifetime " + myMaxAppLifetime);
    }

    if (defaultAppLifetime <= 0) {
      defaultAppLifetime = myMaxAppLifetime;
    }
    return defaultAppLifetime;
  }

  public int getMaxParallelApps() {
    return maxParallelApps;
  }

  public void setMaxParallelApps(int maxParallelApps) {
    this.maxParallelApps = maxParallelApps;
  }

  public long getMaxApplicationLifetime() {
    return maxApplicationLifetime;
  }

  public long getDefaultApplicationLifetime() {
    return defaultApplicationLifetime;
  }

  public boolean isDefaultAppLifetimeWasSpecifiedInConfig() {
    return defaultAppLifetimeWasSpecifiedInConfig;
  }
}
