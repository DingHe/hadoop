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
package org.apache.hadoop.yarn.server.nodemanager.nodelabels;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.Collections;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.NodeLabel;

/**
 * Provides base implementation of NodeDescriptorsProvider with Timer and
 * expects subclass to provide TimerTask which can fetch node descriptors.
 */
//提供了节点描述符（Node Descriptors）提供者的基础实现。它包含了定时任务的管理，期望子类提供具体的任务实现来获取节点描述符
public abstract class AbstractNodeDescriptorsProvider<T>
    extends AbstractService implements NodeDescriptorsProvider<T> {
  //表示禁用定时任务（即不进行定时获取节点描述符），其值为 -1
  public static final long DISABLE_NODE_DESCRIPTORS_PROVIDER_FETCH_TIMER = -1;

  // Delay after which timer task are triggered to fetch node descriptors.
  // Default interval is -1 means it is an one time task, each implementation
  // will override this value from configuration.
  //表示定时任务的间隔时间，单位为毫秒。默认值为 -1，表示任务只执行一次
  private long intervalTime = -1;

  // Timer used to schedule node descriptors fetching
  //用于定时调度获取节点描述符的任务
  private Timer scheduler;

  protected Lock readLock = null;
  protected Lock writeLock = null;
  //表示定时任务，该任务将负责定期更新节点描述符
  protected TimerTask timerTask;
  //存储节点描述符的集合，类型为 Set<T>，用来存储从提供者获取的节点描述符
  private Set<T> nodeDescriptors = Collections
      .unmodifiableSet(new HashSet<>(0));

  public AbstractNodeDescriptorsProvider(String name) {
    super(name);
  }

  public long getIntervalTime() {
    return intervalTime;
  }

  public void setIntervalTime(long intervalMS) {
    this.intervalTime = intervalMS;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    readLock = readWriteLock.readLock();
    writeLock = readWriteLock.writeLock();
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    timerTask = createTimerTask();
    timerTask.run();
    long taskInterval = getIntervalTime();
    if (taskInterval != DISABLE_NODE_DESCRIPTORS_PROVIDER_FETCH_TIMER) {
      scheduler =
          new Timer("DistributedNodeDescriptorsRunner-Timer", true);
      // Start the timer task and then periodically at the configured interval
      // time. Illegal values for intervalTime is handled by timer api
      scheduler.schedule(timerTask, taskInterval, taskInterval);
    }
    super.serviceStart();
  }

  /**
   * terminate the timer
   * @throws Exception
   */
  @Override
  protected void serviceStop() throws Exception {
    if (scheduler != null) {
      scheduler.cancel();
    }
    cleanUp();
    super.serviceStop();
  }

  /**
   * method for subclasses to cleanup.
   */
  protected abstract void cleanUp() throws Exception ;

  /**
   * @return Returns output from provider.
   */
  @Override
  public Set<T> getDescriptors() {
    readLock.lock();
    try {
      return this.nodeDescriptors;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void setDescriptors(Set<T> descriptorsSet) {
    writeLock.lock();
    try {
      this.nodeDescriptors = descriptorsSet;
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Method used to determine if or not node descriptors fetching script is
   * configured and whether it is fit to run. Returns true if following
   * conditions are met:
   *
   * <ol>
   * <li>Path to the script is not empty</li>
   * <li>The script file exists</li>
   * </ol>
   *
   * @throws IOException
   */
  protected void verifyConfiguredScript(String scriptPath)
      throws IOException {
    boolean invalidConfiguration;
    if (scriptPath == null
        || scriptPath.trim().isEmpty()) {
      invalidConfiguration = true;
    } else {
      File f = new File(scriptPath);
      invalidConfiguration = !f.exists() || !FileUtil.canExecute(f);
    }
    if (invalidConfiguration) {
      throw new IOException(
          "Node descriptors provider script \"" + scriptPath
              + "\" is not configured properly. Please check whether"
              + " the script path exists, owner and the access rights"
              + " are suitable for NM process to execute it");
    }
  }

  static Set<NodeLabel> convertToNodeLabelSet(String partitionNodeLabel) {
    if (null == partitionNodeLabel) {
      return null;
    }
    Set<NodeLabel> labels = new HashSet<NodeLabel>();
    labels.add(NodeLabel.newInstance(partitionNodeLabel));
    return labels;
  }

  /**
   * Used only by tests to access the timer task directly
   *
   * @return the timer task
   */
  TimerTask getTimerTask() {
    return timerTask;
  }

  @VisibleForTesting
  public Timer getScheduler() {
    return this.scheduler;
  }

  /**
   * Creates a timer task which be scheduled periodically by the provider,
   * and the task is responsible to update node descriptors to the provider.
   * @return a timer task.
   */
  public abstract TimerTask createTimerTask();
}
