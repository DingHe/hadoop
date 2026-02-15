/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.preemption;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
//资源抢占（Preemption）管理 的核心类之一，它管理 YARN 调度器中的 可抢占资源 和 可抢占容器。主要功能包括：
//维护 各个队列（Queue）对应的 PreemptableQueue，记录可抢占的资源和容器。
//支持层级结构：通过 refreshQueues 方法维护队列层次结构，并与 PreemptableQueue 关联。
//提供读写锁 以确保并发安全：
//读锁（readLock）：用于查询 PreemptableQueue 数据（例如获取可抢占资源）。
//写锁（writeLock）：用于更新 PreemptableQueue 数据（例如添加/移除可抢占容器）。
//支持操作：
//添加、移除、查询 可抢占容器。
//获取特定队列下 可抢占的资源。
//维护 PreemptableQueue 数据结构的完整性。


public class PreemptionManager {
  private ReentrantReadWriteLock.ReadLock readLock;
  private ReentrantReadWriteLock.WriteLock writeLock;
  //维护 队列名称（Queue Name）到 PreemptableQueue 的映射，每个 PreemptableQueue 代表该队列下 可抢占的资源和容器信息
  private Map<String, PreemptableQueue> entities = new HashMap<>();

  public PreemptionManager() {
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();
  }

  //parent：当前队列的父队列
  //current：当前正在处理的队列
  public void refreshQueues(CSQueue parent, CSQueue current) {
    writeLock.lock();
    try {
      PreemptableQueue parentEntity = null;
      if (parent != null) {
        //获取父级队列
        parentEntity = entities.get(parent.getQueuePath());
      }

      if (!entities.containsKey(current.getQueuePath())) {
        entities.put(current.getQueuePath(),
            new PreemptableQueue(parentEntity));
      }
      //递归刷新子队列
      if (current.getChildQueues() != null) {
        for (CSQueue child : current.getChildQueues()) {
          refreshQueues(current, child);
        }
      }
    }
    finally {
      writeLock.unlock();
    }
  }

  //添加可抢占的资源
  public void addKillableContainer(KillableContainer container) {
    writeLock.lock();
    try {
      PreemptableQueue entity = entities.get(container.getLeafQueueName());
      if (null != entity) {
        entity.addKillableContainer(container);
      }
    }
    finally {
      writeLock.unlock();
    }
  }
  //移除可抢占的资源
  public void removeKillableContainer(KillableContainer container) {
    writeLock.lock();
    try {
      PreemptableQueue entity = entities.get(container.getLeafQueueName());
      if (null != entity) {
        entity.removeKillableContainer(container);
      }
    }
    finally {
      writeLock.unlock();
    }
  }

  public void moveKillableContainer(KillableContainer oldContainer,
      KillableContainer newContainer) {
    // TODO, will be called when partition of the node changed OR
    // container moved to different queue
  }

  public void updateKillableContainerResource(KillableContainer container,
      Resource oldResource, Resource newResource) {
    // TODO, will be called when container's resource changed
  }
  //获取特定队列的可抢占容器
  @VisibleForTesting
  public Map<ContainerId, RMContainer> getKillableContainersMap(
      String queueName, String partition) {
    readLock.lock();
    try {
      PreemptableQueue entity = entities.get(queueName);
      if (entity != null) {
        Map<ContainerId, RMContainer> containers =
            entity.getKillableContainers().get(partition);
        if (containers != null) {
          return containers;
        }
      }
      return Collections.emptyMap();
    }
    finally {
      readLock.unlock();
    }
  }
  //获取某个队列的可抢占资源
  public Iterator<RMContainer> getKillableContainers(String queueName,
      String partition) {
    return getKillableContainersMap(queueName, partition).values().iterator();
  }

  public Resource getKillableResource(String queueName, String partition) {
    readLock.lock();
    try {
      PreemptableQueue entity = entities.get(queueName);
      if (entity != null) {
        Resource res = entity.getTotalKillableResources().get(partition);
        if (res == null || res.equals(Resources.none())) {
          return Resources.none();
        }
        return Resources.clone(res);
      }
      return Resources.none();
    }
    finally {
      readLock.unlock();
    }
  }
  //返回 PreemptionManager 中所有 PreemptableQueue 的 浅拷贝
  public Map<String, PreemptableQueue> getShallowCopyOfPreemptableQueues() {
    readLock.lock();
    try {
      Map<String, PreemptableQueue> map = new HashMap<>();
      for (Map.Entry<String, PreemptableQueue> entry : entities.entrySet()) {
        String key = entry.getKey();
        PreemptableQueue entity = entry.getValue();
        map.put(key, new PreemptableQueue(
            new HashMap<>(entity.getTotalKillableResources()),
            new HashMap<>(entity.getKillableContainers())));
      }
      return map;
    } finally {
      readLock.unlock();
    }
  }
}
