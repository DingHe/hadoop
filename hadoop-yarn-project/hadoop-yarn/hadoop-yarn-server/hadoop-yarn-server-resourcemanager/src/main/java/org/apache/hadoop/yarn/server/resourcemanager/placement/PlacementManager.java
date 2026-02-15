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

package org.apache.hadoop.yarn.server.resourcemanager.placement;

import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.exceptions.YarnException;

import org.apache.hadoop.classification.VisibleForTesting;
//用于处理应用程序放置策略的管理类。它的主要功能是根据一组放置规则 (PlacementRule)，决定应用程序的放置位置，即应用程序应该在哪些节点或机架上运行。
// 该类提供了对放置规则的更新、应用程序放置决策的执行等功能，确保应用程序能够根据配置的规则被合理地调度到合适的计算资源上
public class PlacementManager {  
  private static final Logger LOG =
      LoggerFactory.getLogger(PlacementManager.class);
  //存储一组 PlacementRule 对象，定义了应用程序放置的策略。这些规则决定了应用程序应该如何在集群中选择合适的节点或机架进行部署
  List<PlacementRule> rules;
  ReadLock readLock;
  WriteLock writeLock;

  public PlacementManager() {
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();
  }

  public void updateRules(List<PlacementRule> rules) {
    writeLock.lock();
    try {
      this.rules = rules;
    } finally {
      writeLock.unlock();
    }
  }

  public ApplicationPlacementContext placeApplication(
      ApplicationSubmissionContext asc, String user, boolean recovery)
      throws YarnException {
    readLock.lock();
    try {
      if (null == rules || rules.isEmpty()) {
        return null;
      }

      ApplicationPlacementContext placement = null;
      for (PlacementRule rule : rules) {
        placement = rule.getPlacementForApp(asc, user, recovery);
        if (placement != null) {
          break;
        }
      }

      return placement;
    } finally {
      readLock.unlock();
    }
  }

  public ApplicationPlacementContext placeApplication(
      ApplicationSubmissionContext asc, String user) throws YarnException {
    return placeApplication(asc, user, false);
  }
  
  @VisibleForTesting
  public List<PlacementRule> getPlacementRules() {
    return rules;
  }
}
