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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import java.io.IOException;
import java.util.Set;

/**
 * This class determines accessible node labels, configured node labels and the default node
 * label expression based on the {@link CapacitySchedulerConfiguration} object and other queue
 * properties.
 */
//管理 YARN CapacityScheduler 队列的节点标签（Node Labels）相关设置
public class QueueNodeLabelsSettings {
  //该队列的父队列，方便继承父队列的节点标签信息。
  private final CSQueue parent;
  //该队列的路径信息，用于从配置中获取节点标签设置。
  private final QueuePath queuePath;
  //该队列可访问的节点标签集合。如果为空，则队列无法访问任何带标签的节点
  private Set<String> accessibleLabels;
  //该队列在配置文件中明确指定的节点标签集合。
  private Set<String> configuredNodeLabels;
  //该队列的默认节点标签表达式，决定默认情况下任务会提交到哪些带标签的节点上
  private String defaultLabelExpression;

  public QueueNodeLabelsSettings(CapacitySchedulerConfiguration configuration,
      CSQueue parent,
      QueuePath queuePath,
      ConfiguredNodeLabels configuredNodeLabels) throws IOException {
    this.parent = parent;
    this.queuePath = queuePath;
    initializeNodeLabels(configuration, configuredNodeLabels);
  }

  private void initializeNodeLabels(CapacitySchedulerConfiguration configuration,
      ConfiguredNodeLabels configuredNodeLabels)
      throws IOException {
    initializeAccessibleLabels(configuration);
    initializeDefaultLabelExpression(configuration);
    initializeConfiguredNodeLabels(configuration, configuredNodeLabels);
    validateNodeLabels();
  }
  //初始化可访问标签
  private void initializeAccessibleLabels(CapacitySchedulerConfiguration configuration) {
    this.accessibleLabels = configuration.getAccessibleNodeLabels(queuePath.getFullPath());
    // Inherit labels from parent if not set
    //如果该队列没有配置可访问标签，则递归从父队列获取
    if (this.accessibleLabels == null && parent != null) {
      this.accessibleLabels = parent.getAccessibleNodeLabels();
    }
  }
  //初始化默认标签表达式
  private void initializeDefaultLabelExpression(CapacitySchedulerConfiguration configuration) {
    this.defaultLabelExpression = configuration.getDefaultNodeLabelExpression(
        queuePath.getFullPath());
    // If the accessible labels is not null and the queue has a parent with a
    // similar set of labels copy the defaultNodeLabelExpression from the parent
    if (this.accessibleLabels != null && parent != null
        && this.defaultLabelExpression == null &&
        this.accessibleLabels.containsAll(parent.getAccessibleNodeLabels())) {
      this.defaultLabelExpression = parent.getDefaultNodeLabelExpression();
    }
  }
  //初始化已配置节点标签
  private void initializeConfiguredNodeLabels(CapacitySchedulerConfiguration configuration,
      ConfiguredNodeLabels configuredNodeLabelsParam) {
    if (configuredNodeLabelsParam != null) {
      if (queuePath.isRoot()) {
        this.configuredNodeLabels = configuredNodeLabelsParam.getAllConfiguredLabels();
      } else {
        this.configuredNodeLabels = configuredNodeLabelsParam.getLabelsByQueue(
            queuePath.getFullPath());
      }
    } else {
      // Fallback to suboptimal but correct logic
      this.configuredNodeLabels = configuration.getConfiguredNodeLabels(queuePath.getFullPath());
    }
  }

  private void validateNodeLabels() throws IOException {
    // Check if labels of this queue is a subset of parent queue, only do this
    // when the queue in question is not root
    if (!queuePath.isRoot()) {
      if (parent.getAccessibleNodeLabels() != null && !parent
          .getAccessibleNodeLabels().contains(RMNodeLabelsManager.ANY)) {
        // If parent isn't "*", child shouldn't be "*" too
        if (this.getAccessibleNodeLabels().contains(RMNodeLabelsManager.ANY)) {
          throw new IOException("Parent's accessible queue is not ANY(*), "
              + "but child's accessible queue is " + RMNodeLabelsManager.ANY);
        } else {
          Set<String> diff = Sets.difference(this.getAccessibleNodeLabels(),
              parent.getAccessibleNodeLabels());
          if (!diff.isEmpty()) {
            throw new IOException(String.format(
                "Some labels of child queue is not a subset of parent queue, these labels=[%s]",
                StringUtils.join(diff, ",")));
          }
        }
      }
    }
  }

  public boolean isAccessibleToPartition(String nodePartition) {
    // if queue's label is *, it can access any node
    if (accessibleLabels != null && accessibleLabels.contains(RMNodeLabelsManager.ANY)) {
      return true;
    }
    // any queue can access to a node without label
    if (nodePartition == null || nodePartition.equals(RMNodeLabelsManager.NO_LABEL)) {
      return true;
    }
    // a queue can access to a node only if it contains any label of the node
    if (accessibleLabels != null && accessibleLabels.contains(nodePartition)) {
      return true;
    }
    // The partition cannot be accessed
    return false;
  }

  public Set<String> getAccessibleNodeLabels() {
    return accessibleLabels;
  }

  public Set<String> getConfiguredNodeLabels() {
    return configuredNodeLabels;
  }

  public String getDefaultLabelExpression() {
    return defaultLabelExpression;
  }
}
