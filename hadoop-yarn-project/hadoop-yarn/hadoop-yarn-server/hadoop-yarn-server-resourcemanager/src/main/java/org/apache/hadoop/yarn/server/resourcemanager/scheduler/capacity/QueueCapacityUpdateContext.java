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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;

import java.util.ArrayList;
import java.util.List;

/**
 * A storage that encapsulates intermediate calculation values throughout a
 * full queue capacity update phase.
 */
//存储和管理队列容量更新过程中的中间计算值，它在 YARN CapacityScheduler 的队列容量更新阶段（queue capacity update phase）发挥作用
public class QueueCapacityUpdateContext {
  //记录 更新后的整个集群资源信息，用于计算调度容量
  private final Resource updatedClusterResource;
  //节点标签管理器，它负责管理集群中 不同标签（Label）的节点资源
  private final RMNodeLabelsManager labelsManager;
  //存储 队列更新过程中的警告信息，如资源分配异常、配置错误等
  private final List<QueueUpdateWarning> warnings = new ArrayList<QueueUpdateWarning>();

  public QueueCapacityUpdateContext(Resource updatedClusterResource,
                                    RMNodeLabelsManager labelsManager) {
    this.updatedClusterResource = updatedClusterResource;
    this.labelsManager = labelsManager;
  }

  /**
   * Returns the overall cluster resource available for the update phase.
   *
   * @param label node label
   * @return cluster resource
   */
  public Resource getUpdatedClusterResource(String label) {
    return labelsManager.getResourceByLabel(label, updatedClusterResource);
  }

  /**
   * Returns the overall cluster resource available for the update phase of empty label.
   * @return cluster resource
   */
  public Resource getUpdatedClusterResource() {
    return updatedClusterResource;
  }

  /**
   * Adds an update warning to the context.
   * @param warning warning during update phase
   */
  public void addUpdateWarning(QueueUpdateWarning warning) {
    warnings.add(warning);
  }

  /**
   * Returns all update warnings occurred in this update phase.
   * @return update warnings
   */
  public List<QueueUpdateWarning> getUpdateWarnings() {
    return warnings;
  }
}
