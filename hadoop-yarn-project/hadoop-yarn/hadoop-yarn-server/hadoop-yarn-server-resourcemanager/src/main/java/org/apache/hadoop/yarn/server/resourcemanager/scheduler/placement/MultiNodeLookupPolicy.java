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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

/**
 * <p>
 * This class has the following functionality.
 *
 * <p>
 * Provide an interface for MultiNodeLookupPolicy so that different placement
 * allocator can choose nodes based on need.
 * </p>
 */
//用于定义 YARN 资源调度中多节点选择策略。
//它的主要作用是为不同的任务调度算法提供一种通用的方式来选择、存储和排序节点，从而实现更优化的资源分配
public interface MultiNodeLookupPolicy<N extends SchedulerNode> {
  /**
   * Get iterator of preferred node depends on requirement and/or availability.
   *
   * @param nodes
   *          List of Nodes
   * @param partition
   *          node label
   *
   * @return iterator of preferred node
   */
  //返回一个按优先级排序的节点迭代器，用于调度器选择合适的 SchedulerNode 进行资源分配
  //支持按节点标签（Partition）筛选，即不同标签的节点可能会有不同的优先级排序
  //nodes：所有可用的 SchedulerNode 集合（未排序）
  //partition：要查找的节点标签（例如 GPU、HDFS_STORAGE、NO_LABEL 等）
  Iterator<N> getPreferredNodeIterator(Collection<N> nodes, String partition);

  /**
   * Refresh working nodes set for re-ordering based on the algorithm selected.
   *
   * @param nodes
   *          a collection working nm's.
   * @param partition
   *          node label
   */
  //将新的 SchedulerNode 集合添加到当前策略中，并根据选定的调度算法进行排序或刷新
  //nodes：要添加的 SchedulerNode 集合
  //partition：对应的节点标签
  void addAndRefreshNodesSet(Collection<N> nodes, String partition);

  /**
   * Get sorted nodes per partition.
   *
   * @param partition
   *          node label
   *
   * @return collection of sorted nodes
   */
  //返回特定标签（Partition）下的已排序节点集合，供调度器获取可用的 SchedulerNode 资源
  //partition：要查询的节点标签
  Set<N> getNodesPerPartition(String partition);

}
