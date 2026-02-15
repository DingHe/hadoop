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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.common;

import org.apache.hadoop.yarn.api.records.Resource;

/**
 * Scheduler should implement this interface if it wants to have multi-threading
 * plus global scheduling functionality
 */
//用于支持多线程和全局调度功能的接口
//定义了一个核心方法 tryCommit()，用于尝试提交资源分配提案（proposal），并决定是否更新挂起的资源请求。
//主要作用：
//支持多线程调度：允许多个线程同时处理资源分配，提高调度效率。
//支持全局调度：确保资源分配可以在整个集群范围内进行优化。
//提供资源分配确认机制：在尝试提交资源分配方案时，检查是否满足资源约束，并决定是否更新挂起的请求。
public interface ResourceAllocationCommitter {

  /**
   * Try to commit the allocation Proposal. This also gives the option of
   * not updating a pending queued request.
   * @param cluster Cluster Resource.
   * @param proposal Proposal.
   * @param updatePending Decrement pending if successful.
   * @return Is successful or not.
   */
  //cluster  表示整个YARN集群的当前可用资源情况
  //proposal 表示要提交的资源分配提案，包含申请的资源、目标节点等信息
  //updatePending true：如果分配成功，则减少挂起的资源请求数量  false：如果分配成功，不更新挂起的请求数量
  boolean tryCommit(Resource cluster, ResourceCommitRequest proposal,
      boolean updatePending);
}
