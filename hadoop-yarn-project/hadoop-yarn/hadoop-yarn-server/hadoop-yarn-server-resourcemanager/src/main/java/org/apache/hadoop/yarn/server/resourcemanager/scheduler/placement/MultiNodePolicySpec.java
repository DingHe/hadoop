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

/**
 * MultiNodePolicySpec contains policyName and timeout.
 */
//主要用于存储和管理多节点查找策略（MultiNodeLookupPolicy）的配置信息，包括：
//策略名称 (policyName)：指定当前使用的多节点调度策略。
//排序间隔 (sortingInterval)：控制节点排序的时间间隔，用于动态调整节点优先级
public class MultiNodePolicySpec {
  //存储当前使用的多节点查找策略名称
  //例如，可能的策略名称：
  //"ResourceUsage"（基于资源使用情况排序）
  //"RoundRobin"（轮询策略）
  //"Random"（随机分配）
  private String policyName;
  //用于控制多长时间更新一次节点排序，防止频繁排序带来的计算开销
  private long sortingInterval;

  public MultiNodePolicySpec(String policyName, long timeout) {
    this.setSortingInterval(timeout);
    this.setPolicyName(policyName);
  }

  public long getSortingInterval() {
    return sortingInterval;
  }

  public void setSortingInterval(long timeout) {
    this.sortingInterval = timeout;
  }

  public String getPolicyName() {
    return policyName;
  }

  public void setPolicyName(String policyName) {
    this.policyName = policyName;
  }

  @Override
  public String toString() {
    return "MultiNodePolicySpec {" +
        "policyName='" + policyName + '\'' +
        ", sortingInterval=" + sortingInterval +
        '}';
  }
}
