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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import java.util.List;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;

// 资源管理器中用于资源调度的队列。它提供了对队列的基本操作方法，
// 例如获取队列名称、队列指标、队列的访问控制列表（ACL）、队列的资源信息等。
// 此外，它还支持队列的资源预留、资源增加和减少、队列的恢复等功能
@Evolving
@LimitedPrivate("yarn")
public interface Queue {
  /**
   * Get the queue name
   * @return queue name
   */
  //返回队列的名称。队列名称是唯一的标识符，用于调度和管理队列
  String getQueueName();

  /**
   * Get the queue metrics
   * @return the queue metrics
   */
  //获取该队列的度量信息。度量信息通常包括队列的资源使用情况、已分配资源、队列的运行状态等指标
  QueueMetrics getMetrics();

  /**
   * Get queue information
   * @param includeChildQueues include child queues?
   * @param recursive recursively get child queue information?
   * @return queue information
   */
  //获取队列的详细信息，包括是否包含子队列的信息
  QueueInfo getQueueInfo(boolean includeChildQueues, boolean recursive);
  
  /**
   * Get queue ACLs for given <code>user</code>.
   * @param user username
   * @return queue ACLs for user
   */
  //获取给定用户的队列访问控制列表（ACL）。ACL 定义了用户对队列的访问权限
  List<QueueUserACLInfo> getQueueUserAclInfo(UserGroupInformation user);
  //检查给定的用户是否具有对队列的访问权限，具体的权限由 acl 指定
  boolean hasAccess(QueueACL acl, UserGroupInformation user);
  //获取队列的 AbstractUsersManager，用于管理和控制用户的访问权限
  public AbstractUsersManager getAbstractUsersManager();

  /**
   * Recover the state of the queue for a given container.
   * @param clusterResource the resource of the cluster
   * @param schedulerAttempt the application for which the container was allocated
   * @param rmContainer the container that was recovered.
   */
  //恢复队列中的容器状态。此方法会在容器恢复过程中被调用，以便根据集群资源、调度应用程序和容器的状态来恢复队列的资源分配
  public void recoverContainer(Resource clusterResource,
      SchedulerApplicationAttempt schedulerAttempt, RMContainer rmContainer);
  
  /**
   * Get labels can be accessed of this queue
   * labels={*}, means this queue can access any label
   * labels={ }, means this queue cannot access any label except node without label
   * labels={a, b, c} means this queue can access a or b or c  
   * @return labels
   */
  //获取此队列可以访问的节点标签
  public Set<String> getAccessibleNodeLabels();
  
  /**
   * Get default label expression of this queue. If label expression of
   * ApplicationSubmissionContext and label expression of Resource Request not
   * set, this will be used.
   * 
   * @return default label expression
   */
  //获取此队列的默认节点标签表达式。如果应用程序或资源请求没有设置节点标签表达式，则使用该默认标签表达式
  public String getDefaultNodeLabelExpression();

  /**
   * When new outstanding resource is asked, calling this will increase pending
   * resource in a queue.
   * 
   * @param nodeLabel asked by application
   * @param resourceToInc new resource asked
   */
  //增加队列中的待处理资源量。当应用程序请求新的资源时，可以调用此方法增加队列的待处理资源
  public void incPendingResource(String nodeLabel, Resource resourceToInc);
  
  /**
   * When an outstanding resource is fulfilled or canceled, calling this will
   * decrease pending resource in a queue.
   * 
   * @param nodeLabel
   *          asked by application
   * @param resourceToDec
   *          new resource asked
   */
  //减少队列中的待处理资源量。当已分配的资源满足请求或资源被取消时，调用此方法减少队列中的待处理资源
  public void decPendingResource(String nodeLabel, Resource resourceToDec);

  /**
   * Get the Default Application Priority for this queue
   *
   * @return default application priority
   */
  //获取该队列的默认应用程序优先级。应用程序的优先级用于调度器中的 FIFO 排队
  public Priority getDefaultApplicationPriority();

  /**
   * Increment Reserved Capacity
   *
   * @param partition
   *          asked by application
   * @param reservedRes
   *          reserved resource asked
   */
  //增加队列的预留资源。当应用程序要求预留资源时，调用此方法将资源预留给队列
  public void incReservedResource(String partition, Resource reservedRes);

  /**
   * Decrement Reserved Capacity
   *
   * @param partition
   *          asked by application
   * @param reservedRes
   *          reserved resource asked
   */
  //减少队列的预留资源。当已分配的预留资源被满足或取消时，调用此方法减少队列的预留资源
  public void decReservedResource(String partition, Resource reservedRes);
}
