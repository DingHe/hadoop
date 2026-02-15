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
package org.apache.hadoop.net;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/** The interface defines a node in a network topology.
 * A node may be a leave representing a data node or an inner
 * node representing a datacenter or rack.
 * Each data has a name and its location in the network is
 * decided by a string with syntax similar to a file name. 
 * For example, a data node's name is hostname:port# and if it's located at
 * rack "orange" in datacenter "dog", the string representation of its
 * network location is /dog/orange
 */

@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
public interface Node {//该接口定义了 Hadoop 网络拓扑中的一个节点，既可以是表示数据节点的叶子节点，也可以是表示数据中心或机架的内部节点
  /** @return the string representation of this node's network location */
  public String getNetworkLocation();//返回节点在网络拓扑中的位置，通常使用路径格式表示，如 /dc1/rack2

  /** Set this node's network location
   * @param location the location
   */
  public void setNetworkLocation(String location);//设置节点的网络位置

  /** @return this node's name */
  public String getName();//返回节点的名称，通常是唯一标识符（如数据节点的主机名和端口号）

  /** @return this node's parent */
  public Node getParent();//返回该节点的父节点，适用于树状网络拓扑

  /** Set this node's parent
   * @param parent the parent
   */
  public void setParent(Node parent);//设置该节点的父节点，建立拓扑中的上下级关系

  /** @return this node's level in the tree.
   * E.g. the root of a tree returns 0 and its children return 1
   */
  public int getLevel();//节点层级，根节点返回 0，其子节点返回 1，依次类推

  /** Set this node's level in the tree
   * @param i the level
   */
  public void setLevel(int i);
}
