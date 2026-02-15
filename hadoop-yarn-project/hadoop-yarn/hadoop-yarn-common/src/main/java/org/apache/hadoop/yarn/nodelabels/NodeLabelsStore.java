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

package org.apache.hadoop.yarn.nodelabels;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.exceptions.YarnException;
//用于定义与节点标签存储相关的操作。
// 它提供了用于存储、删除、更新节点标签映射、恢复标签信息以及初始化存储的方法。
// NodeLabelsStore 的实现类通常负责将节点标签的状态持久化到磁盘或分布式存储系统中，并在系统重启或恢复时进行恢复操作。
// 这个接口确保了 YARN 系统中节点标签信息的一致性和持久化
/**
 * Interface class for Node label store.
 */
public interface NodeLabelsStore extends Closeable {

  /**
   * Store node {@literal ->} label.
   * @param nodeToLabels node to labels mapping.
   * @throws IOException io error occur.
   */
  //更新节点到标签的映射关系
  //nodeToLabels：一个映射，表示每个节点 ID（NodeId）与其对应的标签集合（Set<String>）之间的关系
  void updateNodeToLabelsMappings(
      Map<NodeId, Set<String>> nodeToLabels) throws IOException;

  /**
   * Store new labels.
   * @param labels labels.
   * @throws IOException io error occur.
   */
  //存储新的集群节点标签
  //labels：一个 NodeLabel 类型的列表，表示集群中需要存储的新的节点标签
  void storeNewClusterNodeLabels(List<NodeLabel> labels)
      throws IOException;

  /**
   * Remove labels.
   * @param labels labels.
   * @throws IOException io error occur.
   */
  //删除指定的节点标签
  //一个字符串集合，包含需要删除的节点标签名称
  void removeClusterNodeLabels(Collection<String> labels)
      throws IOException;

  /**
   * Recover labels and node to labels mappings from store, but if
   * ignoreNodeToLabelsMappings is true then node to labels mappings should not
   * be recovered. In case of Distributed NodeLabels setup
   * ignoreNodeToLabelsMappings will be set to true and recover will be invoked
   * as RM will collect the node labels from NM through registration/HB.
   *
   * @throws IOException io error occur.
   * @throws YarnException exceptions from yarn servers.
   */
  //从存储中恢复节点标签信息和节点到标签的映射关系
  void recover() throws IOException, YarnException;
  //初始化节点标签存储系统
  void init(Configuration conf, CommonNodeLabelsManager mgr)
      throws Exception;

}
