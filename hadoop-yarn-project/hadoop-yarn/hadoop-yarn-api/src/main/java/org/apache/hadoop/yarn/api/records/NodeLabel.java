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

package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;
// 主要用于表示节点标签。节点标签是资源管理系统中的一种机制，用于标记和组织不同类型的节点。
// 它允许用户对节点进行分类，以便在资源调度时更精确地指定哪些应用程序可以在哪些节点上运行
@Public
@Unstable
public abstract class NodeLabel implements Comparable<NodeLabel> {

  /**
   * Default node label partition used for displaying.
   */
  //默认的节点标签分区，用于显示时标识。它的值是<DEFAULT_PARTITION>，表示一个默认的标签分区
  @Private
  @Unstable
  public static final String DEFAULT_NODE_LABEL_PARTITION = "<DEFAULT_PARTITION>";

  /**
   * Node Label expression not set .
   */
  //表示节点标签表达式没有被设置。通常用于标识节点标签尚未配置的情况
  @Private
  @Unstable
  public static final String NODE_LABEL_EXPRESSION_NOT_SET = "<Not set>";

  /**
   * By default, node label is exclusive or not
   */
  //表示节点标签的默认排他性。默认情况下，节点标签是排他的，即一个节点只能属于一个标签，true 表示排他性
  @Private
  @Unstable
  public static final boolean DEFAULT_NODE_LABEL_EXCLUSIVITY = true;

  @Private
  @Unstable
  public static NodeLabel newInstance(String name) {
    return newInstance(name, DEFAULT_NODE_LABEL_EXCLUSIVITY);
  }

  @Private
  @Unstable
  public static NodeLabel newInstance(String name, boolean isExclusive) {
    NodeLabel request = Records.newRecord(NodeLabel.class);
    request.setName(name);
    request.setExclusivity(isExclusive);
    return request;
  }
  //返回节点标签的名称
  @Public
  @Stable
  public abstract String getName();

  @Private
  @Unstable
  public abstract void setName(String name);
  //判断节点标签是否具有排他性
  @Public
  @Stable
  public abstract boolean isExclusive();

  @Private
  @Unstable
  public abstract void setExclusivity(boolean isExclusive);

  @Override
  public int compareTo(NodeLabel other) {
    return getName().compareTo(other.getName());
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof NodeLabel) {
      NodeLabel nl = (NodeLabel) obj;
      return nl.getName().equals(getName())
          && nl.isExclusive() == isExclusive();
    }
    return false;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("<")
        .append(getName())
        .append(":exclusivity=")
        .append(isExclusive())
        .append(">");
    return sb.toString();
  }

  @Override
  public int hashCode() {
    return (getName().hashCode() << 16) + (isExclusive() ? 1 : 0);
  }
}
