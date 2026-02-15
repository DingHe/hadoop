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

package org.apache.hadoop.mapred;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
//描述 输入数据切片（InputSplit） 在不同存储介质（如磁盘、内存）上的位置信息
@Public
@Evolving
public class SplitLocationInfo {
  private boolean inMemory; // 标识数据是否存储在 内存 中，true 表示在内存中，false 表示不在内存。
  private String location; // 存储该数据切片的 节点名称（如 DataNode 主机名或 IP 地址）。

  public SplitLocationInfo(String location, boolean inMemory) {
    this.location = location;
    this.inMemory = inMemory;
  }
  
  public boolean isOnDisk() {
    return true;
  }
  
  public boolean isInMemory() {
    return inMemory;
  }

  public String getLocation() {
    return location;
  }
}
