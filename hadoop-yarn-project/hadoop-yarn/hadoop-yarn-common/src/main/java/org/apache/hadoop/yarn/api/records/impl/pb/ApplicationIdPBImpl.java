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

package org.apache.hadoop.yarn.api.records.impl.pb;


import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;

// ApplicationIdPBImpl 是 ApplicationId 的 Protocol Buffers (PB) 实现类，
// 作用是将 ApplicationId 转换为 ApplicationIdProto，便于数据序列化、网络传输以及持久化存储
@Private
@Unstable
public class ApplicationIdPBImpl extends ApplicationId {
  //存储了 ApplicationId 的 PB 对象，当 ApplicationIdPBImpl 需要获取 ID 或集群时间戳时，直接从 proto 读取数据
  ApplicationIdProto proto = null;
  //builder：用于构造 proto，在 build() 方法调用后被清除，避免不必要的内存占用
  ApplicationIdProto.Builder builder = null;

  public ApplicationIdPBImpl() {
    builder = ApplicationIdProto.newBuilder();
  }

  public ApplicationIdPBImpl(ApplicationIdProto proto) {
    this.proto = proto;
  }

  public ApplicationIdProto getProto() {
    return proto;
  }

  @Override
  public int getId() {
    if (proto == null) {
      throw new NullPointerException("The argument object is NULL");
    }
    return proto.getId();
  }

  @Override
  protected void setId(int id) {
    if (builder == null) {
      throw new NullPointerException("The argument object is NULL");
    }
    builder.setId(id);
  }
  @Override
  public long getClusterTimestamp() {
    if (proto == null) {
      throw new NullPointerException("The argument object is NULL");
    }
    return proto.getClusterTimestamp();
  }

  @Override
  protected void setClusterTimestamp(long clusterTimestamp) {
    if (builder == null) {
      throw new NullPointerException("The argument object is NULL");
    }
    builder.setClusterTimestamp((clusterTimestamp));
  }

  @Override
  protected void build() {
    proto = builder.build();
    builder = null;
  }
}