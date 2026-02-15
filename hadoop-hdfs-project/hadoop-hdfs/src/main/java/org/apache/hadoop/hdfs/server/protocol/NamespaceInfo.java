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

package org.apache.hadoop.hdfs.server.protocol;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.util.VersionInfo;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;

/**
 * NamespaceInfo is returned by the name-node in reply 
 * to a data-node handshake.
 * 存储和管理与HDFS命名节点（NameNode）相关的命名空间信息。它主要用于数据节点（DataNode）与命名节点进行握手时，命名节点返回给数据节点的响应
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class NamespaceInfo extends StorageInfo {
  final String  buildVersion;//存储构建版本信息，表示当前命名节点的构建版本
  String blockPoolID = "";    // id of the block pool 存储块池的ID（Block Pool ID），用于标识HDFS中存储数据的逻辑分组
  String softwareVersion;  //存储软件版本信息，表示当前命名节点所运行的软件版本
  long capabilities;       //表示命名节点支持的能力标志，使用位掩码存储多个特性
  HAServiceState state;    //存储命名节点的高可用性（HA）服务状态（HAServiceState），用于指示命名节点的运行状态，如是否处于活动或待命状态

  // only authoritative on the server-side to determine advertisement to
  // clients.  enum will update the supported values
  private static final long CAPABILITIES_SUPPORTED = getSupportedCapabilities();

  private static long getSupportedCapabilities() {
    long mask = 0;
    for (Capability c : Capability.values()) {
      if (c.supported) {
        mask |= c.mask;
      }
    }
    return mask;
  }

  public enum Capability {
    UNKNOWN(false),
    STORAGE_BLOCK_REPORT_BUFFERS(true); // use optimized ByteString buffers
    private final boolean supported;
    private final long mask;
    Capability(boolean isSupported) {
      supported = isSupported;
      int bits = ordinal() - 1;
      mask = (bits < 0) ? 0 : (1L << bits);
    }
    public long getMask() {
      return mask;
    }
  }

  // defaults to enabled capabilites since this ctor is for server
  public NamespaceInfo() {
    super(NodeType.NAME_NODE);
    buildVersion = null;
    capabilities = CAPABILITIES_SUPPORTED;
  }

  // defaults to enabled capabilites since this ctor is for server
  public NamespaceInfo(int nsID, String clusterID, String bpID,
      long cT, String buildVersion, String softwareVersion) {
    this(nsID, clusterID, bpID, cT, buildVersion, softwareVersion,
        CAPABILITIES_SUPPORTED);
  }

  public NamespaceInfo(int nsID, String clusterID, String bpID,
      long cT, String buildVersion, String softwareVersion,
      long capabilities, HAServiceState st) {
    this(nsID, clusterID, bpID, cT, buildVersion, softwareVersion,
        capabilities);
    this.state = st;
  }

  // for use by server and/or client
  public NamespaceInfo(int nsID, String clusterID, String bpID,
      long cT, String buildVersion, String softwareVersion,
      long capabilities) {
    super(HdfsServerConstants.NAMENODE_LAYOUT_VERSION, nsID, clusterID, cT,
        NodeType.NAME_NODE);
    blockPoolID = bpID;
    this.buildVersion = buildVersion;
    this.softwareVersion = softwareVersion;
    this.capabilities = capabilities;
  }

  public NamespaceInfo(StorageInfo storage) {
    super(storage);
    if (storage instanceof NamespaceInfo) {
      this.capabilities = ((NamespaceInfo)storage).capabilities;
      this.blockPoolID = ((NamespaceInfo)storage).blockPoolID;
    } else {
      this.capabilities = CAPABILITIES_SUPPORTED;
    }
    this.buildVersion = Storage.getBuildVersion();
    this.softwareVersion = VersionInfo.getVersion();
    if (storage instanceof NNStorage) {
      this.blockPoolID = ((NNStorage)storage).getBlockPoolID();
    } else {
      this.blockPoolID = null;
    }

  }

  public NamespaceInfo(StorageInfo storage, HAServiceState st) {
    this(storage);
    this.state = st;
  }

  public NamespaceInfo(int nsID, String clusterID, String bpID, 
      long cT) {
    this(nsID, clusterID, bpID, cT, Storage.getBuildVersion(),
        VersionInfo.getVersion());
  }

  public NamespaceInfo(int nsID, String clusterID, String bpID,
      long cT, HAServiceState st) {
    this(nsID, clusterID, bpID, cT, Storage.getBuildVersion(),
        VersionInfo.getVersion());
    this.state = st;
  }
  
  public long getCapabilities() {
    return capabilities;
  }

  @VisibleForTesting
  public void setCapabilities(long capabilities) {
    this.capabilities = capabilities;
  }

  @VisibleForTesting
  public void setState(HAServiceState state) {
    this.state = state;
  }

  public boolean isCapabilitySupported(Capability capability) {
    Preconditions.checkArgument(capability != Capability.UNKNOWN,
        "cannot test for unknown capability");
    long mask = capability.getMask();
    return (capabilities & mask) == mask;
  }

  public String getBuildVersion() {
    return buildVersion;
  }

  public String getBlockPoolID() {
    return blockPoolID;
  }
  
  public String getSoftwareVersion() {
    return softwareVersion;
  }

  public HAServiceState getState() {
    return state;
  }

  public void setClusterID(String clusterID) {
    this.clusterID = clusterID;
  }

  public void setBlockPoolID(String blockPoolID) {
    this.blockPoolID = blockPoolID;
  }

  @Override
  public String toString(){
    return super.toString() + ";bpid=" + blockPoolID;
  }

  public void validateStorage(NNStorage storage) throws IOException {
    if (layoutVersion != storage.getLayoutVersion() ||
        namespaceID != storage.getNamespaceID() ||
        cTime != storage.cTime ||
        !clusterID.equals(storage.getClusterID()) ||
        !blockPoolID.equals(storage.getBlockPoolID())) {
      throw new IOException("Inconsistent namespace information:\n" +
          "NamespaceInfo has:\n" +
          "LV=" + layoutVersion + ";" +
          "NS=" + namespaceID + ";" +
          "cTime=" + cTime + ";" +
          "CID=" + clusterID + ";" +
          "BPID=" + blockPoolID +
          ".\nStorage has:\n" +
          "LV=" + storage.getLayoutVersion() + ";" +
          "NS=" + storage.getNamespaceID() + ";" +
          "cTime=" + storage.getCTime() + ";" +
          "CID=" + storage.getClusterID() + ";" +
          "BPID=" + storage.getBlockPoolID() + ".");
    }
  }
}
