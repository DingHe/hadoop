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
package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
//块存储策略的集合
/** A collection of block storage policies. */
public class BlockStoragePolicySuite {
  static final Logger LOG = LoggerFactory.getLogger(BlockStoragePolicySuite
      .class);
  //表示存储策略的扩展属性（XAttr）的名称，用于在文件系统中存储与存储策略相关的信息。它的值是 "hsm.block.storage.policy.id"
  public static final String STORAGE_POLICY_XATTR_NAME
      = "hsm.block.storage.policy.id";
  //表示该扩展属性属于系统命名空间。扩展属性（XAttr）有不同的命名空间，如 USER、SYSTEM 等，SYSTEM 表示这个属性是系统使用的
  public static final XAttr.NameSpace XAttrNS = XAttr.NameSpace.SYSTEM;
  //值为 4，表示存储策略的 ID 字段的位长度，用于生成一个存储策略的 ID
  public static final int ID_BIT_LENGTH = 4;

  @VisibleForTesting
  public static BlockStoragePolicySuite createDefaultSuite() {
    return createDefaultSuite(null);
  }
  //创建一个包含默认存储策略的 BlockStoragePolicySuite 实例
  @VisibleForTesting
  public static BlockStoragePolicySuite createDefaultSuite(
      final Configuration conf) {
    final BlockStoragePolicy[] policies =
        new BlockStoragePolicy[1 << ID_BIT_LENGTH]; //创建一个 BlockStoragePolicy 类型的数组 policies，数组大小为 1 << ID_BIT_LENGTH，即 16 个元素
    final byte lazyPersistId =
        HdfsConstants.StoragePolicy.LAZY_PERSIST.value();//表示 "Lazy Persist" 存储策略
    policies[lazyPersistId] = new BlockStoragePolicy(lazyPersistId,
        HdfsConstants.StoragePolicy.LAZY_PERSIST.name(),
        new StorageType[]{StorageType.RAM_DISK, StorageType.DISK},//表示数据首先存储在 RAM_DISK 中，之后存储到 DISK 中
        new StorageType[]{StorageType.DISK},
        new StorageType[]{StorageType.DISK},
        true);    // Cannot be changed on regular files, but inherited.
    final byte allnvdimmId = HdfsConstants.StoragePolicy.ALL_NVDIMM.value();//表示 "All NVDIMM" 存储策略
    policies[allnvdimmId] = new BlockStoragePolicy(allnvdimmId,
        HdfsConstants.StoragePolicy.ALL_NVDIMM.name(),
        new StorageType[]{StorageType.NVDIMM},
        new StorageType[]{StorageType.DISK},
        new StorageType[]{StorageType.DISK});
    final byte allssdId = HdfsConstants.StoragePolicy.ALL_SSD.value();//表示 "All SSD" 存储策略
    policies[allssdId] = new BlockStoragePolicy(allssdId,
        HdfsConstants.StoragePolicy.ALL_SSD.name(),
        new StorageType[]{StorageType.SSD},
        new StorageType[]{StorageType.DISK},
        new StorageType[]{StorageType.DISK});
    final byte onessdId = HdfsConstants.StoragePolicy.ONE_SSD.value(); //表示 "One SSD" 存储策略
    policies[onessdId] = new BlockStoragePolicy(onessdId,
        HdfsConstants.StoragePolicy.ONE_SSD.name(),
        new StorageType[]{StorageType.SSD, StorageType.DISK},
        new StorageType[]{StorageType.SSD, StorageType.DISK},
        new StorageType[]{StorageType.SSD, StorageType.DISK});
    final byte hotId = HdfsConstants.StoragePolicy.HOT.value(); //表示 "Hot" 存储策略
    policies[hotId] = new BlockStoragePolicy(hotId,
        HdfsConstants.StoragePolicy.HOT.name(),
        new StorageType[]{StorageType.DISK}, StorageType.EMPTY_ARRAY,
        new StorageType[]{StorageType.ARCHIVE});
    final byte warmId = HdfsConstants.StoragePolicy.WARM.value(); //表示 "Warm" 存储策略。
    policies[warmId] = new BlockStoragePolicy(warmId,
        HdfsConstants.StoragePolicy.WARM.name(),
        new StorageType[]{StorageType.DISK, StorageType.ARCHIVE},
        new StorageType[]{StorageType.DISK, StorageType.ARCHIVE},
        new StorageType[]{StorageType.DISK, StorageType.ARCHIVE});
    final byte coldId = HdfsConstants.StoragePolicy.COLD.value(); //表示 "Cold" 存储策略
    policies[coldId] = new BlockStoragePolicy(coldId,
        HdfsConstants.StoragePolicy.COLD.name(),
        new StorageType[]{StorageType.ARCHIVE}, StorageType.EMPTY_ARRAY,
        StorageType.EMPTY_ARRAY);
    final byte providedId = HdfsConstants.StoragePolicy.PROVIDED.value(); //表示 "Provided" 存储策略
    policies[providedId] = new BlockStoragePolicy(providedId,
      HdfsConstants.StoragePolicy.PROVIDED.name(),
      new StorageType[]{StorageType.PROVIDED, StorageType.DISK},
      new StorageType[]{StorageType.PROVIDED, StorageType.DISK},
      new StorageType[]{StorageType.PROVIDED, StorageType.DISK});

    return new BlockStoragePolicySuite(getDefaultPolicyID(conf, policies),
        policies);
  }
  //从配置中获取默认存储策略的 ID。它的作用是根据配置文件中指定的默认存储策略，找到对应的策略 ID，并返回该 ID。如果没有配置或者找不到匹配的策略，则返回一个默认值
  private static byte getDefaultPolicyID(
      final Configuration conf, final BlockStoragePolicy[] policies) {
    if (conf != null) {
      HdfsConstants.StoragePolicy defaultPolicy = conf.getEnum(
          DFSConfigKeys.DFS_STORAGE_DEFAULT_POLICY, //默认存储策略的配置项
          DFSConfigKeys.DFS_STORAGE_DEFAULT_POLICY_DEFAULT); //如果配置中没有找到对应的值，使用该默认值
      for (BlockStoragePolicy policy : policies) { //遍历传入的 policies 数组，查找与默认存储策略名称匹配的存储策略
        if (policy != null &&
            policy.getName().equalsIgnoreCase(defaultPolicy.name())) {
          return policy.getId(); //如果找到了匹配的存储策略，则返回该策略的 ID（通过 getId() 获取）
        }
      }
    }
    return DFSConfigKeys.DFS_STORAGE_DEFAULT_POLICY_DEFAULT.value();
  }
  //当前默认的存储策略的 ID。它是一个字节类型的值，表示系统的默认存储策略
  private final byte defaultPolicyID;
  //存储所有可用的存储策略。每个 BlockStoragePolicy 对象表示一个具体的存储策略
  private final BlockStoragePolicy[] policies;

  public BlockStoragePolicySuite(byte defaultPolicyID,
      BlockStoragePolicy[] policies) {
    this.defaultPolicyID = defaultPolicyID;
    this.policies = policies;
  }

  /** @return the corresponding policy. */
  public BlockStoragePolicy getPolicy(byte id) {
    // id == 0 means policy not specified.
    return id == 0? getDefaultPolicy(): policies[id];
  }

  /** @return the default policy. */
  public BlockStoragePolicy getDefaultPolicy() {
    return getPolicy(defaultPolicyID);
  }

  public BlockStoragePolicy getPolicy(String policyName) {
    Preconditions.checkNotNull(policyName);

    if (policies != null) {
      for (BlockStoragePolicy policy : policies) {
        if (policy != null && policy.getName().equalsIgnoreCase(policyName)) {
          return policy;
        }
      }
    }
    return null;
  }

  public BlockStoragePolicy[] getAllPolicies() {
    List<BlockStoragePolicy> list = Lists.newArrayList();
    if (policies != null) {
      for (BlockStoragePolicy policy : policies) {
        if (policy != null) {
          list.add(policy);
        }
      }
    }
    return list.toArray(new BlockStoragePolicy[list.size()]);
  }

  public static String buildXAttrName() {
    return StringUtils.toLowerCase(XAttrNS.toString())
        + "." + STORAGE_POLICY_XATTR_NAME;
  }

  public static XAttr buildXAttr(byte policyId) {
    final String name = buildXAttrName();
    return XAttrHelper.buildXAttr(name, new byte[]{policyId});
  }

  public static String getStoragePolicyXAttrPrefixedName() {
    return XAttrHelper.getPrefixedName(XAttrNS, STORAGE_POLICY_XATTR_NAME);
  }

  public static boolean isStoragePolicyXAttr(XAttr xattr) {
    return xattr != null && xattr.getNameSpace() == XAttrNS
        && xattr.getName().equals(STORAGE_POLICY_XATTR_NAME);
  }
}
