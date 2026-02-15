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

package org.apache.hadoop.yarn.server.federation.store;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.exception.FederationStateVersionIncompatibleException;
import org.apache.hadoop.yarn.server.records.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FederationStore extends the three interfaces used to coordinate the state of
 * a federated cluster: {@link FederationApplicationHomeSubClusterStore},
 * {@link FederationMembershipStateStore}, {@link FederationPolicyStore}, and
 * {@link FederationReservationHomeSubClusterStore}.
 *
 */
//管理和协调联邦集群的状态。它提供了与联邦集群状态存储相关的多个方法，支持存储、加载、检查版本等操作。该接口继承了多个其他接口，涉及到应用程序、成员状态、策略、预定等信息的管理。
// 它主要用于确保集群的不同子系统之间的一致性和协调性，尤其是在多集群（或联邦集群）环境下
public interface FederationStateStore extends
    FederationApplicationHomeSubClusterStore, FederationMembershipStateStore,
    FederationPolicyStore, FederationReservationHomeSubClusterStore,
    FederationDelegationTokenStateStore {

  Logger LOG = LoggerFactory.getLogger(FederationStateStore.class);

  /**
   * Initialize the FederationStore.
   *
   * @param conf the cluster configuration
   * @throws YarnException if initialization fails
   */
  //集群配置对象，用于初始化联邦状态存储
  void init(Configuration conf) throws YarnException;

  /**
   * Perform any cleanup operations of the StateStore.
   *
   * @throws Exception if cleanup fails
   */
  void close() throws Exception;

  /**
   * Get the {@link Version} of the underlying federation state store client.
   * 返回当前 FederationStateStore 的版本信息（Version）
   * @return the {@link Version} of the underlying federation store client
   */
  Version getCurrentVersion();

  /**
   * Load the version information from the federation state store.
   * 从联邦状态存储中加载版本信息。返回存储中的 Version
   * @return the {@link Version} of the federation state store
   * @throws Exception an exception occurred in load version.
   */
  Version loadVersion() throws Exception;

  /**
   * Store the Version information in federation state store.
   * 用于将版本信息存储到联邦状态存储中。如果存储过程中发生异常，抛出 Exception
   * @throws Exception an exception occurred in store version.
   */
  void storeVersion() throws Exception;

  /**
   * Check the version of federation stateStore.
   *
   * @throws Exception an exception occurred in check version.
   */
  default void checkVersion() throws Exception {
    Version loadedVersion = loadVersion();
    LOG.info("Loaded Router State Version Info = {}.", loadedVersion);
    Version currentVersion = getCurrentVersion();
    if (loadedVersion != null && loadedVersion.equals(currentVersion)) {
      return;
    }
    // if there is no version info, treat it as CURRENT_VERSION_INFO;
    if (loadedVersion == null) {
      loadedVersion = currentVersion;
    }
    if (loadedVersion.isCompatibleTo(currentVersion)) {
      LOG.info("Storing Router State Version Info {}.", currentVersion);
      storeVersion();
    } else {
      throw new FederationStateVersionIncompatibleException(
         "Expecting Router state version " + currentVersion +
         ", but loading version " + loadedVersion);
    }
  }

  /**
   * We will clear the data in stateStore through the deleteStateStore method.
   * 清除联邦状态存储中的所有数据。通常，这个方法会在需要重置或删除存储状态时调用
   * @throws Exception an exception occurred in delete store.
   */
  void deleteStateStore() throws Exception;
}
