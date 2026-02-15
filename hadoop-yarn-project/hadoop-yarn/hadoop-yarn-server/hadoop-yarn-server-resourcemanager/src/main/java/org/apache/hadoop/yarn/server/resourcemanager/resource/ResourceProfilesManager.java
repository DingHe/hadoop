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

package org.apache.hadoop.yarn.server.resourcemanager.resource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YARNFeatureNotEnabledException;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.util.Map;

/**
 * Interface for the resource profiles manager. Provides an interface to get
 * the list of available profiles and some helper functions.
 */
// 用于管理 资源配置文件（Resource Profiles）。
// 资源配置文件定义了一组标准化的资源规格，例如 CPU 和内存，以便 YARN 任务可以使用这些预定义的资源配置，而不是每次都手动指定
public interface ResourceProfilesManager {

  /**
   * Method to handle all initialization steps for ResourceProfilesManager.
   * @param config Configuration object
   * @throws IOException when invalid resource profile names are loaded
   */
  //config：YARN 的配置对象，包含资源配置文件的信息。
  void init(Configuration config) throws IOException;

  /**
   * Get the resource capability associated with given profile name.
   * @param profile name of resource profile
   * @return resource capability for given profile
   *
   * @throws YarnException when any invalid profile name or feature is disabled
   */
  //profile：资源配置文件的名称，例如 "LARGE"、"MEDIUM"、"SMALL"
  Resource getProfile(String profile) throws YarnException;

  /**
   * Get all supported resource profiles.
   * @return a map of resource objects associated with each profile
   *
   * @throws YARNFeatureNotEnabledException when feature is disabled
   */
  //返回所有资源配置文件的 Map，键是配置文件名称，值是对应的 Resource 对象
  Map<String, Resource> getResourceProfiles() throws
      YARNFeatureNotEnabledException;

  /**
   * Reload profiles based on updated configuration.
   * @throws IOException when invalid resource profile names are loaded
   */
  //重新加载资源配置文件
  void reloadProfiles() throws IOException;

  /**
   * Get default supported resource profile.
   * @return resource object which is default
   * @throws YarnException when any invalid profile name or feature is disabled
   */
  //YARN 预定义的默认 Resource 配置
  Resource getDefaultProfile() throws YarnException;

  /**
   * Get minimum supported resource profile.
   * @return resource object which is minimum
   * @throws YarnException when any invalid profile name or feature is disabled
   */
  //YARN 允许的最小 Resource 规格
  Resource getMinimumProfile() throws YarnException;

  /**
   * Get maximum supported resource profile.
   * @return resource object which is maximum
   * @throws YarnException when any invalid profile name or feature is disabled
   */
  //YARN 允许的最大 Resource 规格
  Resource getMaximumProfile() throws YarnException;
}
