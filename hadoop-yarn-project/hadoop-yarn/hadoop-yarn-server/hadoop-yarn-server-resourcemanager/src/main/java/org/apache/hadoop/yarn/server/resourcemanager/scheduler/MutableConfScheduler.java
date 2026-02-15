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

import org.apache.hadoop.conf.Configuration;

/**
 * Interface for a scheduler that supports changing configuration at runtime.
 *
 */
//用于支持在运行时修改调度器的配置。它主要提供了以下能力：
//获取当前调度器的配置，允许外部模块访问调度器的配置参数。
//根据队列名称获取调度队列，支持动态查询调度队列。
//判断调度器配置是否可变，用于区分静态和动态调度器。
//提供可变配置提供者，允许外部类直接修改调度器配置
public interface MutableConfScheduler extends ResourceScheduler {

  /** 获取调度器当前使用的配置对象
   * Get the scheduler configuration.
   * @return the scheduler configuration
   */
  Configuration getConfiguration();

  /** 具体实现需要根据 queueName 查询调度器的队列信息，并返回相应的Queue实例。
   * Get queue object based on queue name.
   * @param queueName the queue name
   * @return the queue object
   */
  Queue getQueue(String queueName);

  /** 判断当前调度器的配置是否支持运行时修改
   * Return whether the scheduler configuration is mutable.
   * @return whether scheduler configuration is mutable or not.
   */
  boolean isConfigurationMutable();

  /** 获取调度器的可变配置提供者，允许外部模块修改调度参数
   * Get scheduler's configuration provider, so other classes can directly
   * call mutation APIs on configuration provider.
   * @return scheduler's configuration provider
   */
  MutableConfigurationProvider getMutableConfProvider();
}
