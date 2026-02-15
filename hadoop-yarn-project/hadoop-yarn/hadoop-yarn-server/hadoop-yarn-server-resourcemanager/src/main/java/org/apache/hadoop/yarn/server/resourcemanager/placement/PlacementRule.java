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

package org.apache.hadoop.yarn.server.resourcemanager.placement;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;

/**
 * Abstract base for all Placement Rules.
 */
//用于应用程序调度规则的抽象基类。它的作用是定义和实现应用程序调度到特定队列的规则。
// YARN 资源管理器（ResourceManager）根据这些规则决定应用程序应该被调度到哪个队列。
// 该类为所有具体的调度规则提供了一个统一的接口，允许在不同的调度场景下进行灵活的配置和扩展
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class PlacementRule {

  /**
   * Set the config based on the passed in argument. This construct is used to
   * not pollute this abstract class with implementation specific references.
   * @param initArg initialization arguments.
   */
  //默认实现（空方法），它的作用是为具体实现类提供一个初始化配置的接口。
  // 具体规则类可以根据自己的需要重写这个方法，来根据传入的配置参数设置相应的属性
  public void setConfig(Object initArg) {
    // Default is a noop
  }

  /**
   * Return the name of the rule.
   * @return The name of the rule, the fully qualified class name.
   */
  //返回当前规则的名称，即当前类的完全限定名（包括包名）
  public String getName() {
    return this.getClass().getName();
  }

  /**
   * Initialize the rule with the scheduler.
   * @param scheduler the scheduler using the rule
   * @return <code>true</code> or <code>false</code> The outcome of the
   * initialisation, rule dependent response which might not be persisted in
   * the rule.
   * @throws IOException for any errors
   */
  //具体的规则实现类需要提供该方法的具体实现。在这个方法中，规则类会根据传入的 ResourceScheduler 配置资源调度器并完成初始化
  public abstract boolean initialize(ResourceScheduler scheduler)
      throws IOException;

  /**
   * Return the scheduler queue name the application should be placed in
   * wrapped in an {@link ApplicationPlacementContext} object.
   *
   * A non <code>null</code> return value places the application in a queue,
   * a <code>null</code> value means the queue is not yet determined. The
   * next {@link PlacementRule} in the list maintained in the
   * {@link PlacementManager} will be executed.
   *
   * @param asc The context of the application created on submission
   * @param user The name of the user submitting the application
   * 
   * @throws YarnException for any error while executing the rule
   * 
   * @return The queue name wrapped in {@link ApplicationPlacementContext} or
   * <code>null</code> if no queue was resolved
   */
  //返回一个 ApplicationPlacementContext 对象，封装了应用程序应该被调度到的队列名称。
  // 如果返回 null，表示该规则没有确定队列，接下来会执行下一个调度规则
  public abstract ApplicationPlacementContext getPlacementForApp(
      ApplicationSubmissionContext asc, String user) throws YarnException;


  /**
   * Return the scheduler queue name the application should be placed in
   * wrapped in an {@link ApplicationPlacementContext} object.
   *
   * A non <code>null</code> return value places the application in a queue,
   * a <code>null</code> value means the queue is not yet determined. The
   * next {@link PlacementRule} in the list maintained in the
   * {@link PlacementManager} will be executed.
   *
   * @param asc The context of the application created on submission
   * @param user The name of the user submitting the application
   * @param recovery Indicates if the submission is a recovery
   *
   * @throws YarnException for any error while executing the rule
   *
   * @return The queue name wrapped in {@link ApplicationPlacementContext} or
   * <code>null</code> if no queue was resolved
   */
  public ApplicationPlacementContext getPlacementForApp(
      ApplicationSubmissionContext asc, String user, boolean recovery)
      throws YarnException {
    return getPlacementForApp(asc, user);
  }
}