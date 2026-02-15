/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.service;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

/**
 * Implements the service state model.
 */
// Hadoop Common 服务框架中用于实现服务生命周期状态机的核心组件
// 定义和管理服务状态：它跟踪一个服务（实现了 Service 接口的组件）当前所处的生命周期状态（如 NOTINITED, INITED, STARTED, STOPPED）
// 强制执行状态转移规则：它内置了一个状态转移矩阵 (statemap)，严格定义了服务状态之间合法的转换路径。任何尝试进行不合法状态转移的操作都将被阻止并抛出异常，确保了服务的生命周期管理是健壮和可预测的
// 提供线程安全的状态转换：通过使用 synchronized 关键字，确保在多线程环境下，服务状态的改变是原子性的
@Public
@Evolving
public class ServiceStateModel {

  /**
   * Map of all valid state transitions
   * [current] [proposed1, proposed2, ...]
   */
  //状态转移
  //定义了所有允许（true）和禁止（false）的服务状态转换。矩阵的行索引代表当前状态，列索引代表提议的新状态
  //uninited 可以进入 inited stopped状态
  //inited 可以进进入started stopped状态
  //started 可以进入 stopped状态
  private static final boolean[][] statemap =
    {
      //                uninited inited started stopped
      /* uninited  */    {false, true,  false,  true},
      /* inited    */    {false, true,  true,   true},
      /* started   */    {false, false, true,   true},
      /* stopped   */    {false, false, false,  true},
    };

  /**
   * The state of the service
   */
  // 存储服务当前的生命周期状态
  private volatile Service.STATE state;

  /**
   * The name of the service: used in exceptions
   */
  //存储拥有此状态模型的服务的名称
  private String name;

  /**
   * Create the service state model in the {@link Service.STATE#NOTINITED}
   * state.
   *
   * @param name input name.
   */
  public ServiceStateModel(String name) {
    this(name, Service.STATE.NOTINITED);
  }

  /**
   * Create a service state model instance in the chosen state
   * @param state the starting state
   * @param name input name.
   */
  public ServiceStateModel(String name, Service.STATE state) {
    this.state = state;
    this.name = name;
  }

  /**
   * Query the service state. This is a non-blocking operation.
   * @return the state
   */
  public Service.STATE getState() {
    return state;
  }

  /**
   * Query that the state is in a specific state
   * @param proposed proposed new state
   * @return the state
   */
  //检查当前状态是否与传入的 proposed 状态相等
  public boolean isInState(Service.STATE proposed) {
    return state.equals(proposed);
  }

  /**
   * Verify that that a service is in a given state.
   * @param expectedState the desired state
   * @throws ServiceStateException if the service state is different from
   * the desired state
   */
  //验证服务当前状态是否与期望的 expectedState 一致
  public void ensureCurrentState(Service.STATE expectedState) {
    if (state != expectedState) {
      throw new ServiceStateException(name+ ": for this operation, the " +
                                      "current service state must be "
                                      + expectedState
                                      + " instead of " + state);
    }
  }

  /**
   * Enter a state -thread safe.
   *
   * @param proposed proposed new state
   * @return the original state
   * @throws ServiceStateException if the transition is not permitted
   */
  //线程安全地尝试将服务状态从当前状态切换到 proposed 状态
  public synchronized Service.STATE enterState(Service.STATE proposed) {
    checkStateTransition(name, state, proposed);
    Service.STATE oldState = state;
    //atomic write of the new state
    state = proposed;
    return oldState;
  }

  /**
   * Check that a state tansition is valid and
   * throw an exception if not
   * @param name name of the service (can be null)
   * @param state current state
   * @param proposed proposed new state
   */
  //检查从 state 到 proposed 的转换是否合法
  public static void checkStateTransition(String name,
                                          Service.STATE state,
                                          Service.STATE proposed) {
    if (!isValidStateTransition(state, proposed)) {
      throw new ServiceStateException(name + " cannot enter state "
                                      + proposed + " from state " + state);
    }
  }

  /**
   * Is a state transition valid?
   * There are no checks for current==proposed
   * as that is considered a non-transition.
   *
   * using an array kills off all branch misprediction costs, at the expense
   * of cache line misses.
   *
   * @param current current state
   * @param proposed proposed new state
   * @return true if the transition to a new state is valid
   */
  //检查从 current 状态到 proposed 状态的转换是否在 statemap 矩阵中被允许
  public static boolean isValidStateTransition(Service.STATE current,
                                               Service.STATE proposed) {
    boolean[] row = statemap[current.getValue()];
    return row[proposed.getValue()];
  }

  /**
   * return the state text as the toString() value
   * @return the current state's description
   */
  @Override
  public String toString() {
    return (name.isEmpty() ? "" : ((name) + ": "))
            + state.toString();
  }

}
