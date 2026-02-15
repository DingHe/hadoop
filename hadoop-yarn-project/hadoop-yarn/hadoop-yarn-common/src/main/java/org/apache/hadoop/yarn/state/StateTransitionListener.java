/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.state;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * A State Transition Listener.
 * It exposes a pre and post transition hook called before and
 * after the transition.
 */
// Hadoop YARN 状态机 (StateMachine) 机制中的 状态转换监听器。
// 它提供了 preTransition（转换前） 和 postTransition（转换后） 两个钩子方法，
// 使外部组件可以在状态转换发生前后执行特定操作，例如日志记录、触发回调、统计分析等
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface StateTransitionListener
    <OPERAND, EVENT, STATE extends Enum<STATE>> {

  /**
   * Pre Transition Hook. This will be called before transition.
   * @param op Operand.
   * @param beforeState State before transition.
   * @param eventToBeProcessed Incoming Event.
   */
  //状态转换之前 被调用
  //记录日志、检查转换前的状态是否符合某些条件、触发前置操作（如资源预分配）
  void preTransition(OPERAND op, STATE beforeState, EVENT eventToBeProcessed);

  /**
   * Post Transition Hook. This will be called after the transition.
   * @param op Operand.
   * @param beforeState State before transition.
   * @param afterState State after transition.
   * @param processedEvent Processed Event.
   */
  //状态转换之后 被调用
  //触发回调、更新数据库、发送通知和执行后置清理操作
  void postTransition(OPERAND op, STATE beforeState, STATE afterState,
      EVENT processedEvent);
}
