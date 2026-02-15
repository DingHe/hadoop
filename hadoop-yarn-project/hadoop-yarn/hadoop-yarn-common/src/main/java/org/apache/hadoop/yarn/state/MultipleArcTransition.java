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

package org.apache.hadoop.yarn.state;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

/**
 * Hook for Transition. 
 * Post state is decided by Transition hook. Post state must be one of the 
 * valid post states registered in StateMachine.
 */
//定义状态机中多个状态转移的钩子接口。它通过处理状态机中的事件来实现从一个状态到另一个状态的转换。
// 此接口允许状态机根据当前操作数（operand）和触发的事件（event）来决定新的状态。
@Public
@Evolving
public interface MultipleArcTransition
        <OPERAND, EVENT, STATE extends Enum<STATE>> {

  /**
   * Transition hook.
   * @return the postState. Post state must be one of the 
   *                      valid post states registered in StateMachine.
   * @param operand the entity attached to the FSM, whose internal 
   *                state may change.
   * @param event causal event
   */
  //operand：状态机附加的实体，通常是状态机正在跟踪的对象或数据，其内部状态可能会发生变化
  //event：触发状态转换的事件，这个事件通常是状态机外部或内部发生的某种情况，导致状态的改变
  //返回一个新的状态（postState）。这个新的状态必须是状态机中已经注册的有效状态之一
  public STATE transition(OPERAND operand, EVENT event);

}
