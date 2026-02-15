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
//有限状态机（Finite State Machine, FSM）接口，
// 它定义了状态机的基本行为，包括获取当前状态、获取前一个状态，以及执行状态转换的方法
//STATE：状态枚举，表示系统可能处于的不同状态，例如 RMStateStoreState（ACTIVE、FENCED）。
//EVENTTYPE：事件类型的枚举，表示不同的状态转换事件，例如 RMStateStoreEventType（如 STORE_APP_ATTEMPT、REMOVE_APP）。
//EVENT：事件对象，封装了触发状态转换的具体信息，例如 RMStateStoreEvent
@Public
@Evolving
public interface StateMachine
                 <STATE extends Enum<STATE>,
                  EVENTTYPE extends Enum<EVENTTYPE>, EVENT> {
  public STATE getCurrentState();
  public STATE getPreviousState();
  public STATE doTransition(EVENTTYPE eventType, EVENT event)
        throws InvalidStateTransitionException;
}
