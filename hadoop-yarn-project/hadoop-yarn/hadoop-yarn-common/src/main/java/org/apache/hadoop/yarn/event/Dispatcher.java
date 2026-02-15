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

package org.apache.hadoop.yarn.event;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

/**
 * Event Dispatcher interface. It dispatches events to registered 
 * event handlers based on event types.
 * 
 */
//将不同类型的事件（Event）分发到对应的事件处理器（EventHandler）。
//这个接口允许通过注册特定的事件类型及其处理器，使得事件可以在系统中流转并被正确处理
@Public
@Evolving
public interface Dispatcher {
  //通过这个方法，外部可以获取一个事件处理器（EventHandler），用于接收并处理事件
  EventHandler<Event> getEventHandler();
  //将特定类型的事件（eventType）和对应的事件处理器（handler）注册到分发器中
  void register(Class<? extends Enum> eventType, EventHandler handler);

}
