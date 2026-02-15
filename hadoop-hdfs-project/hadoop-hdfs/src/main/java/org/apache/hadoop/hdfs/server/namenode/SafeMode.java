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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.classification.InterfaceAudience;
//定义了与 HDFS 安全模式（Safe Mode）相关的操作。安全模式是一种特殊的状态，HDFS 中的 NameNode 会进入该状态来进行某些必要的检查，例如检查系统中是否有足够的副本，或者系统是否健康
/** SafeMode related operations. */
@InterfaceAudience.Private
public interface SafeMode {
  /** Is the system in safe mode? */
  public boolean isInSafeMode();//判断当前 HDFS 系统是否处于安全模式

  /**
   * Is the system in startup safe mode, i.e. the system is starting up with
   * safe mode turned on automatically?
   */
  public boolean isInStartupSafeMode();//判断系统是否处于启动时的安全模式
}
