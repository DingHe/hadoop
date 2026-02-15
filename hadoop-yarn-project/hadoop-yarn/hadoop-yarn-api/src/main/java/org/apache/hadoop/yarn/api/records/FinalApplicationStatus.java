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

package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;

/** 表示 YARN 应用程序的最终状态。它用于描述一个应用程序在完成生命周期后（无论成功或失败）所处的状态
 * Enumeration of various final states of an <code>Application</code>.
 */
@Public
@Stable
public enum FinalApplicationStatus {
  //未定义状态，表示应用程序尚未完成或其状态无法确定，通常应用程序在运行过程中处于此状态
  /** Undefined state when either the application has not yet finished */
  UNDEFINED,
  //成功状态，表示应用程序成功完成。它的任务没有失败，最终成功退出
  /** Application which finished successfully. */
  SUCCEEDED,
  //失败状态，表示应用程序执行失败。通常由于任务的失败、资源不足或应用本身的问题导致应用退出
  /** Application which failed. */
  FAILED,
  //被杀死状态，表示应用程序被用户或管理员强制终止。可能是由于手动取消、资源问题、超时等
  /** Application which was terminated by a user or admin. */
  KILLED,
 //结束状态，表示应用程序有子任务，并且这些子任务具有多个最终状态（例如，部分成功或失败）
  /** Application which has subtasks with multiple end states. */
  ENDED
}
