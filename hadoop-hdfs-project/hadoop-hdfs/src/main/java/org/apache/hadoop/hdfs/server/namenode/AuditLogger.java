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
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;

import java.net.InetAddress;

/**用于定义审计日志记录器的接口，主要用于记录 NameNode 执行的关键操作的审计日志。
 此接口允许用户实现自定义的审计日志记录器，并在 HDFS 操作过程中对重要事件进行监控和记录
 审计日志记录的目的包括：
 记录用户在 HDFS 中执行的操作，便于事后审计和问题排查。
 跟踪重要的文件系统更改，如文件创建、删除、权限修改等操作。
 提供安全合规性，确保用户操作可追溯性。
 * Interface defining an audit logger.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface AuditLogger {

  /**
   * Called during initialization of the logger.
   * 在审计日志记录器初始化时调用，提供配置对象以便初始化日志记录器的内部状态或设置必要的资源
   * @param conf The configuration object.
   */
  void initialize(Configuration conf);

  /**
   * Called to log an audit event.
   * <p>
   * This method must return as quickly as possible, since it's called
   * in a critical section of the NameNode's operation.
   *
   * @param succeeded Whether authorization succeeded.  表示操作是否成功
   * @param userName Name of the user executing the request.  执行该操作的用户名称
   * @param addr Remote address of the request. 请求的远程地址（用户的 IP 地址）
   * @param cmd The requested command.  用户请求执行的命令（例如 create, delete, setPermission 等）
   * @param src Path of affected source file.  操作涉及的源文件路径
   * @param dst Path of affected destination file (if any). 操作涉及的目标文件路径（如果有，适用于重命名或移动操作）
   * @param stat File information for operations that change the file's
   *             metadata (permissions, owner, times, etc). 记录操作涉及的文件状态信息（包括权限、所有者、时间戳等）
   */
  //记录 HDFS 操作的审计信息，便于追踪和分析用户对文件系统的访问行为
  //确保每个关键操作都被准确记录，包括成功和失败的情况
  void logAuditEvent(boolean succeeded, String userName,
      InetAddress addr, String cmd, String src, String dst,
      FileStatus stat);

}
