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
package org.apache.hadoop.yarn.server.api;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/** An Interface that can retrieve local directories to read from or write to.
 *  Components can implement this interface to link it to
 *  their own Directory Handler Service
 */
// 主要作用是为 YARN 辅助服务（Auxiliary Services） 提供一个标准化的机制来管理和访问本地磁盘路径
// 在 YARN 中，辅助服务（如 Spark 的 Shuffle Service）需要在 NodeManager 上的本地目录（yarn.nodemanager.local-dirs）中读写数据。
// 这个接口充当了一个目录抽象层，允许不同的组件实现自己的目录处理逻辑（例如，选择负载最小的磁盘、处理磁盘故障等），而无需辅助服务直接关心底层磁盘管理细节
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface AuxiliaryLocalPathHandler {
  /**
   * Get a path from the local FS for reading for a given Auxiliary Service.
   * @param path the requested path
   * @return the complete path to the file on a local disk
   * @throws IOException if the file read encounters a problem
   */
  //用于请求一个单个的本地文件系统路径，以便辅助服务可以从该路径读取数据。这通常用于读取 NodeManager 或其他进程之前写入的文件
  Path getLocalPathForRead(String path) throws IOException;

  /**
   * Get a path from the local FS for writing for a given Auxiliary Service.
   * @param path the requested path
   * @return the complete path to the file on a local disk
   * @throws IOException if the path creations fails
   */
  //用于请求一个单个的本地文件系统路径，以便辅助服务可以向该路径写入新文件。实现类将根据负载均衡或磁盘健康状况，从配置的本地目录中选择一个合适的路径
  Path getLocalPathForWrite(String path) throws IOException;

  /**
   * Get a path from the local FS for writing a file of an estimated size
   * for a given Auxiliary Service.
   * @param path the requested path
   * @param size the size of the file that is going to be written
   * @return the complete path to the file on a local disk
   * @throws IOException if the path creations fails
   */
  //重载方法。除了提供相对路径外，还允许辅助服务提供预估的文件大小 (size)。实现类可以利用这个大小信息，智能地选择有足够空间的磁盘进行写入
  Path getLocalPathForWrite(String path, long size) throws IOException;

  /**
   * Get all paths from the local FS for reading for a given Auxiliary Service.
   * @param path the requested path
   * @return the complete path list to the file on a local disk as an Iterable
   * @throws IOException if the file read encounters a problem
   */
  //用于请求一个包含所有潜在本地文件系统路径的列表，以便辅助服务可以从所有路径中读取数据。这通常用于读取分布在所有配置本地目录中的数据（例如，如果数据可能有副本或需要在所有目录中搜索）
  Iterable<Path> getAllLocalPathsForRead(String path) throws IOException;
}
