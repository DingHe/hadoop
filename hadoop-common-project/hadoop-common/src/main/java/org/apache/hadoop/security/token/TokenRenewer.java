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

package org.apache.hadoop.security.token;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

/**
 * This is the interface for plugins that handle tokens.
 */
//定义了一个处理令牌续期和取消的插件接口。
// 它用于管理和续期 Hadoop 集群中的令牌，以确保令牌在其有效期内能够被有效地续期或者取消。
// 这个类的实现由具体的插件提供，插件负责处理不同类型的令牌
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class TokenRenewer {

  /**
   * Does this renewer handle this kind of token?
   * @param kind the kind of the token
   * @return true if this renewer can renew it
   */
  //kind：表示令牌的种类（例如，HDFS令牌、YARN令牌等）
  //此方法用于判断当前的 TokenRenewer 实例是否能够处理某种类型的令牌
  public abstract boolean handleKind(Text kind);

  /**
   * Is the given token managed? Only managed tokens may be renewed or
   * cancelled.
   * @param token the token being checked
   * @return true if the token may be renewed or cancelled
   * @throws IOException raised on errors performing I/O.
   */
  //用于检查给定的令牌是否被管理。只有被管理的令牌才能进行续期或者取消
  public abstract boolean isManaged(Token<?> token) throws IOException;

    /**
     * Renew the given token.
     *
     * @param token the token being checked.
     * @param conf configuration.
     *
     * @return the new expiration time.
     * @throws IOException raised on errors performing I/O.
     * @throws InterruptedException thrown when a thread is waiting, sleeping,
     *                              or otherwise occupied, and the thread is interrupted,
     *                              either before or during the activity.
     */
    //用于续期给定的令牌，并返回续期后的令牌过期时间
  public abstract long renew(Token<?> token,
                             Configuration conf
                             ) throws IOException, InterruptedException;

    /**
     * Cancel the given token.
     *
     * @param token the token being checked.
     * @param conf configuration.
     *
     * @throws IOException raised on errors performing I/O.
     * @throws InterruptedException thrown when a thread is waiting, sleeping,
     *                              or otherwise occupied, and the thread is interrupted,
     *                              either before or during the activity.
     */
    //取消给定的令牌
  public abstract void cancel(Token<?> token,
                              Configuration conf
                              ) throws IOException, InterruptedException;
}
