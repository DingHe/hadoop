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
import java.util.Arrays;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * An identifier that identifies a token, may contain public information 
 * about a token, including its kind (or type).
 */
//标识一个令牌（Token）。它提供了获取令牌的基本信息、用户身份、以及其他关于令牌的功能。
// 通常，这类令牌会在安全的上下文中使用，比如 Kerberos 认证等，来标识一个特定的用户、服务或者会话
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class TokenIdentifier implements Writable {
  //令牌的跟踪标识符，用于在多个客户端会话之间关联令牌的使用情况
  private String trackingId = null;

  /**
   * Get the token kind
   * @return the kind of the token
   */
  //用于返回令牌的类型，每个实现这个类的子类都会提供具体的令牌类型信息（例如，Hadoop中的访问令牌类型）
  public abstract Text getKind();

  /**
   * Get the Ugi with the username encoded in the token identifier
   * 
   * @return the username. null is returned if username in the identifier is
   *         empty or null.
   */
  //代表了与令牌关联的用户信息
  public abstract UserGroupInformation getUser();

  /**
   * Get the bytes for the token identifier
   * @return the bytes of the identifier
   */
  public byte[] getBytes() {
    DataOutputBuffer buf = new DataOutputBuffer(4096);
    try {
      this.write(buf);
    } catch (IOException ie) {
      throw new RuntimeException("i/o error in getBytes", ie);
    }
    return Arrays.copyOf(buf.getData(), buf.getLength());
  }

  /**
   * Returns a tracking identifier that can be used to associate usages of a
   * token across multiple client sessions.
   *
   * Currently, this function just returns an MD5 of {{@link #getBytes()}.
   *
   * @return tracking identifier
   */
  public String getTrackingId() {
    if (trackingId == null) {
      trackingId = DigestUtils.md5Hex(getBytes());
    }
    return trackingId;
  }
}
