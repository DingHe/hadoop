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

import java.nio.ByteBuffer;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p><code>Token</code> is the security entity used by the framework
 * to verify authenticity of any resource.</p>
 */
//用于身份验证和安全性的令牌（Token）类。它用于标识和验证用户或进程的权限，以确保 YARN 资源的安全访问。
// Token 本质上是一种凭证，YARN 通过它来验证访问特定资源的合法性，例如作业提交、资源分配等
//身份验证：YARN 通过令牌验证用户或进程的身份
//访问控制：确保只有拥有正确令牌的用户或进程才能访问受保护的资源
//分布式安全：在分布式计算环境中，令牌用于不同组件（如 ResourceManager 和 NodeManager）之间的安全通信
@Public
@Stable
public abstract class Token {

  @Private
  @Unstable
  public static Token newInstance(byte[] identifier, String kind, byte[] password,
      String service) {
    Token token = Records.newRecord(Token.class);
    token.setIdentifier(ByteBuffer.wrap(identifier));//令牌的唯一标识符（ID），用于标识某个特定的令牌
    token.setKind(kind);//令牌的类型，例如 "YARN Delegation Token"，用于区分不同类型的令牌。
    token.setPassword(ByteBuffer.wrap(password));//令牌的密码（密钥），用于验证令牌的有效性。
    token.setService(service);//令牌关联的服务，例如 ResourceManager（RM）或 NodeManager（NM）
    return token;
  }

  /**
   * Get the token identifier.
   * @return token identifier
   */
  @Public
  @Stable
  public abstract ByteBuffer getIdentifier();
  
  @Private
  @Unstable
  public abstract void setIdentifier(ByteBuffer identifier);

  /**
   * Get the token password
   * @return token password
   */
  @Public
  @Stable
  public abstract ByteBuffer getPassword();
  
  @Private
  @Unstable
  public abstract void setPassword(ByteBuffer password);

  /**
   * Get the token kind.
   * @return token kind
   */
  @Public
  @Stable
  public abstract String getKind();
  
  @Private
  @Unstable
  public abstract void setKind(String kind);

  /**
   * Get the service to which the token is allocated.
   * @return service to which the token is allocated
   */
  @Public
  @Stable
  public abstract String getService();

  @Private
  @Unstable
  public abstract void setService(String service);

}
