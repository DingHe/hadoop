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

package org.apache.hadoop.yarn.security.client;


import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenRenewer;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenRequest;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;

/**
 * Delegation Token Identifier that identifies the delegation tokens from the 
 * Resource Manager. 
 */
//用于识别资源管理器（Resource Manager，RM）生成的委托令牌（delegation token）的类
@Public
@Evolving
public class RMDelegationTokenIdentifier extends YARNDelegationTokenIdentifier {
  //表示该委托令牌的类型名称。此处固定为 "RM_DELEGATION_TOKEN"，用于标识该令牌为 RM 的委托令牌
  public static final Text KIND_NAME = new Text("RM_DELEGATION_TOKEN");

  public RMDelegationTokenIdentifier(){}

  /**
   * Create a new delegation token identifier
   * @param owner the effective username of the token owner
   * @param renewer the username of the renewer
   * @param realUser the real username of the token owner
   */
  public RMDelegationTokenIdentifier(Text owner, Text renewer, Text realUser) {
    super(owner, renewer, realUser);
  }

  @Override
  public Text getKind() {
    return KIND_NAME;
  }
  
  public static class Renewer extends TokenRenewer {

    @Override
    public boolean handleKind(Text kind) {
      return KIND_NAME.equals(kind);
    }

    @Override
    public boolean isManaged(Token<?> token) throws IOException {
      return true;
    }
    //用于存储当前本地的委托令牌管理器实例。该管理器负责处理与 RM 委托令牌相关的操作
    private static
    AbstractDelegationTokenSecretManager<RMDelegationTokenIdentifier> localSecretManager;
    //用于存储本地服务地址。它帮助判断一个令牌是否与当前本地服务相关联
    private static InetSocketAddress localServiceAddress;
    
    @Private
    public static void setSecretManager(
        AbstractDelegationTokenSecretManager<RMDelegationTokenIdentifier> secretManager,
        InetSocketAddress serviceAddress) {
      localSecretManager = secretManager;
      localServiceAddress = serviceAddress;
    }
    
    @SuppressWarnings("unchecked")
    //token - 需要续期的令牌
    //返回值: long - 令牌续期后的过期时间
    @Override
    public long renew(Token<?> token, Configuration conf) throws IOException,
        InterruptedException {
      final ApplicationClientProtocol rmClient = getRmClient(token, conf);
      if (rmClient != null) {
        try {
          //如果可以获取到 RM 客户端 rmClient，则通过该客户端向 RM 发送续期请求，获取新的过期时间
          RenewDelegationTokenRequest request =
              Records.newRecord(RenewDelegationTokenRequest.class);
          request.setDelegationToken(convertToProtoToken(token));
          return rmClient.renewDelegationToken(request).getNextExpirationTime();
        } catch (YarnException e) {
          throw new IOException(e);
        } finally {
          RPC.stopProxy(rmClient);
        }
      } else {
        return localSecretManager.renewToken(
            (Token<RMDelegationTokenIdentifier>)token, getRenewer(token));
      }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void cancel(Token<?> token, Configuration conf) throws IOException,
        InterruptedException {
      final ApplicationClientProtocol rmClient = getRmClient(token, conf);
      if (rmClient != null) {
        try {
          CancelDelegationTokenRequest request =
              Records.newRecord(CancelDelegationTokenRequest.class);
          request.setDelegationToken(convertToProtoToken(token));
          rmClient.cancelDelegationToken(request);
        } catch (YarnException e) {
          throw new IOException(e);
        } finally {
          RPC.stopProxy(rmClient);
        }
      } else {
        localSecretManager.cancelToken(
            (Token<RMDelegationTokenIdentifier>)token, getRenewer(token));
      }
    }
    //返回值: ApplicationClientProtocol - 用于与 RM 交互的客户端协议
    private static ApplicationClientProtocol getRmClient(Token<?> token,
        Configuration conf) throws IOException {
      //获取令牌服务地址 token.getService()，并与本地服务地址进行比较
      String[] services = token.getService().toString().split(",");
      for (String service : services) {
        InetSocketAddress addr = NetUtils.createSocketAddr(service);
        if (localSecretManager != null) {
          // return null if it's our token
          if (localServiceAddress.getAddress().isAnyLocalAddress()) {
            if (NetUtils.isLocalAddress(addr.getAddress()) &&
                addr.getPort() == localServiceAddress.getPort()) {
              //如果令牌对应的服务地址与本地服务地址相同，则返回 null（表示不需要通过 RM 客户端处理）
              return null;
            }
          } else if (addr.equals(localServiceAddress)) {
            return null;
          }
        }
      }
      //创建并返回一个新的 ApplicationClientProtocol 实例，表示可以通过该客户端与 RM 进行交互
      return ClientRMProxy.createRMProxy(conf, ApplicationClientProtocol.class);
    }

    // get renewer so we can always renew our own tokens
    @SuppressWarnings("unchecked")
    private static String getRenewer(Token<?> token) throws IOException {
      return ((Token<RMDelegationTokenIdentifier>)token).decodeIdentifier()
          .getRenewer().toString();
    }
    
    private static org.apache.hadoop.yarn.api.records.Token
        convertToProtoToken(Token<?> token) {
      return org.apache.hadoop.yarn.api.records.Token.newInstance(
        token.getIdentifier(), token.getKind().toString(), token.getPassword(),
        token.getService().toString());
    }
  }
}
