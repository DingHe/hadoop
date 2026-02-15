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
package org.apache.hadoop.ha;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.retry.Idempotent;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.KerberosInfo;

import java.io.IOException;

/**
 * Protocol interface that provides High Availability related primitives to
 * monitor and fail-over the service.
 * 
 * This interface could be used by HA frameworks to manage the service.
 */
@KerberosInfo(
    serverPrincipal=CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_USER_NAME_KEY)
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface HAServiceProtocol {
  /**
   * Initial version of the protocol
   */
  public static final long versionID = 1L;

  /**
   * An HA service may be in active or standby state. During startup, it is in
   * an unknown INITIALIZING state. During shutdown, it is in the STOPPING state
   * and can no longer return to active/standby states.
   */
  //高可用（HA）服务状态的枚举类型，它定义了一个服务在不同生命周期阶段的可能状态。
  // 这个枚举主要用于表示高可用服务的当前状态，包括服务在初始化、活动、待命等状态下的行为
  public enum HAServiceState {
    INITIALIZING("initializing"),//在启动过程中，服务处于“初始化”状态。此时服务尚未完全启动，可能正在进行配置或初始化工作
    ACTIVE("active"),//服务处于“活动”状态，表示该服务处于主服务状态，能够处理请求或执行主要任务
    STANDBY("standby"),//服务处于“待命”状态，表示该服务是备用的，不处理请求，通常在主服务不可用时会被切换为活动状态
    OBSERVER("observer"),//该状态可能表示服务处于一种观察者模式，它不处理请求，只是观察其他服务的状态，通常用于监控
    STOPPING("stopping");//在服务关闭过程中，服务进入“停止中”状态，此时服务已经不能再返回活动或待命状态，表示正在停止的过程中

    private String name;

    HAServiceState(String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      return name;
    }
  }
  
  public enum RequestSource {
    REQUEST_BY_USER,
    REQUEST_BY_USER_FORCED,
    REQUEST_BY_ZKFC;
  }
  
  /**
   * Information describing the source for a request to change state.
   * This is used to differentiate requests from automatic vs CLI
   * failover controllers, and in the future may include epoch
   * information.
   */
  public static class StateChangeRequestInfo {
    private final RequestSource source;

    public StateChangeRequestInfo(RequestSource source) {
      super();
      this.source = source;
    }

    public RequestSource getSource() {
      return source;
    }
  }

  /**
   * Monitor the health of service. This periodically called by the HA
   * frameworks to monitor the health of the service.
   * 
   * Service is expected to perform checks to ensure it is functional.
   * If the service is not healthy due to failure or partial failure,
   * it is expected to throw {@link HealthCheckFailedException}.
   * The definition of service not healthy is left to the service.
   * 
   * Note that when health check of an Active service fails,
   * failover to standby may be done.
   * 
   * @throws HealthCheckFailedException
   *           if the health check of a service fails.
   * @throws AccessControlException
   *           if access is denied.
   * @throws IOException
   *           if other errors happen
   */
  @Idempotent
  public void monitorHealth() throws HealthCheckFailedException,
                                     AccessControlException,
                                     IOException;

  /**
   * Request service to transition to active state. No operation, if the
   * service is already in active state.
   *
   * @param reqInfo reqInfo.
   * @throws ServiceFailedException
   *           if transition from standby to active fails.
   * @throws AccessControlException
   *           if access is denied.
   * @throws IOException
   *           if other errors happen
   */
  @Idempotent
  public void transitionToActive(StateChangeRequestInfo reqInfo)
                                   throws ServiceFailedException,
                                          AccessControlException,
                                          IOException;

  /**
   * Request service to transition to standby state. No operation, if the
   * service is already in standby state.
   *
   * @param reqInfo reqInfo.
   * @throws ServiceFailedException
   *           if transition from active to standby fails.
   * @throws AccessControlException
   *           if access is denied.
   * @throws IOException
   *           if other errors happen
   */
  @Idempotent
  public void transitionToStandby(StateChangeRequestInfo reqInfo)
                                    throws ServiceFailedException,
                                           AccessControlException,
                                           IOException;

  /**
   * Request service to transition to observer state. No operation, if the
   * service is already in observer state.
   *
   * @param reqInfo reqInfo.
   * @throws ServiceFailedException
   *           if transition from standby to observer fails.
   * @throws AccessControlException
   *           if access is denied.
   * @throws IOException
   *           if other errors happen
   */
  @Idempotent
  void transitionToObserver(StateChangeRequestInfo reqInfo)
                              throws ServiceFailedException,
                                     AccessControlException,
                                     IOException;

  /**
   * Return the current status of the service. The status indicates
   * the current <em>state</em> (e.g ACTIVE/STANDBY) as well as
   * some additional information.
   * 
   * @throws AccessControlException
   *           if access is denied.
   * @throws IOException
   *           if other errors happen
   * @see HAServiceStatus
   * @return HAServiceStatus.
   */
  @Idempotent
  public HAServiceStatus getServiceStatus() throws AccessControlException,
                                                   IOException;
}
