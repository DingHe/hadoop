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

package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.IOException;

/**
 * Leader election implementation that uses Curator.
 */
//ResourceManager（RM） 的领导选举服务，基于 Apache Curator 框架的 LeaderLatch 机制实现 高可用（HA）选举。
//主要作用：
//通过 Zookeeper 进行 RM 选举，确保集群中只有一个 Active（活跃）RM，其余 RM 处于 Standby（备用）状态。
//自动故障转移，当 Active RM 失效时，Standby RM 重新参与选举，选出新的 Active RM。
//管理 Zookeeper 连接状态，维护 RM 之间的协调和角色转换
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class CuratorBasedElectorService extends AbstractService
    implements EmbeddedElector, LeaderLatchListener {
  public static final Logger LOG =
      LoggerFactory.getLogger(CuratorBasedElectorService.class);
  //Curator 提供的 LeaderLatch 机制，用于 管理 RM 选举
  private LeaderLatch leaderLatch;
  //Curator 客户端，用于 与 Zookeeper 通信，管理 RM 选举的持久节点
  private CuratorFramework curator;
  //Zookeeper 选举的路径，所有 RM 通过此路径进行领导选举
  private String latchPath;
  //当前 RM 的 ID，用于在 Zookeeper 选举中标识不同的 RM 实例
  private String rmId;
  private ResourceManager rm;

  public CuratorBasedElectorService(ResourceManager rm) {
    super(CuratorBasedElectorService.class.getName());
    this.rm = rm;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    //获取 当前 RM 的 ID，用于 Zookeeper 选举
    rmId = HAUtil.getRMHAId(conf);
    String clusterId = YarnConfiguration.getClusterId(conf);
    //计算 Zookeeper 选举路径
    String zkBasePath = conf.get(
        YarnConfiguration.AUTO_FAILOVER_ZK_BASE_PATH,
        YarnConfiguration.DEFAULT_AUTO_FAILOVER_ZK_BASE_PATH);
    latchPath = zkBasePath + "/" + clusterId;
    curator = rm.getCurator();
    initAndStartLeaderLatch();
    super.serviceInit(conf);
  }
  //初始化并启动 LeaderLatch
  private void initAndStartLeaderLatch() throws Exception {
    //curator（Zookeeper 客户端）
    //latchPath（Zookeeper 选举路径）
    //rmId（当前 RM 的 ID）
    leaderLatch = new LeaderLatch(curator, latchPath, rmId);
    leaderLatch.addListener(this);
    leaderLatch.start();
  }

  @Override
  protected void serviceStop() throws Exception {
    closeLeaderLatch();
    super.serviceStop();
  }

  @Override
  public void rejoinElection() {
    try {
      closeLeaderLatch();
      Thread.sleep(1000);
      initAndStartLeaderLatch();
    } catch (Exception e) {
      LOG.info("Fail to re-join election.", e);
    }
  }

  @Override
  public String getZookeeperConnectionState() {
    return "Connected to zookeeper : " +
        curator.getZookeeperClient().isConnected();
  }

  @Override
  public void isLeader() {
    LOG.info(rmId + "is elected leader, transitioning to active");
    try {
      rm.getRMContext().getRMAdminService()
          .transitionToActive(
          new HAServiceProtocol.StateChangeRequestInfo(
              HAServiceProtocol.RequestSource.REQUEST_BY_ZKFC));
    } catch (Exception e) {
      LOG.info(rmId + " failed to transition to active, giving up leadership",
          e);
      notLeader();
      rejoinElection();
    }
  }

  private void closeLeaderLatch() throws IOException {
    if (leaderLatch != null) {
      leaderLatch.close();
    }
  }

  @Override
  public void notLeader() {
    LOG.info(rmId + " relinquish leadership");
    try {
      rm.getRMContext().getRMAdminService()
          .transitionToStandby(
          new HAServiceProtocol.StateChangeRequestInfo(
              HAServiceProtocol.RequestSource.REQUEST_BY_ZKFC));
    } catch (Exception e) {
      LOG.info(rmId + " did not transition to standby successfully.");
    }
  }

  // only for testing
  @VisibleForTesting
  public CuratorFramework getCuratorClient() {
    return this.curator;
  }
}
