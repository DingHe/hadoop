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

package org.apache.hadoop.yarn.server.resourcemanager.amlauncher;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.client.NMProxy;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.server.security.AMSecretKeys;
import org.apache.hadoop.yarn.server.webproxy.ProxyCA;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * The launch of the AM itself.
 */
//AMLauncher类负责在YARN ResourceManager中启动和清理应用程序的Application Master (AM)容器。
// 它会创建AM的容器启动请求并通过NodeManager启动容器，还会在AM容器清理时发送清理请求。
// 该类实现了Runnable接口，允许在独立线程中执行启动和清理操作。它是YARN中的一个核心组件，用于管理Application Master容器的生命周期
public class AMLauncher implements Runnable {

  private static final Logger LOG =
      LoggerFactory.getLogger(AMLauncher.class);
  //用于与容器管理器（Container Manager）进行通信的代理。通过它发送请求来启动或停止容器
  private ContainerManagementProtocol containerMgrProxy;
  //表示当前正在启动或清理的应用程序尝试（RMAppAttempt）。它包含关于当前应用程序执行的详细信息
  private final RMAppAttempt application;
  private final Configuration conf;
  //表示事件的类型（启动或清理）。这个属性决定了该AMLauncher对象执行启动还是清理操作
  private final AMLauncherEventType eventType;
  private final RMContext rmContext;
  //当前应用程序的Application Master容器，包含容器的详细信息
  private final Container masterContainer;
  //指示是否启用Timeline Service V2，用于跟踪和管理作业运行的时间线数据
  private boolean timelineServiceV2Enabled;

  @SuppressWarnings("rawtypes")
  private final EventHandler handler;

  public AMLauncher(RMContext rmContext, RMAppAttempt application,
      AMLauncherEventType eventType, Configuration conf) {
    this.application = application;
    this.conf = conf;
    this.eventType = eventType;
    this.rmContext = rmContext;
    this.handler = rmContext.getDispatcher().getEventHandler();
    this.masterContainer = application.getMasterContainer();
    this.timelineServiceV2Enabled = YarnConfiguration.
        timelineServiceV2Enabled(conf);
  }

  private void connect() throws IOException {
    ContainerId masterContainerID = masterContainer.getId();
    //获取NodeManager的通讯客户端
    containerMgrProxy = getContainerMgrProxy(masterContainerID);
  }
  //主要作用是启动一个 Application Master (AM) 的容器，它通过 NodeManager (NM) 启动容器，并处理可能的错误情况
  private void launch() throws IOException, YarnException {
    //用于初始化 containerMgrProxy，即与 NodeManager 进行 RPC 连接，确保可以发送启动容器的请求
    connect();
    ContainerId masterContainerID = masterContainer.getId();
    ApplicationSubmissionContext applicationContext =
        application.getSubmissionContext();
    LOG.info("Setting up container " + masterContainer
        + " for AM " + application.getAppAttemptId());
    ContainerLaunchContext launchContext =
        createAMContainerLaunchContext(applicationContext, masterContainerID);
    //创建 StartContainerRequest
    StartContainerRequest scRequest =
        StartContainerRequest.newInstance(launchContext,
          masterContainer.getContainerToken());
    List<StartContainerRequest> list = new ArrayList<StartContainerRequest>();
    list.add(scRequest);
    StartContainersRequest allRequests =
        StartContainersRequest.newInstance(list);
    //调用 startContainers() 发送请求
    StartContainersResponse response =
        containerMgrProxy.startContainers(allRequests);
    if (response.getFailedRequests() != null
        && response.getFailedRequests().containsKey(masterContainerID)) {
      Throwable t =
          response.getFailedRequests().get(masterContainerID).deSerialize();
      parseAndThrowException(t);
    } else {
      LOG.info("Done launching container " + masterContainer + " for AM "
          + application.getAppAttemptId());
    }
  }

  private void cleanup() throws IOException, YarnException {
    connect();
    ContainerId containerId = masterContainer.getId();
    List<ContainerId> containerIds = new ArrayList<ContainerId>();
    containerIds.add(containerId);
    StopContainersRequest stopRequest =
        StopContainersRequest.newInstance(containerIds);
    StopContainersResponse response =
        containerMgrProxy.stopContainers(stopRequest);
    if (response.getFailedRequests() != null
        && response.getFailedRequests().containsKey(containerId)) {
      Throwable t = response.getFailedRequests().get(containerId).deSerialize();
      parseAndThrowException(t);
    }
  }

  // Protected. For tests.
  //用于获取一个与节点管理器（NodeManager）通信的代理对象，该代理对象实现了 ContainerManagementProtocol 接口
  protected ContainerManagementProtocol getContainerMgrProxy(
      final ContainerId containerId) {
    //从 masterContainer 获取容器所属节点的 NodeId，然后基于主机和端口构建一个 InetSocketAddress 对象，
    // 表示容器管理器（NodeManager）的连接地址
    final NodeId node = masterContainer.getNodeId();
    final InetSocketAddress containerManagerConnectAddress =
        NetUtils.createSocketAddrForHost(node.getHost(), node.getPort());

    final YarnRPC rpc = getYarnRPC();

    UserGroupInformation currentUser =
        UserGroupInformation.createRemoteUser(containerId
            .getApplicationAttemptId().toString());

    String user =
        rmContext.getRMApps()
            .get(containerId.getApplicationAttemptId().getApplicationId())
            .getUser();
    org.apache.hadoop.yarn.api.records.Token token =
        rmContext.getNMTokenSecretManager().createNMToken(
            containerId.getApplicationAttemptId(), node, user);
    currentUser.addToken(ConverterUtils.convertFromYarn(token,
        containerManagerConnectAddress));
    //建并返回代理对象
    return NMProxy.createNMProxy(conf, ContainerManagementProtocol.class,
        currentUser, rpc, containerManagerConnectAddress);
  }

  @VisibleForTesting
  protected YarnRPC getYarnRPC() {
    return YarnRPC.create(conf);  // TODO: Don't create again and again.
  }

  private ContainerLaunchContext createAMContainerLaunchContext(
      ApplicationSubmissionContext applicationMasterContext,
      ContainerId containerID) throws IOException {

    // Construct the actual Container
    ContainerLaunchContext container =
        applicationMasterContext.getAMContainerSpec();

    if (container == null){
      throw new IOException(containerID +
            " has been cleaned before launched");
    }
    // Finalize the container
    setupTokens(container, containerID);
    // set the flow context optionally for timeline service v.2
    setFlowContext(container);

    return container;
  }

  @Private
  @VisibleForTesting
  protected void setupTokens(
      ContainerLaunchContext container, ContainerId containerID)
      throws IOException {
    Map<String, String> environment = container.getEnvironment();
    environment.put(ApplicationConstants.APPLICATION_WEB_PROXY_BASE_ENV,
        application.getWebProxyBase());
    // Set AppSubmitTime to be consumable by the AM.
    ApplicationId applicationId =
        application.getAppAttemptId().getApplicationId();
    environment.put(
        ApplicationConstants.APP_SUBMIT_TIME_ENV,
        String.valueOf(rmContext.getRMApps()
            .get(applicationId)
            .getSubmitTime()));

    Credentials credentials = new Credentials();
    DataInputByteBuffer dibb = new DataInputByteBuffer();
    ByteBuffer tokens = container.getTokens();
    if (tokens != null) {
      // TODO: Don't do this kind of checks everywhere.
      dibb.reset(tokens);
      credentials.readTokenStorageStream(dibb);
      tokens.rewind();
    }

    // Add AMRMToken
    Token<AMRMTokenIdentifier> amrmToken = createAndSetAMRMToken();
    if (amrmToken != null) {
      credentials.addToken(amrmToken.getService(), amrmToken);
    }

    // Setup Keystore and Truststore
    String httpsPolicy = conf.get(YarnConfiguration.RM_APPLICATION_HTTPS_POLICY,
        YarnConfiguration.DEFAULT_RM_APPLICATION_HTTPS_POLICY);
    if (httpsPolicy.equals("LENIENT") || httpsPolicy.equals("STRICT")) {
      ProxyCA proxyCA = rmContext.getProxyCAManager().getProxyCA();
      try {
        String kPass = proxyCA.generateKeyStorePassword();
        byte[] keyStore = proxyCA.createChildKeyStore(applicationId, kPass);
        credentials.addSecretKey(
            AMSecretKeys.YARN_APPLICATION_AM_KEYSTORE, keyStore);
        credentials.addSecretKey(
            AMSecretKeys.YARN_APPLICATION_AM_KEYSTORE_PASSWORD,
            kPass.getBytes(StandardCharsets.UTF_8));
        String tPass = proxyCA.generateKeyStorePassword();
        byte[] trustStore = proxyCA.getChildTrustStore(tPass);
        credentials.addSecretKey(
            AMSecretKeys.YARN_APPLICATION_AM_TRUSTSTORE, trustStore);
        credentials.addSecretKey(
            AMSecretKeys.YARN_APPLICATION_AM_TRUSTSTORE_PASSWORD,
            tPass.getBytes(StandardCharsets.UTF_8));
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    DataOutputBuffer dob = new DataOutputBuffer();
    credentials.writeTokenStorageToStream(dob);
    container.setTokens(ByteBuffer.wrap(dob.getData(), 0, dob.getLength()));
  }

  private void setFlowContext(ContainerLaunchContext container) {
    if (timelineServiceV2Enabled) {
      Map<String, String> environment = container.getEnvironment();
      ApplicationId applicationId =
          application.getAppAttemptId().getApplicationId();
      RMApp app = rmContext.getRMApps().get(applicationId);

      // initialize the flow in the environment with default values for those
      // that do not specify the flow tags
      // flow name: app name (or app id if app name is missing),
      // flow version: "1", flow run id: start time
      setFlowTags(environment, TimelineUtils.FLOW_NAME_TAG_PREFIX,
          TimelineUtils.generateDefaultFlowName(app.getName(), applicationId));
      setFlowTags(environment, TimelineUtils.FLOW_VERSION_TAG_PREFIX,
          TimelineUtils.DEFAULT_FLOW_VERSION);
      setFlowTags(environment, TimelineUtils.FLOW_RUN_ID_TAG_PREFIX,
          String.valueOf(app.getStartTime()));

      // Set flow context info: the flow context is received via the application
      // tags
      for (String tag : app.getApplicationTags()) {
        String[] parts = tag.split(":", 2);
        if (parts.length != 2 || parts[1].isEmpty()) {
          continue;
        }
        switch (parts[0].toUpperCase()) {
        case TimelineUtils.FLOW_NAME_TAG_PREFIX:
          setFlowTags(environment, TimelineUtils.FLOW_NAME_TAG_PREFIX,
              parts[1]);
          break;
        case TimelineUtils.FLOW_VERSION_TAG_PREFIX:
          setFlowTags(environment, TimelineUtils.FLOW_VERSION_TAG_PREFIX,
              parts[1]);
          break;
        case TimelineUtils.FLOW_RUN_ID_TAG_PREFIX:
          setFlowTags(environment, TimelineUtils.FLOW_RUN_ID_TAG_PREFIX,
              parts[1]);
          break;
        default:
          break;
        }
      }
    }
  }

  private static void setFlowTags(
      Map<String, String> environment, String tagPrefix, String value) {
    if (!value.isEmpty()) {
      environment.put(tagPrefix, value);
    }
  }

  @VisibleForTesting
  protected Token<AMRMTokenIdentifier> createAndSetAMRMToken() {
    Token<AMRMTokenIdentifier> amrmToken =
        this.rmContext.getAMRMTokenSecretManager().createAndGetAMRMToken(
          application.getAppAttemptId());
    ((RMAppAttemptImpl)application).setAMRMToken(amrmToken);
    return amrmToken;
  }

  @SuppressWarnings("unchecked")
  public void run() {
    switch (eventType) {
    case LAUNCH:
      try {
        LOG.info("Launching master" + application.getAppAttemptId());
        launch();
        handler.handle(new RMAppAttemptEvent(application.getAppAttemptId(),
            RMAppAttemptEventType.LAUNCHED, System.currentTimeMillis()));
      } catch(Exception ie) {
        onAMLaunchFailed(masterContainer.getId(), ie);
      }
      break;
    case CLEANUP:
      try {
        LOG.info("Cleaning master " + application.getAppAttemptId());
        cleanup();
      } catch(IOException ie) {
        LOG.info("Error cleaning master ", ie);
      } catch (YarnException e) {
        StringBuilder sb = new StringBuilder("Container ");
        sb.append(masterContainer.getId().toString())
            .append(" is not handled by this NodeManager");
        if (!e.getMessage().contains(sb.toString())) {
          // Ignoring if container is already killed by Node Manager.
          LOG.info("Error cleaning master ", e);
        }
      }
      break;
    default:
      LOG.warn("Received unknown event-type " + eventType + ". Ignoring.");
      break;
    }
  }

  private void parseAndThrowException(Throwable t) throws YarnException,
      IOException {
    if (t instanceof YarnException) {
      throw (YarnException) t;
    } else if (t instanceof InvalidToken) {
      throw (InvalidToken) t;
    } else {
      throw (IOException) t;
    }
  }

  @SuppressWarnings("unchecked")
  protected void onAMLaunchFailed(ContainerId containerId, Exception ie) {
    String message = "Error launching " + application.getAppAttemptId()
            + ". Got exception: " + StringUtils.stringifyException(ie);
    LOG.info(message);
    handler.handle(new RMAppAttemptEvent(application
           .getAppAttemptId(), RMAppAttemptEventType.LAUNCH_FAILED, message));
  }
}
