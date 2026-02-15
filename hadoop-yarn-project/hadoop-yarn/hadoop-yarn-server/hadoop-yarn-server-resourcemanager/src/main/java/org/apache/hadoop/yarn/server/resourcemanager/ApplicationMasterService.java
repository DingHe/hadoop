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

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.ams.ApplicationMasterServiceProcessor;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.client.AMRMClientUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException;
import org.apache.hadoop.yarn.exceptions.ApplicationMasterNotRegisteredException;
import org.apache.hadoop.yarn.exceptions.InvalidApplicationMasterRequestException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.processor.AbstractPlacementProcessor;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.processor.DisabledPlacementProcessor;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.processor.PlacementConstraintProcessor;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.processor.SchedulerPlacementProcessor;
import org.apache.hadoop.yarn.server.resourcemanager.security.AMRMTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.authorize.RMPolicyProvider;
import org.apache.hadoop.yarn.server.security.MasterKeyData;
import org.apache.hadoop.yarn.server.utils.YarnServerSecurityUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

import org.apache.hadoop.classification.VisibleForTesting;
//是 Apache Hadoop YARN 资源管理器（ResourceManager，简称 RM）的一部分，负责处理 ApplicationMaster（简称 AM）和 RM 之间的通信。
//AM 通过 AMS 向 RM 注册、请求资源分配、报告任务完成等。AMS 主要实现了 ApplicationMasterProtocol 接口，为 AM 提供了以下核心功能：
//处理 AM 的注册请求 (RegisterApplicationMaster)。
//处理 AM 的资源请求 (Allocate)，涉及到资源分配、容器状态更新等。
//处理 AM 的终止 (FinishApplicationMaster) 请求。
//维护 AM 的存活性，确保 RM 能够监控 AM 的状态。
@SuppressWarnings("unchecked")
@Private
public class ApplicationMasterService extends AbstractService implements
    ApplicationMasterProtocol {
  private static final Logger LOG = LoggerFactory.
      getLogger(ApplicationMasterService.class);
  //监控AM的存活状态，如果AM长时间未发送心跳，会触发失败处理。
  private final AMLivelinessMonitor amLivelinessMonitor;
  //资源调度器，负责资源分配和调度
  private YarnScheduler rScheduler;
  //AMS 监听的地址。
  protected InetSocketAddress masterServiceAddress;
  protected Server server;
  //用于创建 YARN 记录对象。
  protected final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);
  //存储AM资源请求的响应锁，用于并发控制。
  private final ConcurrentMap<ApplicationAttemptId, AllocateResponseLock> responseMap =
      new ConcurrentHashMap<ApplicationAttemptId, AllocateResponseLock>();
  //缓存已经完成的 AM 尝试，防止重复处理。
  private final ConcurrentHashMap<ApplicationAttemptId, Boolean>
      finishedAttemptCache = new ConcurrentHashMap<>();
  protected final RMContext rmContext;
  //处理 AM 相关请求的责任链，可扩展不同处理逻辑
  private final AMSProcessingChain amsProcessingChain;
  //是否启用 Timeline Service v2 进行应用历史记录存储
  private boolean timelineServiceV2Enabled;

  public ApplicationMasterService(RMContext rmContext,
      YarnScheduler scheduler) {
    this(ApplicationMasterService.class.getName(), rmContext, scheduler);
  }

  public ApplicationMasterService(String name, RMContext rmContext,
      YarnScheduler scheduler) {
    super(name);
    this.amLivelinessMonitor = rmContext.getAMLivelinessMonitor();
    this.rScheduler = scheduler;
    this.rmContext = rmContext;
    this.amsProcessingChain = new AMSProcessingChain(new DefaultAMSProcessor());
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    masterServiceAddress = conf.getSocketAddr(
        YarnConfiguration.RM_BIND_HOST,
        YarnConfiguration.RM_SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT);
    initializeProcessingChain(conf);
  }
  // 用于根据 YARN 配置选择适当的调度约束处理器（Placement Constraint Processor），
  // 并将其添加到 amsProcessingChain 处理链中。这决定了 ApplicationMasterService 如何处理资源调度请求
  private void addPlacementConstraintHandler(Configuration conf) {
    //获取调度约束处理器的配置
    String placementConstraintsHandler =
        conf.get(YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_HANDLER,
            YarnConfiguration.DISABLED_RM_PLACEMENT_CONSTRAINTS_HANDLER);
    if (placementConstraintsHandler
        .equals(YarnConfiguration.DISABLED_RM_PLACEMENT_CONSTRAINTS_HANDLER)) {
      //禁用调度约束处理器
      LOG.info(YarnConfiguration.DISABLED_RM_PLACEMENT_CONSTRAINTS_HANDLER
          + " placement handler will be used, all scheduling requests will "
          + "be rejected.");
      amsProcessingChain.addProcessor(new DisabledPlacementProcessor());
    } else if (placementConstraintsHandler
        .equals(YarnConfiguration.PROCESSOR_RM_PLACEMENT_CONSTRAINTS_HANDLER)) {
      //如果配置为 PROCESSOR_RM_PLACEMENT_CONSTRAINTS_HANDLER，则使用 PlacementConstraintProcessor 处理器，
      // 该处理器会根据特定的约束规则进行资源调度
      LOG.info(YarnConfiguration.PROCESSOR_RM_PLACEMENT_CONSTRAINTS_HANDLER
          + " placement handler will be used. Scheduling requests will be "
          + "handled by the placement constraint processor");
      amsProcessingChain.addProcessor(new PlacementConstraintProcessor());
    } else if (placementConstraintsHandler
        .equals(YarnConfiguration.SCHEDULER_RM_PLACEMENT_CONSTRAINTS_HANDLER)) {
      //如果配置为 SCHEDULER_RM_PLACEMENT_CONSTRAINTS_HANDLER，则使用 SchedulerPlacementProcessor，该处理器直接交由 YARN 的主调度器进行调度
      LOG.info(YarnConfiguration.SCHEDULER_RM_PLACEMENT_CONSTRAINTS_HANDLER
          + " placement handler will be used. Scheduling requests will be "
          + "handled by the main scheduler.");
      amsProcessingChain.addProcessor(new SchedulerPlacementProcessor());
    }
  }
 //用于初始化 ApplicationMasterService 的处理链（amsProcessingChain）。该方法会根据配置来设置调度约束处理器，并将配置中定义的处理器按顺序添加到处理链中。
 // 如果发现处理器是 PlacementProcessor，则会忽略并输出警告，因为它应该通过专门的配置项来设置
  private void initializeProcessingChain(Configuration conf) {
    amsProcessingChain.init(rmContext, null);
    //添加调度约束处理器
    addPlacementConstraintHandler(conf);
    //获取配置中的处理器列表
    List<ApplicationMasterServiceProcessor> processors = getProcessorList(conf);
    if (processors != null) {
      Collections.reverse(processors);
      for (ApplicationMasterServiceProcessor p : processors) {
        // Ensure only single instance of PlacementProcessor is included
        //如果是 AbstractPlacementProcessor 类型的处理器，输出警告信息并跳过该处理器。
        // 原因是 PlacementProcessor 应该通过专门的配置项 RM_PLACEMENT_CONSTRAINTS_HANDLER 来处理，
        // 而不是直接在 RM_APPLICATION_MASTER_SERVICE_PROCESSORS 配置项中定义
        if (p instanceof AbstractPlacementProcessor) {
          LOG.warn("Found PlacementProcessor=" + p.getClass().getCanonicalName()
              + " defined in "
              + YarnConfiguration.RM_APPLICATION_MASTER_SERVICE_PROCESSORS
              + ", however PlacementProcessor handler should be configured "
              + "by using " + YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_HANDLER
              + ", this processor will be ignored.");
          continue;
        }
        this.amsProcessingChain.addProcessor(p);
      }
    }
  }
  // 从YARN 配置中获取一个列表，列出需要添加到 ApplicationMasterService 中的所有 ApplicationMasterServiceProcessor 实例。
  // 这些处理器将用于在应用程序的生命周期中处理与 ApplicationMaster 相关的不同任务
  protected List<ApplicationMasterServiceProcessor> getProcessorList(
      Configuration conf) {
    return conf.getInstances(
        YarnConfiguration.RM_APPLICATION_MASTER_SERVICE_PROCESSORS,
        ApplicationMasterServiceProcessor.class);
  }

  @Override
  protected void serviceStart() throws Exception {
    Configuration conf = getConfig();
    YarnRPC rpc = YarnRPC.create(conf);

    Configuration serverConf = conf;
    // If the auth is not-simple, enforce it to be token-based.
    serverConf = new Configuration(conf);
    serverConf.set(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        SaslRpcServer.AuthMethod.TOKEN.toString());
    this.server = getServer(rpc, serverConf, masterServiceAddress,
        this.rmContext.getAMRMTokenSecretManager());
    // TODO more exceptions could be added later.
    this.server.addTerseExceptions(
        ApplicationMasterNotRegisteredException.class);

    // Enable service authorization?
    if (conf.getBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, 
        false)) {
      InputStream inputStream =
          this.rmContext.getConfigurationProvider()
              .getConfigurationInputStream(conf,
                  YarnConfiguration.HADOOP_POLICY_CONFIGURATION_FILE);
      if (inputStream != null) {
        conf.addResource(inputStream);
      }
      refreshServiceAcls(conf, RMPolicyProvider.getInstance());
    }

    this.server.start();
    this.masterServiceAddress =
        conf.updateConnectAddr(YarnConfiguration.RM_BIND_HOST,
                               YarnConfiguration.RM_SCHEDULER_ADDRESS,
                               YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS,
                               server.getListenerAddress());
    this.timelineServiceV2Enabled = YarnConfiguration.
        timelineServiceV2Enabled(conf);

    super.serviceStart();
  }

  protected Server getServer(YarnRPC rpc, Configuration serverConf,
      InetSocketAddress addr, AMRMTokenSecretManager secretManager) {
    return rpc.getServer(ApplicationMasterProtocol.class, this, addr,
        serverConf, secretManager,
        serverConf.getInt(YarnConfiguration.RM_SCHEDULER_CLIENT_THREAD_COUNT,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_CLIENT_THREAD_COUNT));
  }

  protected AMSProcessingChain getProcessingChain() {
    return this.amsProcessingChain;
  }

  @Private
  public InetSocketAddress getBindAddress() {
    return this.masterServiceAddress;
  }
  //用于注册应用程序的 ApplicationMaster。它是资源管理器服务端用来处理来自 ApplicationMaster 的注册请求的关键方法。
  // 注册时会验证请求的合法性、检查是否已注册、处理注册过程中与资源相关的逻辑，并将注册结果返回
  //request：RegisterApplicationMasterRequest 类型的请求对象，包含了应用程序主节点（AM）在注册时所需的所有信息，如主机名、端口等
  @Override
  public RegisterApplicationMasterResponse registerApplicationMaster(
      RegisterApplicationMasterRequest request) throws YarnException,
      IOException {

    AMRMTokenIdentifier amrmTokenIdentifier =
        YarnServerSecurityUtils.authorizeRequest();//对请求进行授权，确保请求来自有效的应用程序
    //获取应用程序的尝试 ID（applicationAttemptId）和应用程序 ID（appID），这是应用程序在注册过程中需要用到的标识
    ApplicationAttemptId applicationAttemptId =
        amrmTokenIdentifier.getApplicationAttemptId();

    ApplicationId appID = applicationAttemptId.getApplicationId();
    AllocateResponseLock lock = responseMap.get(applicationAttemptId);
    //无法找到应用程序
    if (lock == null) {
      RMAuditLogger.logFailure(this.rmContext.getRMApps().get(appID).getUser(),
          AuditConstants.REGISTER_AM, "Application doesn't exist in cache "
              + applicationAttemptId, "ApplicationMasterService",
          "Error in registering application master", appID,
          applicationAttemptId);
      throwApplicationDoesNotExistInCacheException(applicationAttemptId);
    }

    // Allow only one thread in AM to do registerApp at a time.
    synchronized (lock) {
      AllocateResponse lastResponse = lock.getAllocateResponse();
      //如果已经注册且不允许重新注册
      if (hasApplicationMasterRegistered(applicationAttemptId)) {
        // allow UAM re-register if work preservation is enabled
        ApplicationSubmissionContext appContext =
            rmContext.getRMApps().get(appID).getApplicationSubmissionContext();
        if (!(appContext.getUnmanagedAM()
            && appContext.getKeepContainersAcrossApplicationAttempts())) {
          String message =
              AMRMClientUtils.APP_ALREADY_REGISTERED_MESSAGE + appID;
          LOG.warn(message);
          RMAuditLogger.logFailure(
              this.rmContext.getRMApps().get(appID).getUser(),
              AuditConstants.REGISTER_AM, "", "ApplicationMasterService",
              message, appID, applicationAttemptId);
          throw new InvalidApplicationMasterRequestException(message);
        }
      }
      //更新 AM 活跃监控
      this.amLivelinessMonitor.receivedPing(applicationAttemptId);

      // Setting the response id to 0 to identify if the
      // application master is register for the respective attemptid
      //设置响应 ID 和处理请求
      lastResponse.setResponseId(0);
      lock.setAllocateResponse(lastResponse);

      RegisterApplicationMasterResponse response =
          recordFactory.newRecordInstance(
              RegisterApplicationMasterResponse.class);
      this.amsProcessingChain.registerApplicationMaster(
          amrmTokenIdentifier.getApplicationAttemptId(), request, response);
      return response;
    }
  }

  @Override
  public FinishApplicationMasterResponse finishApplicationMaster(
      FinishApplicationMasterRequest request) throws YarnException,
      IOException {

    ApplicationAttemptId applicationAttemptId =
        YarnServerSecurityUtils.authorizeRequest().getApplicationAttemptId();
    ApplicationId appId = applicationAttemptId.getApplicationId();

    RMApp rmApp =
        rmContext.getRMApps().get(applicationAttemptId.getApplicationId());

    // Remove collector address when app get finished.
    if (timelineServiceV2Enabled) {
      ((RMAppImpl) rmApp).removeCollectorData();
    }
    // checking whether the app exits in RMStateStore at first not to throw
    // ApplicationDoesNotExistInCacheException before and after
    // RM work-preserving restart.
    if (rmApp.isAppFinalStateStored()) {
      LOG.info(rmApp.getApplicationId() + " unregistered successfully. ");
      return FinishApplicationMasterResponse.newInstance(true);
    }

    AllocateResponseLock lock = responseMap.get(applicationAttemptId);
    if (lock == null) {
      throwApplicationDoesNotExistInCacheException(applicationAttemptId);
    }

    // Allow only one thread in AM to do finishApp at a time.
    synchronized (lock) {
      if (!hasApplicationMasterRegistered(applicationAttemptId)) {
        String message =
            "Application Master is trying to unregister before registering for: "
                + appId;
        LOG.error(message);
        RMAuditLogger.logFailure(
            this.rmContext.getRMApps()
                .get(appId).getUser(),
            AuditConstants.UNREGISTER_AM, "", "ApplicationMasterService",
            message, appId,
            applicationAttemptId);
        throw new ApplicationMasterNotRegisteredException(message);
      }

      FinishApplicationMasterResponse response =
          FinishApplicationMasterResponse.newInstance(false);
      if (finishedAttemptCache.putIfAbsent(applicationAttemptId, true)
          == null) {
        this.amsProcessingChain
            .finishApplicationMaster(applicationAttemptId, request, response);
      }
      this.amLivelinessMonitor.receivedPing(applicationAttemptId);
      return response;
    }
  }

  private void throwApplicationDoesNotExistInCacheException(
      ApplicationAttemptId appAttemptId)
      throws InvalidApplicationMasterRequestException {
    String message = "Application doesn't exist in cache "
        + appAttemptId;
    LOG.error(message);
    throw new InvalidApplicationMasterRequestException(message);
  }
  
  /**
   * @param appAttemptId
   * @return true if application is registered for the respective attemptid
   */
  public boolean hasApplicationMasterRegistered(
      ApplicationAttemptId appAttemptId) {
    boolean hasApplicationMasterRegistered = false;
    AllocateResponseLock lastResponse = responseMap.get(appAttemptId);
    if (lastResponse != null) {
      synchronized (lastResponse) {
        if (lastResponse.getAllocateResponse() != null
            && lastResponse.getAllocateResponse().getResponseId() >= 0) {
          hasApplicationMasterRegistered = true;
        }
      }
    }
    return hasApplicationMasterRegistered;
  }

  private final static List<Container> EMPTY_CONTAINER_LIST =
      new ArrayList<Container>();
  protected static final Allocation EMPTY_ALLOCATION = new Allocation(
      EMPTY_CONTAINER_LIST, Resources.createResource(0), null, null, null);

  @Override
  public AllocateResponse allocate(AllocateRequest request)
      throws YarnException, IOException {

    AMRMTokenIdentifier amrmTokenIdentifier =
        YarnServerSecurityUtils.authorizeRequest();

    ApplicationAttemptId appAttemptId =
        amrmTokenIdentifier.getApplicationAttemptId();

    this.amLivelinessMonitor.receivedPing(appAttemptId);

    /* check if its in cache */
    AllocateResponseLock lock = responseMap.get(appAttemptId);
    if (lock == null) {
      String message =
          "Application attempt " + appAttemptId
              + " doesn't exist in ApplicationMasterService cache.";
      LOG.error(message);
      throw new ApplicationAttemptNotFoundException(message);
    }
    synchronized (lock) {
      AllocateResponse lastResponse = lock.getAllocateResponse();
      if (!hasApplicationMasterRegistered(appAttemptId)) {
        String message =
            "AM is not registered for known application attempt: "
                + appAttemptId
                + " or RM had restarted after AM registered. "
                + " AM should re-register.";
        throw new ApplicationMasterNotRegisteredException(message);
      }

      // Normally request.getResponseId() == lastResponse.getResponseId()
      if (AMRMClientUtils.getNextResponseId(
          request.getResponseId()) == lastResponse.getResponseId()) {
        // heartbeat one step old, simply return lastReponse
        return lastResponse;
      } else if (request.getResponseId() != lastResponse.getResponseId()) {
        throw new InvalidApplicationMasterRequestException(AMRMClientUtils
            .assembleInvalidResponseIdExceptionMessage(appAttemptId,
                lastResponse.getResponseId(), request.getResponseId()));
      }

      AllocateResponse response =
          recordFactory.newRecordInstance(AllocateResponse.class);
      this.amsProcessingChain.allocate(
          amrmTokenIdentifier.getApplicationAttemptId(), request, response);

      // update AMRMToken if the token is rolled-up
      MasterKeyData nextMasterKey =
          this.rmContext.getAMRMTokenSecretManager().getNextMasterKeyData();

      if (nextMasterKey != null
          && nextMasterKey.getMasterKey().getKeyId() != amrmTokenIdentifier
          .getKeyId()) {
        RMApp app =
            this.rmContext.getRMApps().get(appAttemptId.getApplicationId());
        RMAppAttempt appAttempt = app.getRMAppAttempt(appAttemptId);
        RMAppAttemptImpl appAttemptImpl = (RMAppAttemptImpl)appAttempt;
        Token<AMRMTokenIdentifier> amrmToken = appAttempt.getAMRMToken();
        if (nextMasterKey.getMasterKey().getKeyId() !=
            appAttemptImpl.getAMRMTokenKeyId()) {
          LOG.info("The AMRMToken has been rolled-over. Send new AMRMToken back"
              + " to application: " + appAttemptId.getApplicationId());
          amrmToken = rmContext.getAMRMTokenSecretManager()
              .createAndGetAMRMToken(appAttemptId);
          appAttemptImpl.setAMRMToken(amrmToken);
        }
        response.setAMRMToken(org.apache.hadoop.yarn.api.records.Token
            .newInstance(amrmToken.getIdentifier(), amrmToken.getKind()
                .toString(), amrmToken.getPassword(), amrmToken.getService()
                .toString()));
      }

      /*
       * As we are updating the response inside the lock object so we don't
       * need to worry about unregister call occurring in between (which
       * removes the lock object).
       */
      response.setResponseId(
          AMRMClientUtils.getNextResponseId(lastResponse.getResponseId()));
      lock.setAllocateResponse(response);
      return response;
    }
  }

  public void registerAppAttempt(ApplicationAttemptId attemptId) {
    //创建响应对象
    AllocateResponse response =
        recordFactory.newRecordInstance(AllocateResponse.class);
    // set response id to -1 before application master for the following
    // attemptID get registered
    response.setResponseId(AMRMClientUtils.PRE_REGISTER_RESPONSE_ID);
    LOG.info("Registering app attempt : " + attemptId);
    //将响应对象与应用程序尝试映射
    responseMap.put(attemptId, new AllocateResponseLock(response));
    //通过 nmTokenSecretManager，应用程序尝试的信息会被传递到 NodeManager 以供后续的认证和通信
    rmContext.getNMTokenSecretManager().registerApplicationAttempt(attemptId);
  }

  @VisibleForTesting
  protected boolean setAttemptLastResponseId(ApplicationAttemptId attemptId,
      int lastResponseId) {
    AllocateResponseLock lock = responseMap.get(attemptId);
    if (lock == null || lock.getAllocateResponse() == null) {
      return false;
    }
    lock.getAllocateResponse().setResponseId(lastResponseId);
    return true;
  }

  public void unregisterAttempt(ApplicationAttemptId attemptId) {
    LOG.info("Unregistering app attempt : " + attemptId);
    responseMap.remove(attemptId);
    finishedAttemptCache.remove(attemptId);
    rmContext.getNMTokenSecretManager().unregisterApplicationAttempt(attemptId);
  }

  public void refreshServiceAcls(Configuration configuration, 
      PolicyProvider policyProvider) {
    this.server.refreshServiceAclWithLoadedConfiguration(configuration,
        policyProvider);
  }
  
  @Override
  protected void serviceStop() throws Exception {
    if (this.server != null) {
      this.server.stop();
    }
    responseMap.clear();
    finishedAttemptCache.clear();
    super.serviceStop();
  }
  
  public static class AllocateResponseLock {
    private AllocateResponse response;
    
    public AllocateResponseLock(AllocateResponse response) {
      this.response = response;
    }
    
    public synchronized AllocateResponse getAllocateResponse() {
      return response;
    }
    
    public synchronized void setAllocateResponse(AllocateResponse response) {
      this.response = response;
    }
  }

  @VisibleForTesting
  public Server getServer() {
    return this.server;
  }
}
