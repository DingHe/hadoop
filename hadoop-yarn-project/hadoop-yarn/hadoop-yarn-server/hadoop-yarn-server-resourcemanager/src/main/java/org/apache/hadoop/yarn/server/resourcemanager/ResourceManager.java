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

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.classification.VisibleForTesting;
import com.sun.jersey.spi.container.servlet.ServletContainer;

import org.apache.hadoop.yarn.metrics.GenericEventTypeMetrics;
import org.apache.hadoop.yarn.server.webproxy.DefaultAppReportFetcher;
import org.apache.hadoop.yarn.webapp.WebAppException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.apache.curator.framework.AuthInfo;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.JvmPauseMonitor;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.curator.ZKCuratorManager;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.ConfigurationProvider;
import org.apache.hadoop.yarn.conf.ConfigurationProviderFactory;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.nodelabels.NodeAttributesManager;
import org.apache.hadoop.yarn.server.resourcemanager.ahs.RMApplicationHistoryWriter;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncherEventType;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.ApplicationMasterLauncher;
import org.apache.hadoop.yarn.server.resourcemanager.federation.FederationStateStoreService;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.CombinedSystemMetricsPublisher;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.NoOpSystemMetricPublisher;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.SystemMetricsPublisher;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.TimelineServiceV1Publisher;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.TimelineServiceV2Publisher;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NodeAttributesManagerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMDelegatedNodeLabelsUpdater;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.NullRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStoreFactory;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Recoverable;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.AbstractReservationSystem;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSystem;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceProfilesManager;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceProfilesManagerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.monitor.RMAppLifetimeMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.YarnConfigurationStore;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.YarnConfigurationStoreFactory;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.AllocationTagsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.MemoryPlacementConstraintManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.PlacementConstraintManagerService;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.MultiNodeSortingManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.MutableConfScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.DelegationTokenRenewer;
import org.apache.hadoop.yarn.server.resourcemanager.security.ProxyCAManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.QueueACLsManager;
import org.apache.hadoop.yarn.server.resourcemanager.timelineservice.RMTimelineCollectorManager;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.VolumeManager;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.VolumeManagerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.processor.VolumeAMSProcessor;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebApp;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebAppUtil;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.service.SystemServiceManager;
import org.apache.hadoop.yarn.server.webproxy.AppReportFetcher;
import org.apache.hadoop.yarn.server.webproxy.ProxyCA;
import org.apache.hadoop.yarn.server.webproxy.ProxyUriUtils;
import org.apache.hadoop.yarn.server.webproxy.WebAppProxy;
import org.apache.hadoop.yarn.server.webproxy.WebAppProxyServlet;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;
import org.apache.hadoop.yarn.webapp.WebApps.Builder;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.eclipse.jetty.webapp.WebAppContext;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The ResourceManager is the main class that is a set of components.
 * "I am the ResourceManager. All your resources belong to us..."
 *
 */
//负责管理集群资源并协调应用程序的执行。它与 NodeManager 和 ApplicationMaster 共同协作，实现资源的分配和任务调度
@SuppressWarnings("unchecked")
public class ResourceManager extends CompositeService
        implements Recoverable, ResourceManagerMXBean {

  /**
   * Priority of the ResourceManager shutdown hook.
   */
  public static final int SHUTDOWN_HOOK_PRIORITY = 30;

  /**
   * Used for generation of various ids.
   */
  public static final int EPOCH_BIT_SHIFT = 40;

  private static final Logger LOG =
      LoggerFactory.getLogger(ResourceManager.class);
  private static final Marker FATAL = MarkerFactory.getMarker("FATAL");
  private static long clusterTimeStamp = System.currentTimeMillis();

  /*
   * UI2 webapp name
   */
  public static final String UI2_WEBAPP_NAME = "/ui2";

  /**
   * "Always On" services. Services that need to run always irrespective of
   * the HA state of the RM.
   */
  @VisibleForTesting
  protected RMContextImpl rmContext;
  //事件派发器，负责不同组件间的事件通信，例如 NodeManager 状态变化、任务提交等
  private Dispatcher rmDispatcher;
  //提供 ResourceManager 的管理 API，允许外部工具或管理员进行配置和管理
  @VisibleForTesting
  protected AdminService adminService;

  /**
   * "Active" services. Services that need to run only on the Active RM.
   * These services are managed (initialized, started, stopped) by the
   * {@link CompositeService} RMActiveServices.
   *
   * RM is active when (1) HA is disabled, or (2) HA is enabled and the RM is
   * in Active state.
   */
  // 管理 ResourceManager 处于 Active 状态时运行的服务，例如 ApplicationMaster 管理、调度器等
  protected RMActiveServices activeServices;
  // 管理安全令牌（如 DelegationTokens），确保安全通信
  protected RMSecretManagerService rmSecretManagerService;
  //调度器组件，负责根据调度策略（如 CapacityScheduler 或 FairScheduler）分配资源
  protected ResourceScheduler scheduler;
  //资源预留系统，允许用户提前预留计算资源
  protected ReservationSystem reservationSystem;
  private ClientRMService clientRM;
  protected ApplicationMasterService masterService;
  protected NMLivelinessMonitor nmLivelinessMonitor;
  //管理 NodeManager 列表，维护可用计算节点的信息
  protected NodesListManager nodesListManager;
  //管理 ApplicationMaster 的生命周期，包括应用提交、运行、失败恢复等
  protected RMAppManager rmAppManager;
  protected ApplicationACLsManager applicationACLsManager;
  protected QueueACLsManager queueACLsManager;
  private FederationStateStoreService federationStateStoreService;
  private ProxyCAManager proxyCAManager;
  //ResourceManager 的 Web UI，提供可视化的资源管理和应用状态信息
  private WebApp webApp;
  private AppReportFetcher fetcher = null;
  protected ResourceTrackerService resourceTracker;
  private JvmMetrics jvmMetrics;
  private boolean curatorEnabled = false;
  private ZKCuratorManager zkManager;
  private final String zkRootNodePassword =
      Long.toString(new SecureRandom().nextLong());
  private boolean recoveryEnabled;

  @VisibleForTesting
  protected String webAppAddress;
  private ConfigurationProvider configurationProvider = null;
  /** End of Active services */

  private Configuration conf;
  //存储 ResourceManager 的登录用户信息，确保认证安全
  private UserGroupInformation rmLoginUGI;

  public ResourceManager() {
    super("ResourceManager");
  }

  public RMContext getRMContext() {
    return this.rmContext;
  }

  public static long getClusterTimeStamp() {
    return clusterTimeStamp;
  }

  public String getRMLoginUser() {
    return rmLoginUGI.getShortUserName();
  }

  private RMInfo rmStatusInfoBean;

  @VisibleForTesting
  protected static void setClusterTimeStamp(long timestamp) {
    clusterTimeStamp = timestamp;
  }

  @VisibleForTesting
  Dispatcher getRmDispatcher() {
    return rmDispatcher;
  }

  @VisibleForTesting
  protected ResourceProfilesManager createResourceProfileManager() {
    ResourceProfilesManager resourceProfilesManager =
        new ResourceProfilesManagerImpl();
    return resourceProfilesManager;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    this.conf = conf;
    UserGroupInformation.setConfiguration(conf);
    this.rmContext = new RMContextImpl();
    rmContext.setResourceManager(this);
    rmContext.setYarnConfiguration(conf);

    rmStatusInfoBean = new RMInfo(this);
    rmStatusInfoBean.register();

    // Set HA configuration should be done before login
    this.rmContext.setHAEnabled(HAUtil.isHAEnabled(this.conf));
    if (this.rmContext.isHAEnabled()) {
      HAUtil.verifyAndSetConfiguration(this.conf);
    }

    // Set UGI and do login
    // If security is enabled, use login user
    // If security is not enabled, use current user
    this.rmLoginUGI = UserGroupInformation.getCurrentUser();
    try {
      doSecureLogin();
    } catch(IOException ie) {
      throw new YarnRuntimeException("Failed to login", ie);
    }

    this.configurationProvider =
        ConfigurationProviderFactory.getConfigurationProvider(conf);
    this.configurationProvider.init(this.conf);
    rmContext.setConfigurationProvider(configurationProvider);

    // load core-site.xml
    loadConfigurationXml(YarnConfiguration.CORE_SITE_CONFIGURATION_FILE);

    // Do refreshSuperUserGroupsConfiguration with loaded core-site.xml
    // Or use RM specific configurations to overwrite the common ones first
    // if they exist
    RMServerUtils.processRMProxyUsersConf(conf);
    ProxyUsers.refreshSuperUserGroupsConfiguration(this.conf);

    // load yarn-site.xml
    loadConfigurationXml(YarnConfiguration.YARN_SITE_CONFIGURATION_FILE);

    validateConfigs(this.conf);

    // register the handlers for all AlwaysOn services using setupDispatcher().
    rmDispatcher = setupDispatcher();
    addIfService(rmDispatcher);
    rmContext.setDispatcher(rmDispatcher);

    // The order of services below should not be changed as services will be
    // started in same order
    // As elector service needs admin service to be initialized and started,
    // first we add admin service then elector service

    adminService = createAdminService();
    addService(adminService);
    rmContext.setRMAdminService(adminService);

    // elector must be added post adminservice
    if (this.rmContext.isHAEnabled()) {
      // If the RM is configured to use an embedded leader elector,
      // initialize the leader elector.
      if (HAUtil.isAutomaticFailoverEnabled(conf)
          && HAUtil.isAutomaticFailoverEmbedded(conf)) {
        EmbeddedElector elector = createEmbeddedElector();
        addIfService(elector);
        rmContext.setLeaderElectorService(elector);
      }
    }

    createAndInitActiveServices(false);

    webAppAddress = WebAppUtils.getWebAppBindURL(this.conf,
                      YarnConfiguration.RM_BIND_HOST,
                      WebAppUtils.getRMWebAppURLWithoutScheme(this.conf));

    RMApplicationHistoryWriter rmApplicationHistoryWriter =
        createRMApplicationHistoryWriter();
    addService(rmApplicationHistoryWriter);
    rmContext.setRMApplicationHistoryWriter(rmApplicationHistoryWriter);

    // initialize the RM timeline collector first so that the system metrics
    // publisher can bind to it
    if (YarnConfiguration.timelineServiceV2Enabled(this.conf)) {
      RMTimelineCollectorManager timelineCollectorManager =
          createRMTimelineCollectorManager();
      addService(timelineCollectorManager);
      rmContext.setRMTimelineCollectorManager(timelineCollectorManager);
    }

    SystemMetricsPublisher systemMetricsPublisher =
        createSystemMetricsPublisher();
    addIfService(systemMetricsPublisher);
    rmContext.setSystemMetricsPublisher(systemMetricsPublisher);

    registerMXBean();

    super.serviceInit(this.conf);
  }

  private void loadConfigurationXml(String configurationFile)
      throws YarnException, IOException {
    InputStream configurationInputStream =
        this.configurationProvider.getConfigurationInputStream(this.conf,
            configurationFile);
    if (configurationInputStream != null) {
      this.conf.addResource(configurationInputStream, configurationFile);
    }
  }

  protected EmbeddedElector createEmbeddedElector() throws IOException {
    EmbeddedElector elector;
    curatorEnabled =
        conf.getBoolean(YarnConfiguration.CURATOR_LEADER_ELECTOR,
            YarnConfiguration.DEFAULT_CURATOR_LEADER_ELECTOR_ENABLED);
    if (curatorEnabled) {
      this.zkManager = createAndStartZKManager(conf);
      elector = new CuratorBasedElectorService(this);
    } else {
      elector = new ActiveStandbyElectorBasedElectorService(this);
    }
    return elector;
  }

  /**
   * Get ZooKeeper Curator manager, creating and starting if not exists.
   * @param config Configuration for the ZooKeeper curator.
   * @return ZooKeeper Curator manager.
   * @throws IOException If it cannot create the manager.
   */
  public ZKCuratorManager createAndStartZKManager(Configuration
      config) throws IOException {
    ZKCuratorManager manager = new ZKCuratorManager(config);

    // Get authentication
    List<AuthInfo> authInfos = new ArrayList<>();
    if (HAUtil.isHAEnabled(config) && HAUtil.getConfValueForRMInstance(
        YarnConfiguration.ZK_RM_STATE_STORE_ROOT_NODE_ACL, config) == null) {
      String zkRootNodeUsername = HAUtil.getConfValueForRMInstance(
          YarnConfiguration.RM_ADDRESS,
          YarnConfiguration.DEFAULT_RM_ADDRESS, config);
      String defaultFencingAuth =
          zkRootNodeUsername + ":" + zkRootNodePassword;
      byte[] defaultFencingAuthData =
          defaultFencingAuth.getBytes(StandardCharsets.UTF_8);
      String scheme = new DigestAuthenticationProvider().getScheme();
      AuthInfo authInfo = new AuthInfo(scheme, defaultFencingAuthData);
      authInfos.add(authInfo);
    }

    boolean isSSLEnabled =
        config.getBoolean(CommonConfigurationKeys.ZK_CLIENT_SSL_ENABLED,
            config.getBoolean(YarnConfiguration.RM_ZK_CLIENT_SSL_ENABLED,
                YarnConfiguration.DEFAULT_RM_ZK_CLIENT_SSL_ENABLED));
    manager.start(authInfos, isSSLEnabled);
    return manager;
  }

  public ZKCuratorManager getZKManager() {
    return zkManager;
  }

  public CuratorFramework getCurator() {
    if (this.zkManager == null) {
      return null;
    }
    return this.zkManager.getCurator();
  }

  public String getZkRootNodePassword() {
    return this.zkRootNodePassword;
  }


  protected QueueACLsManager createQueueACLsManager(ResourceScheduler scheduler,
      Configuration conf) {
    return QueueACLsManager.getQueueACLsManager(scheduler, conf);
  }

  @VisibleForTesting
  protected void setRMStateStore(RMStateStore rmStore) {
    rmStore.setRMDispatcher(rmDispatcher);
    rmStore.setResourceManager(this);
    rmContext.setStateStore(rmStore);
  }
  //创建并返回一个 事件处理器（EventHandler），用于调度 SchedulerEvent 事件
  protected EventHandler<SchedulerEvent> createSchedulerEventDispatcher() {
    String dispatcherName = "SchedulerEventDispatcher";
    EventDispatcher dispatcher;
    //获取调度器事件分发器的 CPU 监控参数
    //表示 每分钟采样的 CPU 监控次数
    int threadMonitorRate = conf.getInt(
        YarnConfiguration.YARN_DISPATCHER_CPU_MONITOR_SAMPLES_PER_MIN,
        YarnConfiguration.DEFAULT_YARN_DISPATCHER_CPU_MONITOR_SAMPLES_PER_MIN);
     //创建 SchedulerEventDispatcher 或 EventDispatcher
    if (threadMonitorRate > 0) {
      dispatcher = new SchedulerEventDispatcher(dispatcherName,
          threadMonitorRate);
      ClusterMetrics.getMetrics().setRmEventProcMonitorEnable(true);
    } else {
      dispatcher = new EventDispatcher(this.scheduler, dispatcherName);
    }
    dispatcher.
        setMetrics(GenericEventTypeMetricsManager.
            create(dispatcher.getName(), SchedulerEventType.class));
    return dispatcher;
  }
  // 用于创建并初始化 Dispatcher（事件调度器）。
  // 在 YARN 资源管理器 (ResourceManager) 中，事件驱动机制是系统运作的关键，
  // 该调度器用于分发和处理不同类型的事件，以便协调 ResourceManager 内部的各种组件
  protected Dispatcher createDispatcher() {
    AsyncDispatcher dispatcher = new AsyncDispatcher("RM Event dispatcher");

    // Add 4 busy event types.
    GenericEventTypeMetrics
        nodesListManagerEventTypeMetrics =
        GenericEventTypeMetricsManager.
            create(dispatcher.getName(), NodesListManagerEventType.class);
    dispatcher.addMetrics(nodesListManagerEventTypeMetrics,
        nodesListManagerEventTypeMetrics
            .getEnumClass());

    GenericEventTypeMetrics
        rmNodeEventTypeMetrics =
        GenericEventTypeMetricsManager.
            create(dispatcher.getName(), RMNodeEventType.class);
    dispatcher.addMetrics(rmNodeEventTypeMetrics,
        rmNodeEventTypeMetrics
            .getEnumClass());

    GenericEventTypeMetrics
        rmAppEventTypeMetrics =
        GenericEventTypeMetricsManager.
            create(dispatcher.getName(), RMAppEventType.class);
    dispatcher.addMetrics(rmAppEventTypeMetrics,
        rmAppEventTypeMetrics
            .getEnumClass());

    GenericEventTypeMetrics
        rmAppAttemptEventTypeMetrics =
        GenericEventTypeMetricsManager.
            create(dispatcher.getName(), RMAppAttemptEventType.class);
    dispatcher.addMetrics(rmAppAttemptEventTypeMetrics,
        rmAppAttemptEventTypeMetrics
            .getEnumClass());

    return dispatcher;
  }
  //默认是容量调度
  protected ResourceScheduler createScheduler() {
    String schedulerClassName = conf.get(YarnConfiguration.RM_SCHEDULER,
        YarnConfiguration.DEFAULT_RM_SCHEDULER);
    LOG.info("Using Scheduler: " + schedulerClassName);
    try {
      Class<?> schedulerClazz = Class.forName(schedulerClassName);
      if (ResourceScheduler.class.isAssignableFrom(schedulerClazz)) {
        return (ResourceScheduler) ReflectionUtils.newInstance(schedulerClazz,
            this.conf);
      } else {
        throw new YarnRuntimeException("Class: " + schedulerClassName
            + " not instance of " + ResourceScheduler.class.getCanonicalName());
      }
    } catch (ClassNotFoundException e) {
      throw new YarnRuntimeException("Could not instantiate Scheduler: "
          + schedulerClassName, e);
    }
  }

  protected ReservationSystem createReservationSystem() {
    String reservationClassName =
        conf.get(YarnConfiguration.RM_RESERVATION_SYSTEM_CLASS,
            AbstractReservationSystem.getDefaultReservationSystem(scheduler));
    if (reservationClassName == null) {
      return null;
    }
    LOG.info("Using ReservationSystem: " + reservationClassName);
    try {
      Class<?> reservationClazz = Class.forName(reservationClassName);
      if (ReservationSystem.class.isAssignableFrom(reservationClazz)) {
        return (ReservationSystem) ReflectionUtils.newInstance(
            reservationClazz, this.conf);
      } else {
        throw new YarnRuntimeException("Class: " + reservationClassName
            + " not instance of " + ReservationSystem.class.getCanonicalName());
      }
    } catch (ClassNotFoundException e) {
      throw new YarnRuntimeException(
          "Could not instantiate ReservationSystem: " + reservationClassName, e);
    }
  }

  protected SystemServiceManager createServiceManager() {
    String schedulerClassName =
        YarnConfiguration.DEFAULT_YARN_API_SYSTEM_SERVICES_CLASS;
    LOG.info("Using SystemServiceManager: " + schedulerClassName);
    try {
      Class<?> schedulerClazz = Class.forName(schedulerClassName);
      if (SystemServiceManager.class.isAssignableFrom(schedulerClazz)) {
        return (SystemServiceManager) ReflectionUtils
            .newInstance(schedulerClazz, this.conf);
      } else {
        throw new YarnRuntimeException(
            "Class: " + schedulerClassName + " not instance of "
                + SystemServiceManager.class.getCanonicalName());
      }
    } catch (ClassNotFoundException e) {
      throw new YarnRuntimeException(
          "Could not instantiate SystemServiceManager: " + schedulerClassName,
          e);
    }
  }

  protected ApplicationMasterLauncher createAMLauncher() {
    return new ApplicationMasterLauncher(this.rmContext);
  }

  private NMLivelinessMonitor createNMLivelinessMonitor() {
    return new NMLivelinessMonitor(this.rmContext
        .getDispatcher());
  }

  protected AMLivelinessMonitor createAMLivelinessMonitor() {
    return new AMLivelinessMonitor(this.rmDispatcher);
  }
  
  protected RMNodeLabelsManager createNodeLabelManager()
      throws InstantiationException, IllegalAccessException {
    return new RMNodeLabelsManager();
  }

  protected NodeAttributesManager createNodeAttributesManager() {
    NodeAttributesManagerImpl namImpl = new NodeAttributesManagerImpl();
    namImpl.setRMContext(rmContext);
    return namImpl;
  }

  protected AllocationTagsManager createAllocationTagsManager() {
    return new AllocationTagsManager(this.rmContext);
  }

  protected PlacementConstraintManagerService
      createPlacementConstraintManager() {
    // Use the in memory Placement Constraint Manager.
    return new MemoryPlacementConstraintManager();
  }
  
  protected DelegationTokenRenewer createDelegationTokenRenewer() {
    return new DelegationTokenRenewer();
  }

  protected RMAppManager createRMAppManager() {
    return new RMAppManager(this.rmContext, this.scheduler, this.masterService,
      this.applicationACLsManager, this.conf);
  }

  protected RMApplicationHistoryWriter createRMApplicationHistoryWriter() {
    return new RMApplicationHistoryWriter();
  }

  private RMTimelineCollectorManager createRMTimelineCollectorManager() {
    return new RMTimelineCollectorManager(this);
  }

  private FederationStateStoreService createFederationStateStoreService() {
    return new FederationStateStoreService(rmContext);
  }

  protected MultiNodeSortingManager<SchedulerNode> createMultiNodeSortingManager() {
    return new MultiNodeSortingManager<SchedulerNode>();
  }

  protected SystemMetricsPublisher createSystemMetricsPublisher() {
    List<SystemMetricsPublisher> publishers =
        new ArrayList<SystemMetricsPublisher>();
    if (YarnConfiguration.timelineServiceV1Enabled(conf) &&
        YarnConfiguration.systemMetricsPublisherEnabled(conf)) {
      SystemMetricsPublisher publisherV1 = new TimelineServiceV1Publisher();
      publishers.add(publisherV1);
    }
    if (YarnConfiguration.timelineServiceV2Enabled(conf) &&
        YarnConfiguration.systemMetricsPublisherEnabled(conf)) {
      // we're dealing with the v.2.x publisher
      LOG.info("system metrics publisher with the timeline service V2 is "
          + "configured");
      SystemMetricsPublisher publisherV2 = new TimelineServiceV2Publisher(
          rmContext.getRMTimelineCollectorManager());
      publishers.add(publisherV2);
    }
    if (publishers.isEmpty()) {
      LOG.info("TimelineServicePublisher is not configured");
      SystemMetricsPublisher noopPublisher = new NoOpSystemMetricPublisher();
      publishers.add(noopPublisher);
    }

    for (SystemMetricsPublisher publisher : publishers) {
      addIfService(publisher);
    }

    SystemMetricsPublisher combinedPublisher =
        new CombinedSystemMetricsPublisher(publishers);
    return combinedPublisher;
  }

  // sanity check for configurations
  protected static void validateConfigs(Configuration conf) {
    // validate max-attempts
    int rmMaxAppAttempts = conf.getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
        YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
    if (rmMaxAppAttempts <= 0) {
      throw new YarnRuntimeException("Invalid rm am max attempts configuration"
          + ", " + YarnConfiguration.RM_AM_MAX_ATTEMPTS
          + "=" + rmMaxAppAttempts + ", it should be a positive integer.");
    }
    int globalMaxAppAttempts = conf.getInt(
        YarnConfiguration.GLOBAL_RM_AM_MAX_ATTEMPTS,
        conf.getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
            YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS));
    if (globalMaxAppAttempts <= 0) {
      throw new YarnRuntimeException("Invalid global max attempts configuration"
          + ", " + YarnConfiguration.GLOBAL_RM_AM_MAX_ATTEMPTS
          + "=" + globalMaxAppAttempts + ", it should be a positive integer.");
    }

    // validate expireIntvl >= heartbeatIntvl
    long expireIntvl = conf.getLong(YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS,
        YarnConfiguration.DEFAULT_RM_NM_EXPIRY_INTERVAL_MS);
    long heartbeatIntvl =
        conf.getLong(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS,
            YarnConfiguration.DEFAULT_RM_NM_HEARTBEAT_INTERVAL_MS);
    if (expireIntvl < heartbeatIntvl) {
      throw new YarnRuntimeException("Nodemanager expiry interval should be no"
          + " less than heartbeat interval, "
          + YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS + "=" + expireIntvl
          + ", " + YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS + "="
          + heartbeatIntvl);
    }

    if (HAUtil.isFederationEnabled(conf)) {
      /*
       * In Yarn Federation, we need UAMs in secondary sub-clusters to stay
       * alive when the next attempt AM in home sub-cluster gets launched. If
       * the previous AM died because the node is lost after NM timeout. It will
       * already be too late if AM timeout is even shorter.
       */
      String rmAmExpiryIntervalMS = conf.get(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS);
      long amExpireIntvl;
      if (NumberUtils.isDigits(rmAmExpiryIntervalMS)) {
        amExpireIntvl = conf.getLong(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS,
            YarnConfiguration.DEFAULT_RM_AM_EXPIRY_INTERVAL_MS);
      } else {
        amExpireIntvl = conf.getTimeDuration(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS,
            YarnConfiguration.DEFAULT_RM_AM_EXPIRY_INTERVAL_MS, TimeUnit.MILLISECONDS);
      }

      if (amExpireIntvl <= expireIntvl) {
        throw new YarnRuntimeException("When Yarn Federation is enabled, "
            + "AM expiry interval should be no less than NM expiry interval, "
            + YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS + "=" + amExpireIntvl
            + ", " + YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS + "="
            + expireIntvl);
      }
    }
  }

  /**
   * RMActiveServices handles all the Active services in the RM.
   */
  //资源管理器（ResourceManager，RM）中负责管理所有激活服务的核心组件。
  // 它继承自 CompositeService，用于初始化、启动和停止 RM 运行时所需的各项服务。其主要功能包括：
  //资源管理：管理调度器、节点管理、标签管理、调度策略等。
  //应用管理：维护应用生命周期，支持 AM（ApplicationMaster）调度、监控和恢复。
  //安全机制：管理委托令牌（Delegation Token），确保安全环境下的身份验证。
  //状态管理与恢复：支持 RM 高可用（HA）模式，并提供应用恢复能力。
  //事件处理：注册和分发调度、应用、节点相关的事件
  @Private
  public class RMActiveServices extends CompositeService {

    private DelegationTokenRenewer delegationTokenRenewer;//管理 YARN 安全模式下的委托令牌续订。
    private EventHandler<SchedulerEvent> schedulerDispatcher;//事件分发器，处理调度器的事件
    private ApplicationMasterLauncher applicationMasterLauncher;//负责启动 ApplicationMaster（AM）
    private ContainerAllocationExpirer containerAllocationExpirer;//负责管理容器的超时和回收。
    private ResourceManager rm;
    private boolean fromActive = false;//标识 RM 是否处于活动状态
    private StandByTransitionRunnable standByTransitionRunnable;//处理 RM 由 standby 转换到 active 状态的任务
    private RMNMInfo rmnmInfo;//资源管理器和节点管理信息
    private ScheduledThreadPoolExecutor eventQueueMetricExecutor;//线程池，用于定期更新调度器和 RM 事件队列的大小

    RMActiveServices(ResourceManager rm) {
      super("RMActiveServices");
      this.rm = rm;
    }

    @Override
    protected void serviceInit(Configuration configuration) throws Exception {
      //处理 RM 从 Standby 到 Active 的状态转换
      standByTransitionRunnable = new StandByTransitionRunnable();
      //负责 管理 YARN 的安全凭证（如 DelegationToken），确保在启用安全模式时，所有应用能够正确验证身份
      rmSecretManagerService = createRMSecretManagerService();
      addService(rmSecretManagerService);
      //负责回收 长期未使用的 Container，防止资源泄漏
      containerAllocationExpirer = new ContainerAllocationExpirer(rmDispatcher);
      addService(containerAllocationExpirer);
      rmContext.setContainerAllocationExpirer(containerAllocationExpirer);
      //监控 ApplicationMaster（AM） 是否存活，防止 AM 进程崩溃导致任务卡死
      AMLivelinessMonitor amLivelinessMonitor = createAMLivelinessMonitor();
      addService(amLivelinessMonitor);
      rmContext.setAMLivelinessMonitor(amLivelinessMonitor);
      //监控 AM 关闭状态，确保 任务在 AM 关闭后被正确清理
      AMLivelinessMonitor amFinishingMonitor = createAMLivelinessMonitor();
      addService(amFinishingMonitor);
      rmContext.setAMFinishingMonitor(amFinishingMonitor);
      //监控 Application 的生命周期，确保 超过时限的任务被回收
      RMAppLifetimeMonitor rmAppLifetimeMonitor = createRMAppLifetimeMonitor();
      addService(rmAppLifetimeMonitor);
      rmContext.setRMAppLifetimeMonitor(rmAppLifetimeMonitor);
      //负责 管理节点标签（Node Labels），支持 标签调度
      RMNodeLabelsManager nlm = createNodeLabelManager();
      nlm.setRMContext(rmContext);
      addService(nlm);
      rmContext.setNodeLabelManager(nlm);
      //负责管理节点的 属性（Attributes），增强调度的灵活性
      NodeAttributesManager nam = createNodeAttributesManager();
      addService(nam);
      rmContext.setNodeAttributesManager(nam);
      //用于管理分配标签，在调度任务时，提供更灵活的资源管理策略
      AllocationTagsManager allocationTagsManager =
          createAllocationTagsManager();
      rmContext.setAllocationTagsManager(allocationTagsManager);
      //用于管理 调度约束，确保任务调度时满足特定的资源和位置限制
      PlacementConstraintManagerService placementConstraintManager =
          createPlacementConstraintManager();
      addService(placementConstraintManager);
      rmContext.setPlacementConstraintManager(placementConstraintManager);

      // add resource profiles here because it's used by AbstractYarnScheduler
      //管理资源配置文件，支持在调度时使用 预定义的资源模板，以便优化集群资源分配
      ResourceProfilesManager resourceProfilesManager =
          createResourceProfileManager();
      resourceProfilesManager.init(conf);
      rmContext.setResourceProfilesManager(resourceProfilesManager);
      //在资源调度时，对多个计算节点进行 排序，从而提高调度决策的效率
      MultiNodeSortingManager<SchedulerNode> multiNodeSortingManager =
          createMultiNodeSortingManager();
      multiNodeSortingManager.setRMContext(rmContext);
      addService(multiNodeSortingManager);
      rmContext.setMultiNodeSortingManager(multiNodeSortingManager);
      //如果启用了 节点标签功能，该组件会 定期更新 集群节点的标签信息，从而影响调度决策
      RMDelegatedNodeLabelsUpdater delegatedNodeLabelsUpdater =
          createRMDelegatedNodeLabelsUpdater();
      if (delegatedNodeLabelsUpdater != null) {
        addService(delegatedNodeLabelsUpdater);
        rmContext.setRMDelegatedNodeLabelsUpdater(delegatedNodeLabelsUpdater);
      }
      //如果 YarnConfiguration.RECOVERY_ENABLED 设为 true，则 RM 会尝试 恢复先前的状态，否则使用 NullRMStateStore，即 不进行恢复
      recoveryEnabled = conf.getBoolean(YarnConfiguration.RECOVERY_ENABLED,
          YarnConfiguration.DEFAULT_RM_RECOVERY_ENABLED);

      RMStateStore rmStore = null;
      if (recoveryEnabled) {
        rmStore = RMStateStoreFactory.getStore(conf);
        //在 RM 失效重启后，可以 尽可能恢复 之前运行的任务，而不是重新调度所有任务
        boolean isWorkPreservingRecoveryEnabled =
            conf.getBoolean(
              YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_ENABLED,
              YarnConfiguration.DEFAULT_RM_WORK_PRESERVING_RECOVERY_ENABLED);
        rmContext
            .setWorkPreservingRecoveryEnabled(isWorkPreservingRecoveryEnabled);
      } else {
        rmStore = new NullRMStateStore();
      }

      try {
        rmStore.setResourceManager(rm);
        rmStore.init(conf);
        rmStore.setRMDispatcher(rmDispatcher);
      } catch (Exception e) {
        // the Exception from stateStore.init() needs to be handled for
        // HA and we need to give up master status if we got fenced
        LOG.error("Failed to init state store", e);
        throw e;
      }
      rmContext.setStateStore(rmStore);

      if (UserGroupInformation.isSecurityEnabled()) {
        delegationTokenRenewer = createDelegationTokenRenewer();
        rmContext.setDelegationTokenRenewer(delegationTokenRenewer);
      }

      // Register event handler for NodesListManager
      nodesListManager = new NodesListManager(rmContext);
      rmDispatcher.register(NodesListManagerEventType.class, nodesListManager);
      addService(nodesListManager);
      rmContext.setNodesListManager(nodesListManager);

      // Initialize the scheduler
      //默认为容量调度
      scheduler = createScheduler();
      scheduler.setRMContext(rmContext);
      addIfService(scheduler);
      rmContext.setScheduler(scheduler);
      //创建并注册调度器事件处理器
      schedulerDispatcher = createSchedulerEventDispatcher();
      addIfService(schedulerDispatcher);
      //处理 SchedulerEvent 事件
      rmDispatcher.register(SchedulerEventType.class, schedulerDispatcher);

      // Register event handler for RmAppEvents
      // 处理 RM 应用程序事件
      rmDispatcher.register(RMAppEventType.class,
          new ApplicationEventDispatcher(rmContext));

      // Register event handler for RmAppAttemptEvents
      // 处理 RM 应用尝试事件
      rmDispatcher.register(RMAppAttemptEventType.class,
          new ApplicationAttemptEventDispatcher(rmContext));

      // Register event handler for RmNodes
      // 处理 RM 节点事件
      rmDispatcher.register(
          RMNodeEventType.class, new NodeEventDispatcher(rmContext));
      //创建并添加节点存活监视器
      nmLivelinessMonitor = createNMLivelinessMonitor();
      addService(nmLivelinessMonitor);
      //创建并添加资源跟踪服务
      resourceTracker = createResourceTrackerService();
      addService(resourceTracker);
      rmContext.setResourceTrackerService(resourceTracker);
      //初始化度量系统
      MetricsSystem ms = DefaultMetricsSystem.initialize("ResourceManager");
      if (fromActive) {
        JvmMetrics.reattach(ms, jvmMetrics);
        UserGroupInformation.reattachMetrics();
      } else {
        jvmMetrics = JvmMetrics.initSingleton("ResourceManager", null);
      }
      //添加 JVM 暂停监视器
      JvmPauseMonitor pauseMonitor = new JvmPauseMonitor();
      addService(pauseMonitor);
      jvmMetrics.setPauseMonitor(pauseMonitor);
      //初始化 YARN 预留系统
      // Initialize the Reservation system
      if (conf.getBoolean(YarnConfiguration.RM_RESERVATION_SYSTEM_ENABLE,
          YarnConfiguration.DEFAULT_RM_RESERVATION_SYSTEM_ENABLE)) {
        reservationSystem = createReservationSystem();
        if (reservationSystem != null) {
          reservationSystem.setRMContext(rmContext);
          addIfService(reservationSystem);
          rmContext.setReservationSystem(reservationSystem);
          LOG.info("Initialized Reservation system");
        }
      }
      //创建并添加应用程序 Master 服务
      masterService = createApplicationMasterService();
      createAndRegisterOpportunisticDispatcher(masterService);
      addService(masterService) ;
      rmContext.setApplicationMasterService(masterService);

      //创建并初始化访问控制列表（ACLs）管理器
      applicationACLsManager = new ApplicationACLsManager(conf);
      //创建 队列 ACL 管理器，用于控制 用户对队列的访问权限
      queueACLsManager = createQueueACLsManager(scheduler, conf);
      //创建并注册 RM 应用程序管理器
      rmAppManager = createRMAppManager();
      // Register event handler for RMAppManagerEvents
      rmDispatcher.register(RMAppManagerEventType.class, rmAppManager);
      //创建 客户端服务，客户端服务负责与 ResourceManager 交互，如提交作业、获取资源等
      clientRM = createClientRMService();
      addService(clientRM);
      rmContext.setClientRMService(clientRM);
      //创建 Application Master 启动器，负责 启动应用程序的 ApplicationMaster
      applicationMasterLauncher = createAMLauncher();
      rmDispatcher.register(AMLauncherEventType.class,
          applicationMasterLauncher);

      addService(applicationMasterLauncher);
      //创建并添加 Delegation Token Renew 服务
      if (UserGroupInformation.isSecurityEnabled()) {
        addService(delegationTokenRenewer);
        delegationTokenRenewer.setRMContext(rmContext);
      }
      //处理 Federation 配置
      if(HAUtil.isFederationEnabled(conf)) {
        String cId = YarnConfiguration.getClusterId(conf);
        if (cId.isEmpty()) {
          String errMsg =
              "Cannot initialize RM as Federation is enabled"
                  + " but cluster id is not configured.";
          LOG.error(errMsg);
          throw new YarnRuntimeException(errMsg);
        }
        federationStateStoreService = createFederationStateStoreService();
        addIfService(federationStateStoreService);
        rmAppManager.setFederationStateStoreService(federationStateStoreService);
        LOG.info("Initialized Federation membership.");
      }
      //创建并添加代理证书管理服务
      proxyCAManager = new ProxyCAManager(new ProxyCA(), rmContext);
      addService(proxyCAManager);
      rmContext.setProxyCAManager(proxyCAManager);

      rmnmInfo = new RMNMInfo(rmContext, scheduler);
      //如果 YarnConfiguration.YARN_API_SERVICES_ENABLE 配置为 true，则 启用系统服务管理器，处理 一些额外的服务，如 REST API 服务
      if (conf.getBoolean(YarnConfiguration.YARN_API_SERVICES_ENABLE,
          false)) {
        SystemServiceManager systemServiceManager = createServiceManager();
        addIfService(systemServiceManager);
      }

      // Add volume manager to RM context when it is necessary
      //添加 Volume Manager 到 RM 上下文
      String[] amsProcessorList = conf.getStrings(
          YarnConfiguration.RM_APPLICATION_MASTER_SERVICE_PROCESSORS);
      if (amsProcessorList != null&& Arrays.stream(amsProcessorList)
          .anyMatch(s -> VolumeAMSProcessor.class.getName().equals(s))) {
        VolumeManager volumeManager = new VolumeManagerImpl();
        rmContext.setVolumeManager(volumeManager);
        addIfService(volumeManager);
      }
      //创建 单线程调度线程池，用于 定期监控 YARN 事件队列的大小
      eventQueueMetricExecutor = new ScheduledThreadPoolExecutor(1,
              new ThreadFactoryBuilder().
              setDaemon(true).setNameFormat("EventQueueSizeMetricThread").
              build());
      //定期统计 YARN 事件队列大小
      eventQueueMetricExecutor.scheduleAtFixedRate(new Runnable() {
        @Override
        public void run() {
          int rmEventQueueSize = ((AsyncDispatcher)getRMContext().
              getDispatcher()).getEventQueueSize();
          ClusterMetrics.getMetrics().setRmEventQueueSize(rmEventQueueSize);
          int schedulerEventQueueSize = ((EventDispatcher)schedulerDispatcher).
              getEventQueueSize();
          ClusterMetrics.getMetrics().
              setSchedulerEventQueueSize(schedulerEventQueueSize);
        }
      }, 1, 1, TimeUnit.SECONDS);

      super.serviceInit(conf);
    }

    private void createAndRegisterOpportunisticDispatcher(
        ApplicationMasterService service) {
      if (!isOpportunisticSchedulingEnabled(conf)) {
        return;
      }
      EventDispatcher oppContainerAllocEventDispatcher = new EventDispatcher(
          (OpportunisticContainerAllocatorAMService) service,
          OpportunisticContainerAllocatorAMService.class.getName());
      // Add an event dispatcher for the
      // OpportunisticContainerAllocatorAMService to handle node
      // additions, updates and removals. Since the SchedulerEvent is currently
      // a super set of theses, we register interest for it.
      addService(oppContainerAllocEventDispatcher);
      rmDispatcher
          .register(SchedulerEventType.class, oppContainerAllocEventDispatcher);
    }

    @Override
    protected void serviceStart() throws Exception {
      RMStateStore rmStore = rmContext.getStateStore();
      // The state store needs to start irrespective of recoveryEnabled as apps
      // need events to move to further states.
      rmStore.start();

      if(recoveryEnabled) {
        try {
          LOG.info("Recovery started");
          rmStore.checkVersion();
          if (rmContext.isWorkPreservingRecoveryEnabled()) {
            rmContext.setEpoch(rmStore.getAndIncrementEpoch());
          }
          RMState state = rmStore.loadState();
          recover(state);
          LOG.info("Recovery ended");

          // Make sure that the App is cleaned up after the RM memory is restored.
          if (HAUtil.isFederationEnabled(conf)) {
            federationStateStoreService.
                createCleanUpFinishApplicationThread("Recovery");
          }

        } catch (Exception e) {
          // the Exception from loadState() needs to be handled for
          // HA and we need to give up master status if we got fenced
          LOG.error("Failed to load/recover state", e);
          throw e;
        }
      } else {
        if (HAUtil.isFederationEnabled(conf)) {
          long epoch = conf.getLong(YarnConfiguration.RM_EPOCH,
              YarnConfiguration.DEFAULT_RM_EPOCH);
          rmContext.setEpoch(epoch);
          LOG.info("Epoch set for Federation: " + epoch);
        }
      }

      super.serviceStart();
    }

    @Override
    protected void serviceStop() throws Exception {

      super.serviceStop();

      DefaultMetricsSystem.shutdown();
      // unregister rmnmInfo bean
      if (rmnmInfo != null) {
        rmnmInfo.unregister();
      }
      if (rmContext != null) {
        RMStateStore store = rmContext.getStateStore();
        try {
          if (null != store) {
            store.close();
          }
        } catch (Exception e) {
          LOG.error("Error closing store.", e);
        }
      }
      if (eventQueueMetricExecutor != null) {
        eventQueueMetricExecutor.shutdownNow();
      }

    }
  }
  //主要用于 处理 RM 发生的严重错误，决定是 进入 Standby 模式 还是 直接关闭 RM
  @Private
  private class RMFatalEventDispatcher implements EventHandler<RMFatalEvent> {
    @Override
    public void handle(RMFatalEvent event) {
      LOG.error("Received " + event);
      //如果启用了 HA（高可用），则进入 Standby
      if (HAUtil.isHAEnabled(getConfig())) {
        // If we're in an HA config, the right answer is always to go into
        // standby.
        LOG.warn("Transitioning the resource manager to standby.");
        handleTransitionToStandByInNewThread();
      } else {
        // If we're stand-alone, we probably want to shut down, but the if and
        // how depends on the event.
        switch(event.getType()) {
          //STATE_STORE_FENCED：状态存储被 Fencing（锁定）
        case STATE_STORE_FENCED:
          LOG.error(FATAL, "State store fenced even though the resource " +
              "manager is not configured for high availability. Shutting " +
              "down this resource manager to protect the integrity of the " +
              "state store.");
          ExitUtil.terminate(1, event.getExplanation());
          break;
          //STATE_STORE_OP_FAILED：状态存储操作失败
        case STATE_STORE_OP_FAILED:
          if (YarnConfiguration.shouldRMFailFast(getConfig())) {
            LOG.error(FATAL, "Shutting down the resource manager because a " +
                "state store operation failed, and the resource manager is " +
                "configured to fail fast. See the yarn.fail-fast and " +
                "yarn.resourcemanager.fail-fast properties.");
            ExitUtil.terminate(1, event.getExplanation());
          } else {
            LOG.warn("Ignoring state store operation failure because the " +
                "resource manager is not configured to fail fast. See the " +
                "yarn.fail-fast and yarn.resourcemanager.fail-fast " +
                "properties.");
          }
          break;
        default:
          LOG.error(FATAL, "Shutting down the resource manager.");
          ExitUtil.terminate(1, event.getExplanation());
        }
      }
    }
  }
  // 与 EventDispatcher 相比，它新增了事件处理器的 CPU 监控功能，
  // 通过 EventProcessorMonitor 线程周期性地采样 EventProcessor 线程的 CPU 使用情况，
  // 并将统计结果上报到 ClusterMetrics 进行监控
  @Private
  private class SchedulerEventDispatcher extends
      EventDispatcher<SchedulerEvent> {
    //事件处理器监控线程，用于周期性检测 EventProcessor 线程的 CPU 使用情况，并更新 ClusterMetrics
    private final Thread eventProcessorMonitor;
    //samplesPerMin：每分钟采样次数（用于 CPU 监控）
    SchedulerEventDispatcher(String name, int samplesPerMin) {
      super(scheduler, name);
      this.eventProcessorMonitor =
          new Thread(new EventProcessorMonitor(getEventProcessorId(),
              samplesPerMin));
      this.eventProcessorMonitor
          .setName("ResourceManager Event Processor Monitor");
    }
    // EventProcessorMonitor keeps track of how much CPU the EventProcessor
    // thread is using. It takes a configurable number of samples per minute,
    // and then reports the Avg and Max of previous 60 seconds as cluster
    // metrics. Units are usecs per second of CPU used.
    // Avg is not accurate until one minute of samples have been received.
    private final class EventProcessorMonitor implements Runnable {
      private final long tid; //被监控的 EventProcessor 线程 ID
      private final boolean run;//是否启用监控（ThreadMXBean 是否支持 ThreadCpuTime）
      private final ThreadMXBean tmxb;//Java ThreadMXBean，用于获取线程的 CPU 使用情况
      private final ClusterMetrics clusterMetrics = ClusterMetrics.getMetrics();
      private final int samples;//每分钟采样的次数
      EventProcessorMonitor(long id, int samplesPerMin) {
        assert samplesPerMin > 0;
        this.tid = id;
        this.samples = samplesPerMin;
        this.tmxb = ManagementFactory.getThreadMXBean();
        if (clusterMetrics != null &&
            tmxb != null && tmxb.isThreadCpuTimeSupported()) {
          this.run = true;
          clusterMetrics.setRmEventProcMonitorEnable(true);
        } else {
          this.run = false;
        }
      }
      public void run() {
        int index = 0;
        long[] values = new long[samples];
        int sleepMs = (60 * 1000) / samples;

        while (run && !isStopped() && !Thread.currentThread().isInterrupted()) {
          try {
            long cpuBefore = tmxb.getThreadCpuTime(tid);
            long wallClockBefore = Time.monotonicNow();
            Thread.sleep(sleepMs);
            long wallClockDelta = Time.monotonicNow() - wallClockBefore;
            long cpuDelta = tmxb.getThreadCpuTime(tid) - cpuBefore;

            // Nanoseconds / Milliseconds = usec per second
            values[index] = cpuDelta / wallClockDelta;

            index = (index + 1) % samples;
            long max = 0;
            long sum = 0;
            for (int i = 0; i < samples; i++) {
              sum += values[i];
              max = Math.max(max, values[i]);
            }
            clusterMetrics.setRmEventProcCPUAvg(sum / samples);
            clusterMetrics.setRmEventProcCPUMax(max);
          } catch (InterruptedException e) {
            LOG.error("Returning, interrupted : " + e);
            return;
          }
        }
      }
    }
    @Override
    protected void serviceStart() throws Exception {
      super.serviceStart();
      this.eventProcessorMonitor.start();
    }

    @Override
    protected void serviceStop() throws Exception {
      super.serviceStop();
      this.eventProcessorMonitor.interrupt();
      try {
        this.eventProcessorMonitor.join();
      } catch (InterruptedException e) {
        throw new YarnRuntimeException(e);
      }
    }
  }

    /**
   * Transition to standby state in a new thread. The transition operation is
   * asynchronous to avoid deadlock caused by cyclic dependency.
   */
  private void handleTransitionToStandByInNewThread() {
    Thread standByTransitionThread =
        new Thread(activeServices.standByTransitionRunnable);
    standByTransitionThread.setName("StandByTransitionThread");
    standByTransitionThread.start();
  }

  /**
   * The class to transition RM to standby state. The same
   * {@link StandByTransitionRunnable} object could be used in multiple threads,
   * but runs only once. That's because RM can go back to active state after
   * transition to standby state, the same runnable in the old context can't
   * transition RM to standby state again. A new runnable is created every time
   * RM transitions to active state.
   */
  //用于将 YARN 资源管理器（ResourceManager，RM）切换到 Standby 模式。
  //在 高可用（HA）模式 下，RM 可能因为错误或外部事件（如领导者选举）需要 从 Active 切换到 Standby，
  // 这个类确保 切换操作只执行一次，即使多个线程同时触发
    //安全地将 RM 切换到 Standby 模式
  private class StandByTransitionRunnable implements Runnable {
    // The atomic variable to make sure multiple threads with the same runnable
    // run only once.
    //用于保证 run() 只能执行一次
    private final AtomicBoolean hasAlreadyRun = new AtomicBoolean(false);

    @Override
    public void run() {
      // Run this only once, even if multiple threads end up triggering
      // this simultaneously.
      //如果 hasAlreadyRun 是 false，则设为 true 并返回原值 false，表示当前线程是第一个执行的
      if (hasAlreadyRun.getAndSet(true)) {
        return;
      }

      if (rmContext.isHAEnabled()) {
        try {
          // Transition to standby and reinit active services
          LOG.info("Transitioning RM to Standby mode");
          //切换 RM 到 Standby 模式，并重新初始化相关服务
          transitionToStandby(true);
          EmbeddedElector elector = rmContext.getLeaderElectorService();
          if (elector != null) {
            elector.rejoinElection();
          }
        } catch (Exception e) {
          LOG.error(FATAL, "Failed to transition RM to Standby mode.", e);
          ExitUtil.terminate(1, e);
        }
      }
    }
  }
  //事件分发器，用于处理 应用（Application） 相关的事件
  @Private
  public static final class ApplicationEventDispatcher implements
      EventHandler<RMAppEvent> {

    private final RMContext rmContext;

    public ApplicationEventDispatcher(RMContext rmContext) {
      this.rmContext = rmContext;
    }

    @Override
    public void handle(RMAppEvent event) {
      ApplicationId appID = event.getApplicationId();
      RMApp rmApp = this.rmContext.getRMApps().get(appID);
      if (rmApp != null) {
        try {
          rmApp.handle(event);
        } catch (Throwable t) {
          LOG.error("Error in handling event type " + event.getType()
              + " for application " + appID, t);
        }
      }
    }
  }
  //事件分发器，用于处理 应用尝试（Application Attempt） 相关的事件
  @Private
  public static final class ApplicationAttemptEventDispatcher implements
      EventHandler<RMAppAttemptEvent> {

    private final RMContext rmContext;

    public ApplicationAttemptEventDispatcher(RMContext rmContext) {
      this.rmContext = rmContext;
    }

    @Override
    public void handle(RMAppAttemptEvent event) {
      //解析事件对应的 ApplicationAttemptId 和 ApplicationId
      ApplicationAttemptId appAttemptId = event.getApplicationAttemptId();
      ApplicationId appId = appAttemptId.getApplicationId();
      //通过 ApplicationId 获取 RMApp，代表 该应用的整体信息
      RMApp rmApp = this.rmContext.getRMApps().get(appId);
      if (rmApp != null) {
        RMAppAttempt rmAppAttempt = rmApp.getRMAppAttempt(appAttemptId);
        if (rmAppAttempt != null) {
          try {
            // 处理事件
            rmAppAttempt.handle(event);
          } catch (Throwable t) {
            LOG.error("Error in handling event type " + event.getType()
                + " for applicationAttempt " + appAttemptId, t);
          }
          //处理 AM 重启的情况
        } else if (rmApp.getApplicationSubmissionContext() != null
            //是否启用了 AM 容器保持（即 Work-Preserving AM Restart）
            //在 AM 容器保持模式 下，即使 当前 RMAppAttempt 已删除，仍然可能会有 CONTAINER_FINISHED 事件到达
            && rmApp.getApplicationSubmissionContext()
            .getKeepContainersAcrossApplicationAttempts()
            && event.getType() == RMAppAttemptEventType.CONTAINER_FINISHED) {
          // For work-preserving AM restart, failed attempts are still
          // capturing CONTAINER_FINISHED events and record the finished
          // containers which will be used by current attempt.
          // We just keep 'yarn.resourcemanager.am.max-attempts' in
          // RMStateStore. If the finished container's attempt is deleted, we
          // use the first attempt in app.attempts to deal with these events.

          RMAppAttempt previousFailedAttempt =
              rmApp.getAppAttempts().values().iterator().next();
          if (previousFailedAttempt != null) {
            try {
              LOG.debug("Event {} handled by {}", event.getType(),
                  previousFailedAttempt);
              previousFailedAttempt.handle(event);
            } catch (Throwable t) {
              LOG.error("Error in handling event type " + event.getType()
                  + " for applicationAttempt " + appAttemptId
                  + " with " + previousFailedAttempt, t);
            }
          } else {
            LOG.error("Event " + event.getType()
                + " not handled, because previousFailedAttempt is null");
          }
        }
      }
    }
  }
  //节点事件处理器
  @Private
  public static final class NodeEventDispatcher implements
      EventHandler<RMNodeEvent> {

    private final RMContext rmContext;

    public NodeEventDispatcher(RMContext rmContext) {
      this.rmContext = rmContext;
    }

    @Override
    public void handle(RMNodeEvent event) {
      NodeId nodeId = event.getNodeId();
      //转发给对应的RMNode处理
      RMNode node = this.rmContext.getRMNodes().get(nodeId);
      if (node != null) {
        try {
          ((EventHandler<RMNodeEvent>) node).handle(event);
        } catch (Throwable t) {
          LOG.error("Error in handling event type " + event.getType()
              + " for node " + nodeId, t);
        }
      }
    }
  }

  /**
   * Return a HttpServer.Builder that the journalnode / namenode / secondary
   * namenode can use to initialize their HTTP / HTTPS server.
   *
   * @param conf configuration object
   * @param httpAddr HTTP address
   * @param httpsAddr HTTPS address
   * @param name  Name of the server
   * @throws IOException from Builder
   * @return builder object
   */
  public static HttpServer2.Builder httpServerTemplateForRM(Configuration conf,
      final InetSocketAddress httpAddr, final InetSocketAddress httpsAddr,
      String name) throws IOException {
    HttpServer2.Builder builder = new HttpServer2.Builder().setName(name)
        .setConf(conf).setSecurityEnabled(false);

    if (httpAddr.getPort() == 0) {
      builder.setFindPort(true);
    }

    URI uri = URI.create("http://" + NetUtils.getHostPortString(httpAddr));
    builder.addEndpoint(uri);
    LOG.info("Starting Web-server for " + name + " at: " + uri);

    return builder;
  }

  protected void startWepApp() {
    Map<String, String> serviceConfig = null;
    Configuration conf = getConfig();

    RMWebAppUtil.setupSecurityAndFilters(conf,
        getClientRMService().rmDTSecretManager);

    Map<String, String> params = new HashMap<String, String>();
    if (getConfig().getBoolean(YarnConfiguration.YARN_API_SERVICES_ENABLE,
        false)) {
      String apiPackages = "org.apache.hadoop.yarn.service.webapp;" +
          "org.apache.hadoop.yarn.webapp";
      params.put("com.sun.jersey.config.property.resourceConfigClass",
          "com.sun.jersey.api.core.PackagesResourceConfig");
      params.put("com.sun.jersey.config.property.packages", apiPackages);
    }

    Builder<ResourceManager> builder =
        WebApps
            .$for("cluster", ResourceManager.class, this,
                "ws")
            .with(conf)
            .withServlet("API-Service", "/app/*",
                ServletContainer.class, params, false)
            .withHttpSpnegoPrincipalKey(
                YarnConfiguration.RM_WEBAPP_SPNEGO_USER_NAME_KEY)
            .withHttpSpnegoKeytabKey(
                YarnConfiguration.RM_WEBAPP_SPNEGO_KEYTAB_FILE_KEY)
            .withCSRFProtection(YarnConfiguration.RM_CSRF_PREFIX)
            .withXFSProtection(YarnConfiguration.RM_XFS_PREFIX)
            .at(webAppAddress);
    String proxyHostAndPort = rmContext.getProxyHostAndPort(conf);
    if(WebAppUtils.getResolvedRMWebAppURLWithoutScheme(conf).
        equals(proxyHostAndPort)) {
      if (HAUtil.isHAEnabled(conf)) {
        fetcher = new DefaultAppReportFetcher(conf);
      } else {
        fetcher = new DefaultAppReportFetcher(conf, getClientRMService());
      }
      builder.withServlet(ProxyUriUtils.PROXY_SERVLET_NAME,
          ProxyUriUtils.PROXY_PATH_SPEC, WebAppProxyServlet.class);
      builder.withAttribute(WebAppProxy.PROXY_CA,
          rmContext.getProxyCAManager().getProxyCA());
      builder.withAttribute(WebAppProxy.FETCHER_ATTRIBUTE, fetcher);
      String[] proxyParts = proxyHostAndPort.split(":");
      builder.withAttribute(WebAppProxy.PROXY_HOST_ATTRIBUTE, proxyParts[0]);
    }

    WebAppContext uiWebAppContext = null;
    if (getConfig().getBoolean(YarnConfiguration.YARN_WEBAPP_UI2_ENABLE,
        YarnConfiguration.DEFAULT_YARN_WEBAPP_UI2_ENABLE)) {
      String onDiskPath = getConfig()
          .get(YarnConfiguration.YARN_WEBAPP_UI2_WARFILE_PATH);

      uiWebAppContext = new WebAppContext();
      uiWebAppContext.setContextPath(UI2_WEBAPP_NAME);

      if (null == onDiskPath) {
        String war = "hadoop-yarn-ui-" + VersionInfo.getVersion() + ".war";
        URL url = getClass().getClassLoader().getResource(war);

        if (null == url) {
          onDiskPath = getWebAppsPath("ui2");
        } else {
          onDiskPath = url.getFile();
        }
      }
      if (onDiskPath == null || onDiskPath.isEmpty()) {
          LOG.error("No war file or webapps found for ui2 !");
      } else {
        if (onDiskPath.endsWith(".war")) {
          uiWebAppContext.setWar(onDiskPath);
          LOG.info("Using war file at: " + onDiskPath);
        } else {
          uiWebAppContext.setResourceBase(onDiskPath);
          LOG.info("Using webapps at: " + onDiskPath);
        }
      }
    }

    builder.withAttribute(IsResourceManagerActiveServlet.RM_ATTRIBUTE, this);
    builder.withServlet(IsResourceManagerActiveServlet.SERVLET_NAME,
        IsResourceManagerActiveServlet.PATH_SPEC,
        IsResourceManagerActiveServlet.class);

    try {
      webApp = builder.start(new RMWebApp(this), uiWebAppContext);
    } catch (WebAppException e) {
      webApp = e.getWebApp();
      throw e;
    }
  }

  private String getWebAppsPath(String appName) {
    URL url = getClass().getClassLoader().getResource("webapps/" + appName);
    if (url == null) {
      return "";
    }
    return url.toString();
  }

  /**
   * Helper method to create and init {@link #activeServices}. This creates an
   * instance of {@link RMActiveServices} and initializes it.
   *
   * @param fromActive Indicates if the call is from the active state transition
   *                   or the RM initialization.
   */
  protected void createAndInitActiveServices(boolean fromActive) {
    activeServices = new RMActiveServices(this);
    activeServices.fromActive = fromActive;
    activeServices.init(conf);
  }

  /**
   * Helper method to start {@link #activeServices}.
   * @throws Exception
   */
  void startActiveServices() throws Exception {
    if (activeServices != null) {
      clusterTimeStamp = System.currentTimeMillis();
      activeServices.start();
    }
  }

  /**
   * Helper method to stop {@link #activeServices}.
   * @throws Exception
   */
  void stopActiveServices() {
    if (activeServices != null) {
      activeServices.stop();
      activeServices = null;
    }
  }

  void reinitialize(boolean initialize) {
    ClusterMetrics.destroy();
    QueueMetrics.clearQueueMetrics();
    getResourceScheduler().resetSchedulerMetrics();
    if (initialize) {
      resetRMContext();
      createAndInitActiveServices(true);
    }
  }

  @VisibleForTesting
  protected boolean areActiveServicesRunning() {
    return activeServices != null && activeServices.isInState(STATE.STARTED);
  }

  synchronized void transitionToActive() throws Exception {
    if (rmContext.getHAServiceState() == HAServiceProtocol.HAServiceState.ACTIVE) {
      LOG.info("Already in active state");
      return;
    }
    LOG.info("Transitioning to active state");

    this.rmLoginUGI.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        try {
          startActiveServices();
          return null;
        } catch (Exception e) {
          reinitialize(true);
          throw e;
        }
      }
    });

    rmContext.setHAServiceState(HAServiceProtocol.HAServiceState.ACTIVE);
    LOG.info("Transitioned to active state");
  }
  //实现了 ResourceManager (RM) 的 Standby 状态切换逻辑，在 高可用 (HA) 模式 下，RM 需要从 Active 切换到 Standby 时调用
  //initialize 控制是否需要 重新初始化 RM 相关服务
  synchronized void transitionToStandby(boolean initialize)
      throws Exception {
    //如果已经是 Standby，则直接返回
    if (rmContext.getHAServiceState() ==
        HAServiceProtocol.HAServiceState.STANDBY) {
      LOG.info("Already in standby state");
      return;
    }

    LOG.info("Transitioning to standby state");
    //记录当前 RM 状态（可能是 ACTIVE）
    HAServiceState state = rmContext.getHAServiceState();
    //将 RM 状态切换为 STANDBY，表示不再负责调度和管理应用程序
    rmContext.setHAServiceState(HAServiceProtocol.HAServiceState.STANDBY);
    if (state == HAServiceProtocol.HAServiceState.ACTIVE) {
      //如果 RM 之前是 Active
      //停止所有 Active 状态下运行的服务 (stopActiveServices())
      stopActiveServices();
      reinitialize(initialize);
    }
    LOG.info("Transitioned to standby state");
  }

  @Override
  protected void serviceStart() throws Exception {
    if (this.rmContext.isHAEnabled()) {
      transitionToStandby(false);
    }

    startWepApp();
    if (getConfig().getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER,
        false)) {
      int port = webApp.port();
      WebAppUtils.setRMWebAppPort(conf, port);
    }
    super.serviceStart();

    // Non HA case, start after RM services are started.
    if (!this.rmContext.isHAEnabled()) {
      transitionToActive();
    }
  }
  
  protected void doSecureLogin() throws IOException {
	InetSocketAddress socAddr = getBindAddress(conf);
    SecurityUtil.login(this.conf, YarnConfiguration.RM_KEYTAB,
        YarnConfiguration.RM_PRINCIPAL, socAddr.getHostName());

    // if security is enable, set rmLoginUGI as UGI of loginUser
    if (UserGroupInformation.isSecurityEnabled()) {
      this.rmLoginUGI = UserGroupInformation.getLoginUser();
    }
  }

  @Override
  protected void serviceStop() throws Exception {
    if (webApp != null) {
      webApp.stop();
    }
    if (fetcher != null) {
      fetcher.stop();
    }
    if (configurationProvider != null) {
      configurationProvider.close();
    }
    super.serviceStop();
    if (zkManager != null) {
      zkManager.close();
    }
    transitionToStandby(false);
    rmContext.setHAServiceState(HAServiceState.STOPPING);
    rmStatusInfoBean.unregister();
  }
  
  protected ResourceTrackerService createResourceTrackerService() {
    return new ResourceTrackerService(this.rmContext, this.nodesListManager,
        this.nmLivelinessMonitor,
        this.rmContext.getContainerTokenSecretManager(),
        this.rmContext.getNMTokenSecretManager());
  }

  protected ClientRMService createClientRMService() {
    return new ClientRMService(this.rmContext, scheduler, this.rmAppManager,
        this.applicationACLsManager, this.queueACLsManager,
        this.rmContext.getRMDelegationTokenSecretManager());
  }

  protected ApplicationMasterService createApplicationMasterService() {
    Configuration config = this.rmContext.getYarnConfiguration();
    if (isOpportunisticSchedulingEnabled(conf)) {
      if (YarnConfiguration.isDistSchedulingEnabled(config) &&
          !YarnConfiguration
              .isOpportunisticContainerAllocationEnabled(config)) {
        throw new YarnRuntimeException(
            "Invalid parameters: opportunistic container allocation has to " +
                "be enabled when distributed scheduling is enabled.");
      }
      OpportunisticContainerAllocatorAMService
          oppContainerAllocatingAMService =
          new OpportunisticContainerAllocatorAMService(this.rmContext,
              scheduler);
      this.rmContext.setContainerQueueLimitCalculator(
          oppContainerAllocatingAMService.getNodeManagerQueueLimitCalculator());
      return oppContainerAllocatingAMService;
    }
    return new ApplicationMasterService(this.rmContext, scheduler);
  }
  //创建管理服务
  protected AdminService createAdminService() {
    return new AdminService(this);
  }
  //创建委托令牌管理服务
  protected RMSecretManagerService createRMSecretManagerService() {
    return new RMSecretManagerService(conf, rmContext);
  }

  private boolean isOpportunisticSchedulingEnabled(Configuration conf) {
    return YarnConfiguration.isOpportunisticContainerAllocationEnabled(conf)
        || YarnConfiguration.isDistSchedulingEnabled(conf);
  }

  /**
   * Create RMDelegatedNodeLabelsUpdater based on configuration.
   * @return RMDelegatedNodeLabelsUpdater.
   */
  protected RMDelegatedNodeLabelsUpdater createRMDelegatedNodeLabelsUpdater() {
    if (conf.getBoolean(YarnConfiguration.NODE_LABELS_ENABLED,
            YarnConfiguration.DEFAULT_NODE_LABELS_ENABLED)
        && YarnConfiguration.isDelegatedCentralizedNodeLabelConfiguration(
            conf)) {
      return new RMDelegatedNodeLabelsUpdater(rmContext);
    } else {
      return null;
    }
  }

  @Private
  public ClientRMService getClientRMService() {
    return this.clientRM;
  }

  /**
   * return the scheduler.
   * @return the scheduler for the Resource Manager.
   */
  @Private
  public ResourceScheduler getResourceScheduler() {
    return this.scheduler;
  }

  /**
   * return the resource tracking component.
   * @return the resource tracking component.
   */
  @Private
  public ResourceTrackerService getResourceTrackerService() {
    return this.resourceTracker;
  }

  @Private
  public ApplicationMasterService getApplicationMasterService() {
    return this.masterService;
  }

  @Private
  public ApplicationACLsManager getApplicationACLsManager() {
    return this.applicationACLsManager;
  }

  @Private
  public QueueACLsManager getQueueACLsManager() {
    return this.queueACLsManager;
  }

  @Private
  @VisibleForTesting
  public FederationStateStoreService getFederationStateStoreService() {
    return this.federationStateStoreService;
  }

  @Private
  WebApp getWebapp() {
    return this.webApp;
  }

  @Override
  public void recover(RMState state) throws Exception {
    // recover RMdelegationTokenSecretManager
    rmContext.getRMDelegationTokenSecretManager().recover(state);

    // recover AMRMTokenSecretManager
    rmContext.getAMRMTokenSecretManager().recover(state);

    // recover reservations
    if (reservationSystem != null) {
      reservationSystem.recover(state);
    }
    // recover applications
    rmAppManager.recover(state);

    // recover ProxyCA
    rmContext.getProxyCAManager().recover(state);

    setSchedulerRecoveryStartAndWaitTime(state, conf);
  }

  public static void main(String argv[]) {
    Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
    StringUtils.startupShutdownMessage(ResourceManager.class, argv, LOG);
    try {
      Configuration conf = new YarnConfiguration();
      GenericOptionsParser hParser = new GenericOptionsParser(conf, argv);
      argv = hParser.getRemainingArgs();
      // If -format-state-store, then delete RMStateStore; else startup normally
      if (argv.length >= 1) {
        if (argv[0].equals("-format-state-store")) {
          deleteRMStateStore(conf);
        } else if (argv[0].equals("-format-conf-store")) {
          deleteRMConfStore(conf);
        } else if (argv[0].equals("-remove-application-from-state-store")
            && argv.length == 2) {
          removeApplication(conf, argv[1]);
        } else {
          printUsage(System.err);
        }
      } else {
        ResourceManager resourceManager = new ResourceManager();
        ShutdownHookManager.get().addShutdownHook(
          new CompositeServiceShutdownHook(resourceManager),
          SHUTDOWN_HOOK_PRIORITY);
        resourceManager.init(conf);
        resourceManager.start();
      }
    } catch (Throwable t) {
      LOG.error(FATAL, "Error starting ResourceManager", t);
      System.exit(-1);
    }
  }

  /**
   * Register the handlers for alwaysOn services
   */
  private Dispatcher setupDispatcher() {
    Dispatcher dispatcher = createDispatcher();
    dispatcher.register(RMFatalEventType.class,
        new ResourceManager.RMFatalEventDispatcher());
    return dispatcher;
  }

  private void resetRMContext() {
    RMContextImpl rmContextImpl = new RMContextImpl();
    // transfer service context to new RM service Context
    rmContextImpl.setServiceContext(rmContext.getServiceContext());

    // reset dispatcher
    Dispatcher dispatcher = setupDispatcher();
    ((Service) dispatcher).init(this.conf);
    ((Service) dispatcher).start();
    removeService((Service) rmDispatcher);
    // Need to stop previous rmDispatcher before assigning new dispatcher
    // otherwise causes "AsyncDispatcher event handler" thread leak
    ((Service) rmDispatcher).stop();
    rmDispatcher = dispatcher;
    addIfService(rmDispatcher);
    rmContextImpl.setDispatcher(dispatcher);

    rmContext = rmContextImpl;
  }

  private void setSchedulerRecoveryStartAndWaitTime(RMState state,
      Configuration conf) {
    if (!state.getApplicationState().isEmpty()) {
      long waitTime =
          conf.getLong(YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_SCHEDULING_WAIT_MS,
            YarnConfiguration.DEFAULT_RM_WORK_PRESERVING_RECOVERY_SCHEDULING_WAIT_MS);
      rmContext.setSchedulerRecoveryStartAndWaitTime(waitTime);
    }
  }

  /**
   * Retrieve RM bind address from configuration.
   * 
   * @param conf Configuration.
   * @return InetSocketAddress
   */
  public static InetSocketAddress getBindAddress(Configuration conf) {
    return conf.getSocketAddr(YarnConfiguration.RM_ADDRESS,
      YarnConfiguration.DEFAULT_RM_ADDRESS, YarnConfiguration.DEFAULT_RM_PORT);
  }

  /**
   * Deletes the RMStateStore
   *
   * @param conf Configuration.
   * @throws Exception error occur.
   */
  @VisibleForTesting
  static void deleteRMStateStore(Configuration conf) throws Exception {
    RMStateStore rmStore = RMStateStoreFactory.getStore(conf);
    rmStore.setResourceManager(new ResourceManager());
    rmStore.init(conf);
    rmStore.start();
    try {
      LOG.info("Deleting ResourceManager state store...");
      rmStore.deleteStore();
      LOG.info("State store deleted");
    } finally {
      rmStore.stop();
    }
  }

  /**
   * Deletes the YarnConfigurationStore
   *
   * @param conf
   * @throws Exception
   */
  @VisibleForTesting
  static void deleteRMConfStore(Configuration conf) throws Exception {
    ResourceManager rm = new ResourceManager();
    rm.conf = conf;
    ResourceScheduler scheduler = rm.createScheduler();
    RMContextImpl rmContext = new RMContextImpl();
    rmContext.setResourceManager(rm);

    boolean isConfigurationMutable = false;
    String confProviderStr = conf.get(
        YarnConfiguration.SCHEDULER_CONFIGURATION_STORE_CLASS,
        YarnConfiguration.DEFAULT_CONFIGURATION_STORE);
    switch (confProviderStr) {
      case YarnConfiguration.MEMORY_CONFIGURATION_STORE:
      case YarnConfiguration.LEVELDB_CONFIGURATION_STORE:
      case YarnConfiguration.ZK_CONFIGURATION_STORE:
      case YarnConfiguration.FS_CONFIGURATION_STORE:
        isConfigurationMutable = true;
        break;
      default:
    }

    if (scheduler instanceof MutableConfScheduler && isConfigurationMutable) {
      try (YarnConfigurationStore confStore = YarnConfigurationStoreFactory
          .getStore(conf)) {
        confStore.initialize(conf, conf, rmContext);
        confStore.format();
      }
    } else {
      System.out.println(String.format("Scheduler Configuration format only " +
          "supported by %s.", MutableConfScheduler.class.getSimpleName()));
    }
  }

  @VisibleForTesting
  static void removeApplication(Configuration conf, String applicationId)
      throws Exception {
    RMStateStore rmStore = RMStateStoreFactory.getStore(conf);
    rmStore.setResourceManager(new ResourceManager());
    rmStore.init(conf);
    rmStore.start();
    try {
      ApplicationId removeAppId = ApplicationId.fromString(applicationId);
      LOG.info("Deleting application " + removeAppId + " from state store");
      rmStore.removeApplication(removeAppId);
      LOG.info("Application is deleted from state store");
    } finally {
      rmStore.stop();
    }
  }

  private static void printUsage(PrintStream out) {
    out.println("Usage: yarn resourcemanager [-format-state-store]");
    out.println("                            "
        + "[-remove-application-from-state-store <appId>]");
    out.println("                            "
        + "[-format-conf-store]" + "\n");

  }

  protected RMAppLifetimeMonitor createRMAppLifetimeMonitor() {
    return new RMAppLifetimeMonitor(this.rmContext);
  }

  /**
   * Register ResourceManagerMXBean.
   */
  private void registerMXBean() {
    MBeans.register("ResourceManager", "ResourceManager", this);
  }

  @Override
  public boolean isSecurityEnabled() {
    return UserGroupInformation.isSecurityEnabled();
  }
}
