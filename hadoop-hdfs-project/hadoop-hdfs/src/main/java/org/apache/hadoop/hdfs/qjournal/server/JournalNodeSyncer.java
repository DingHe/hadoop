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
package org.apache.hadoop.hdfs.qjournal.server;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;

import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.qjournal.protocol.InterQJournalProtocol;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEditLogManifestResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocolPB.InterQJournalProtocolPB;
import org.apache.hadoop.hdfs.qjournal.protocolPB.InterQJournalProtocolTranslatorPB;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A Journal Sync thread runs through the lifetime of the JN. It periodically
 * gossips with other journal nodes to compare edit log manifests and if it
 * detects any missing log segment, it downloads it from the other journal node
 */
// JournalNodeSyncer 是 HDFS JournalNode 组件中的一个同步线程。它的主要作用是在 JournalNode 的整个生命周期内运行，
// 定期与其他 JournalNode 进行通信（gossip），比较各自的 edit log 清单。
// 如果发现本地缺少某些日志片段（log segment），它会从其他 JournalNode 下载这些缺失的日志，以保持日志数据的一致性.
@InterfaceAudience.Private
public class JournalNodeSyncer {
  public static final Logger LOG = LoggerFactory.getLogger(
      JournalNodeSyncer.class);
  //该 JournalNodeSyncer 关联的 JournalNode 实例。
  private final JournalNode jn;
  //该 JournalNodeSyncer 负责同步的 Journal 实例。
  private final Journal journal;
  //该 Journal 对应的 journalId，用于唯一标识一个 Journal。
  private final String jid;
  //HDFS NameService 的 ID，支持 HDFS Federation。
  private  String nameServiceId;
  private final JNStorage jnStorage;
  private final Configuration conf;
  private volatile Daemon syncJournalDaemon;
  //控制同步操作是否应继续运行，true 表示应继续，false 表示应停止。
  private volatile boolean shouldSync = true;
  //存储其他 JournalNode 的代理，用于与其他 JournalNode 进行通信。
  private List<JournalNodeProxy> otherJNProxies = Lists.newArrayList();
  //其他可用的 JournalNode 数量。
  private int numOtherJNs;
  //记录当前正在与哪个 JournalNode 进行同步。
  private int journalNodeIndexForSync = 0;
  //日志同步的时间间隔（从 conf 配置中读取）。
  private final long journalSyncInterval;
  //日志段传输的超时时间（从 conf 配置中读取）。
  private final int logSegmentTransferTimeout;
  //用于控制数据传输速率的限流器（DataTransferThrottler）。
  private final DataTransferThrottler throttler;
  private final JournalMetrics metrics;
  //标志同步线程是否已经启动。
  private boolean journalSyncerStarted;

  JournalNodeSyncer(JournalNode jouranlNode, Journal journal, String jid,
      Configuration conf, String nameServiceId) {
    this.jn = jouranlNode;
    this.journal = journal;
    this.jid = jid;
    this.nameServiceId = nameServiceId;
    this.jnStorage = journal.getStorage();
    this.conf = conf;
    journalSyncInterval = conf.getLong(
        DFSConfigKeys.DFS_JOURNALNODE_SYNC_INTERVAL_KEY,
        DFSConfigKeys.DFS_JOURNALNODE_SYNC_INTERVAL_DEFAULT);
    logSegmentTransferTimeout = conf.getInt(
        DFSConfigKeys.DFS_EDIT_LOG_TRANSFER_TIMEOUT_KEY,
        DFSConfigKeys.DFS_EDIT_LOG_TRANSFER_TIMEOUT_DEFAULT);
    throttler = getThrottler(conf);
    metrics = journal.getMetrics();
    journalSyncerStarted = false;
  }

  void stopSync() {
    shouldSync = false;
    // Delete the edits.sync directory
    File editsSyncDir = journal.getStorage().getEditsSyncDir();
    if (editsSyncDir.exists()) {
      FileUtil.fullyDelete(editsSyncDir);
    }
    if (syncJournalDaemon != null) {
      syncJournalDaemon.interrupt();
    }
  }

  public void start(String nsId) {
    if (nsId != null) {
      this.nameServiceId = nsId;
      journal.setTriedJournalSyncerStartedwithnsId(true);
    }
    if (!journalSyncerStarted && getOtherJournalNodeProxies()) {
      LOG.info("Starting SyncJournal daemon for journal " + jid);
      startSyncJournalsDaemon();
      journalSyncerStarted = true;
    }

  }

  public boolean isJournalSyncerStarted() {
    return journalSyncerStarted;
  }
  //创建编辑日志同步的临时目录
  private boolean createEditsSyncDir() {
    File editsSyncDir = journal.getStorage().getEditsSyncDir();
    if (editsSyncDir.exists()) {
      LOG.info(editsSyncDir + " directory already exists.");
      return true;
    }
    return editsSyncDir.mkdir();
  }
  //获取其他 JournalNode 的地址，以便进行日志同步
  //为每个 JournalNode 创建代理对象 JournalNodeProxy，用于后续的 RPC 交互
  //确保至少有一个可用的 JournalNode 进行同步，否则返回 false
  private boolean getOtherJournalNodeProxies() {
    //获取其他 JournalNode 的 InetSocketAddress 列表
    List<InetSocketAddress> otherJournalNodes = getOtherJournalNodeAddrs();
    if (otherJournalNodes == null || otherJournalNodes.isEmpty()) {
      LOG.warn("Other JournalNode addresses not available. Journal Syncing " +
          "cannot be done");
      return false;
    }
    //遍历 JournalNode 地址列表，尝试为每个 JournalNode 创建一个 JournalNodeProxy
    for (InetSocketAddress addr : otherJournalNodes) {
      try {
        otherJNProxies.add(new JournalNodeProxy(addr));
      } catch (IOException e) {
        LOG.warn("Could not add proxy for Journal at addresss " + addr, e);
      }
    }
    // Check if there are any other JournalNodes before starting the sync.  Although some proxies
    // may be unresolved now, the act of attempting to sync will instigate resolution when the
    // servers become available.
    if (otherJNProxies.isEmpty()) {
      LOG.error("Cannot sync as there is no other JN available for sync.");
      return false;
    }
    numOtherJNs = otherJNProxies.size();
    return true;
  }
  //用于启动一个后台守护线程（Daemon），定期同步 JournalNode 的日志，确保 HDFS 高可用模式（HA） 下 JournalNode 之间的日志数据保持一致
  private void startSyncJournalsDaemon() {
    syncJournalDaemon = new Daemon(() -> {
      // Wait for journal to be formatted to create edits.sync directory
      while(!journal.isFormatted()) {
        try {
          Thread.sleep(journalSyncInterval);
        } catch (InterruptedException e) {
          LOG.error("JournalNodeSyncer daemon received Runtime exception.", e);
          Thread.currentThread().interrupt();
          return;
        }
      }
      //如果编辑日志同步的临时目录不存在，则报错
      if (!createEditsSyncDir()) {
        LOG.error("Failed to create directory for downloading log " +
                "segments: {}. Stopping Journal Node Sync.",
            journal.getStorage().getEditsSyncDir());
        return;
      }
      while(shouldSync) {
        try {
          if (!journal.isFormatted()) {
            LOG.warn("Journal cannot sync. Not formatted.");
          } else {
            syncJournals();
          }
        } catch (Throwable t) {
          if (!shouldSync) {
            if (t instanceof InterruptedException) {
              LOG.info("Stopping JournalNode Sync.");
              Thread.currentThread().interrupt();
              return;
            } else {
              LOG.warn("JournalNodeSyncer received an exception while " +
                  "shutting down.", t);
            }
            break;
          } else {
            if (t instanceof InterruptedException) {
              LOG.warn("JournalNodeSyncer interrupted", t);
              Thread.currentThread().interrupt();
              return;
            }
          }
          LOG.error(
              "JournalNodeSyncer daemon received Runtime exception. ", t);
        }
        try {
          Thread.sleep(journalSyncInterval);
        } catch (InterruptedException e) {
          if (!shouldSync) {
            LOG.info("Stopping JournalNode Sync.");
          } else {
            LOG.warn("JournalNodeSyncer interrupted", e);
          }
          Thread.currentThread().interrupt();
          return;
        }
      }
    });
    //开始后台同步
    syncJournalDaemon.start();
  }
  //同步JournalNode节点之间的日志
  private void syncJournals() {
    //同步序号为journalNodeIndexForSync的JournalNode日志，然后递增journalNodeIndexForSync
    syncWithJournalAtIndex(journalNodeIndexForSync);
    journalNodeIndexForSync = (journalNodeIndexForSync + 1) % numOtherJNs;
  }
  //用于 将当前 JournalNode（JN）与指定的远程 JN 进行同步，确保分布式日志一致性
  private void syncWithJournalAtIndex(int index) {
    //记录当前 JN 和 otherJNProxies.get(index) 之间的同步操作，方便调试
    LOG.info("Syncing Journal " + jn.getBoundIpcAddress().getAddress() + ":"
        + jn.getBoundIpcAddress().getPort() + " with "
        + otherJNProxies.get(index) + ", journal id: " + jid);
    //获取目标 JN 代理
    final InterQJournalProtocol jnProxy = otherJNProxies.get(index).jnProxy;
    if (jnProxy == null) {
      LOG.error("JournalNode Proxy not found.");
      return;
    }
    //获取当前 JN 的 edit log manifest
    List<RemoteEditLog> thisJournalEditLogs;
    try {
      thisJournalEditLogs = journal.getEditLogManifest(0, false).getLogs();
    } catch (IOException e) {
      LOG.error("Exception in getting local edit log manifest", e);
      return;
    }
    //获取远程 JN 的 edit log manifest
    GetEditLogManifestResponseProto editLogManifest;
    try {
      editLogManifest = jnProxy.getEditLogManifestFromJournal(jid,
          nameServiceId, 0, false);
    } catch (IOException e) {
      LOG.debug("Could not sync with Journal at {}.",
          otherJNProxies.get(journalNodeIndexForSync), e);
      return;
    }
    //计算缺失日志并同步
    getMissingLogSegments(thisJournalEditLogs, editLogManifest,
        otherJNProxies.get(index));
  }
  //当 Active NameNode 发生变更（例如切换为 Standby），新的 Active NameNode 需要从 JournalNode 重新同步编辑日志，以保持一致性
  //返回 List<InetSocketAddress> 类型的对象，表示 JournalNode 地址列表
  private List<InetSocketAddress> getOtherJournalNodeAddrs() {
    String uriStr = "";
    try {
      //获取共享编辑日志的 URI
      //qjournal://host1:port1;host2:port2;host3:port3/mycluster
      uriStr = conf.getTrimmed(DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY);
      //如果未获取到 URI，尝试基于 NameServiceId 查找
      if (uriStr == null || uriStr.isEmpty()) {
        if (nameServiceId != null) {
          uriStr = conf.getTrimmed(DFSConfigKeys
              .DFS_NAMENODE_SHARED_EDITS_DIR_KEY + "." + nameServiceId);
        }
      }
      //如果仍然未找到 URI，进一步检查 NameNode IDs
      if (uriStr == null || uriStr.isEmpty()) {
        HashSet<String> sharedEditsUri = new HashSet<>();
        if (nameServiceId != null) {
          Collection<String> nnIds = DFSUtilClient.getNameNodeIds(
              conf, nameServiceId);
          for (String nnId : nnIds) {
            //通过 nameServiceId + "." + nnId 构造新的配置项
            String suffix = nameServiceId + "." + nnId;
            uriStr = conf.getTrimmed(DFSConfigKeys
                .DFS_NAMENODE_SHARED_EDITS_DIR_KEY + "." + suffix);
            sharedEditsUri.add(uriStr);
          }
          if (sharedEditsUri.size() > 1) {
            uriStr = null;
            LOG.error("The conf property " + DFSConfigKeys
                .DFS_NAMENODE_SHARED_EDITS_DIR_KEY + " not set properly, " +
                "it has been configured with different journalnode values " +
                sharedEditsUri.toString() + " for a" +
                " single nameserviceId" + nameServiceId);
          }
        }
      }
      //检查是否存在多个不同的共享编辑日志 URI
      if (uriStr == null || uriStr.isEmpty()) {
        LOG.error("Could not construct Shared Edits Uri");
        return null;
      } else {
        return getJournalAddrList(uriStr);  //如果没问题，最终在这里解析
      }

    } catch (URISyntaxException e) {
      LOG.error("The conf property " + DFSConfigKeys
          .DFS_NAMENODE_SHARED_EDITS_DIR_KEY + " not set properly.");
    } catch (IOException e) {
      LOG.error("Could not parse JournalNode addresses: " + uriStr);
    }
    return null;
  }
  //uriStr：HDFS 配置中的 JournalNode 地址 URI，例如：
  //qjournal://host1:port1;host2:port2;host3:port3/mycluster
  //解析 JournalNode 地址列表
  @VisibleForTesting
  protected List<InetSocketAddress> getJournalAddrList(String uriStr) throws
      URISyntaxException,
      IOException {
    //将 uriStr 转换为 URI 对象，以便后续提取 JournalNode 地址部分
    URI uri = new URI(uriStr);
    //获取当前 JournalNode 的绑定地址
    InetSocketAddress boundIpcAddress = jn.getBoundIpcAddress();
    Set<InetSocketAddress> excluded = Sets.newHashSet(boundIpcAddress);
    //解析 JournalNode 地址列表
    List<InetSocketAddress> addrList = Util.getLoggerAddresses(uri, excluded, conf);

    // Exclude the current JournalNode instance (a local address and the same port).  If the address
    // is bound to a local address on the same port, then remove it to handle scenarios where a
    // wildcard address (e.g. "0.0.0.0") is used.   We can't simply exclude all local addresses
    // since we may be running multiple servers on the same host.
    //过滤掉当前 JournalNode
    addrList.removeIf(addr -> !addr.isUnresolved() &&  addr.getAddress().isAnyLocalAddress()
          && boundIpcAddress.getPort() == addr.getPort());

    return addrList;
  }
  //用于 从远程 JournalNode 获取缺失的日志片段。它比较本地和远程的 edit log manifest，找出缺失的日志，然后从远程节点下载这些缺失的日志
  private void getMissingLogSegments(List<RemoteEditLog> thisJournalEditLogs,
                                     GetEditLogManifestResponseProto response,
                                     JournalNodeProxy remoteJNproxy) {
    //获取远程日志清单
    List<RemoteEditLog> otherJournalEditLogs = PBHelper.convert(
        response.getManifest()).getLogs();
    if (otherJournalEditLogs == null || otherJournalEditLogs.isEmpty()) {
      LOG.warn("Journal at " + remoteJNproxy.jnAddr + " has no edit logs");
      return;
    }
    //获取缺失的日志片段
    List<RemoteEditLog> missingLogs = getMissingLogList(thisJournalEditLogs,
        otherJournalEditLogs);

    if (!missingLogs.isEmpty()) {
      NamespaceInfo nsInfo = jnStorage.getNamespaceInfo();
      //下载缺失的日志片段
      for (RemoteEditLog missingLog : missingLogs) {
        URL url = null;
        boolean success = false;
        try {
          if (remoteJNproxy.httpServerUrl == null) {
            if (response.hasFromURL()) {
              remoteJNproxy.httpServerUrl = getHttpServerURI(
                  response.getFromURL(), remoteJNproxy.jnAddr.getHostName());
            } else {
              LOG.error("EditLogManifest response does not have fromUrl " +
                  "field set. Aborting current sync attempt");
              break;
            }
          }
          //构建下载路径并尝试下载
          String urlPath = GetJournalEditServlet.buildPath(jid, missingLog
              .getStartTxId(), nsInfo, false);
          url = new URL(remoteJNproxy.httpServerUrl, urlPath);
          success = downloadMissingLogSegment(url, missingLog);
        } catch (URISyntaxException e) {
          LOG.error("EditLogManifest's fromUrl field syntax incorrect", e);
        } catch (MalformedURLException e) {
          LOG.error("MalformedURL when download missing log segment", e);
        } catch (Exception e) {
          LOG.error("Exception in downloading missing log segment from url " +
              url, e);
        }
        if (!success) {
          LOG.error("Aborting current sync attempt.");
          break;
        }
      }
    }
  }

  /**
   *  Returns the logs present in otherJournalEditLogs and missing from
   *  thisJournalEditLogs.
   */
  private List<RemoteEditLog> getMissingLogList(
      List<RemoteEditLog> thisJournalEditLogs,
      List<RemoteEditLog> otherJournalEditLogs) {
    if (thisJournalEditLogs.isEmpty()) {
      return otherJournalEditLogs;
    }

    List<RemoteEditLog> missingEditLogs = Lists.newArrayList();

    int localJnIndex = 0, remoteJnIndex = 0;
    int localJnNumLogs = thisJournalEditLogs.size();
    int remoteJnNumLogs = otherJournalEditLogs.size();

    while (localJnIndex < localJnNumLogs && remoteJnIndex < remoteJnNumLogs) {
      long localJNstartTxId = thisJournalEditLogs.get(localJnIndex)
          .getStartTxId();
      long remoteJNstartTxId = otherJournalEditLogs.get(remoteJnIndex)
          .getStartTxId();

      if (localJNstartTxId == remoteJNstartTxId) {
        localJnIndex++;
        remoteJnIndex++;
      } else if (localJNstartTxId > remoteJNstartTxId) {
        missingEditLogs.add(otherJournalEditLogs.get(remoteJnIndex));
        remoteJnIndex++;
      } else {
        localJnIndex++;
      }
    }

    if (remoteJnIndex < remoteJnNumLogs) {
      for (; remoteJnIndex < remoteJnNumLogs; remoteJnIndex++) {
        missingEditLogs.add(otherJournalEditLogs.get(remoteJnIndex));
      }
    }

    return missingEditLogs;
  }

  private URL getHttpServerURI(String fromUrl, String hostAddr)
      throws URISyntaxException, MalformedURLException {
    URI uri = new URI(fromUrl);
    return new URL(uri.getScheme(), hostAddr, uri.getPort(), "");
  }

  /**
   * Transfer an edit log from one journal node to another for sync-up.
   */
  private boolean downloadMissingLogSegment(URL url, RemoteEditLog log)
      throws IOException {
    LOG.info("Downloading missing Edit Log from " + url + " to " + jnStorage
        .getRoot());

    assert log.getStartTxId() > 0 && log.getEndTxId() > 0 : "bad log: " + log;
    File finalEditsFile = jnStorage.getFinalizedEditsFile(log.getStartTxId(),
        log.getEndTxId());

    if (finalEditsFile.exists() && FileUtil.canRead(finalEditsFile)) {
      LOG.info("Skipping download of remote edit log " + log + " since it's" +
          " already stored locally at " + finalEditsFile);
      return true;
    }

    // Download the log segment to current.tmp directory first.
    File tmpEditsFile = jnStorage.getTemporaryEditsFile(
        log.getStartTxId(), log.getEndTxId());

    if (!SecurityUtil.doAsLoginUser(() -> {
      if (UserGroupInformation.isSecurityEnabled()) {
        UserGroupInformation.getCurrentUser().checkTGTAndReloginFromKeytab();
      }
      try {
        Util.doGetUrl(url, ImmutableList.of(tmpEditsFile), jnStorage, false,
            logSegmentTransferTimeout, throttler);
      } catch (IOException e) {
        LOG.error("Download of Edit Log file for Syncing failed. Deleting temp "
            + "file: " + tmpEditsFile, e);
        if (!tmpEditsFile.delete()) {
          LOG.warn("Deleting " + tmpEditsFile + " has failed");
        }
        return false;
      }
      return true;
    })) {
      return false;
    }
    LOG.info("Downloaded file " + tmpEditsFile.getName() + " of size " +
        tmpEditsFile.length() + " bytes.");

    boolean moveSuccess = false;
    try {
      moveSuccess = journal.moveTmpSegmentToCurrent(tmpEditsFile,
          finalEditsFile, log.getEndTxId());
    } catch (IOException e) {
      LOG.info("Could not move {} to current directory.", tmpEditsFile);
    } finally {
      if (tmpEditsFile.exists() && !tmpEditsFile.delete()) {
        LOG.warn("Deleting " + tmpEditsFile + " has failed");
      }
    }
    if (moveSuccess) {
      metrics.incrNumEditLogsSynced();
      return true;
    } else {
      return false;
    }
  }
  //获取数据的传输带宽
  private static DataTransferThrottler getThrottler(Configuration conf) {
    long transferBandwidth =
        conf.getLong(DFSConfigKeys.DFS_EDIT_LOG_TRANSFER_RATE_KEY,
            DFSConfigKeys.DFS_EDIT_LOG_TRANSFER_RATE_DEFAULT);
    DataTransferThrottler throttler = null;
    if (transferBandwidth > 0) {
      throttler = new DataTransferThrottler(transferBandwidth);
    }
    return throttler;
  }
  //代理类，用于与远程 JournalNode 进行 RPC 通信
  //封装 JournalNode 的地址 (jnAddr)，用于标识要连接的 JournalNode
  //建立 RPC 连接 (jnProxy)，允许本地节点通过 InterQJournalProtocol 协议与远程 JournalNode 进行交互
  //维护 httpServerUrl（虽然代码中未使用，但可能用于访问 JournalNode 的 Web 界面）
  private class JournalNodeProxy {
    private final InetSocketAddress jnAddr;//存储 JournalNode 的网络地址
    private final InterQJournalProtocol jnProxy;//用于 RPC 远程调用 JournalNode 的 InterQJournalProtocol 接口
    private URL httpServerUrl;

    JournalNodeProxy(InetSocketAddress jnAddr) throws IOException {
      final Configuration confCopy = new Configuration(conf);
      this.jnAddr = jnAddr;
      this.jnProxy = SecurityUtil.doAsLoginUser(
          new PrivilegedExceptionAction<InterQJournalProtocol>() {
            @Override
            public InterQJournalProtocol run() throws IOException {
              RPC.setProtocolEngine(confCopy, InterQJournalProtocolPB.class,
                  ProtobufRpcEngine2.class);
              InterQJournalProtocolPB interQJournalProtocolPB = RPC.getProxy(
                  InterQJournalProtocolPB.class,
                  RPC.getProtocolVersion(InterQJournalProtocolPB.class),
                  jnAddr, confCopy);
              return new InterQJournalProtocolTranslatorPB(
                  interQJournalProtocolPB);
            }
          });
    }

    @Override
    public String toString() {
      return jnAddr.toString();
    }
  }
}
