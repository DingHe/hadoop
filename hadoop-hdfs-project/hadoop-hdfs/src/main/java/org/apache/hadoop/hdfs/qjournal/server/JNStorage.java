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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.common.IncorrectVersionException;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageErrorReporter;
import org.apache.hadoop.hdfs.server.namenode.FileJournalManager;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;

/**
 * A {@link Storage} implementation for the {@link JournalNode}.
 * 
 * The JN has a storage directory for each namespace for which it stores
 * metadata. There is only a single directory per JN in the current design.
 */
//JNStorage 类是 Hadoop HDFS 中 JournalNode（JN）的存储实现类，继承自 Storage 类，主要用于管理 JournalNode 节点上的元数据和事务日志的存储。JournalNode 是 HDFS 高可用 (HA) 架构下实现 Quorum Journal Manager (QJM) 的核心组件，负责在主备 NameNode 之间提供事务日志的共享存储。
//主要功能包括：
//管理 JournalNode 节点上的事务日志目录和 Paxos 文件
//提供日志的写入、读取、清理和恢复操作
//维护节点的存储状态（如已格式化、未格式化、异常等）
//支持事务日志的下载与恢复
//执行日志的清理操作，删除过期的事务日志
class JNStorage extends Storage {
  //管理事务日志的核心类，负责日志的写入、读取、回滚和清理等操作
  private final FileJournalManager fjm;
  //封装了 JournalNode 的存储目录信息，提供文件路径管理、目录分析、恢复、格式化等操作
  private final StorageDirectory sd;
  //记录当前 JournalNode 的存储状态
  private StorageState state;
  //用于匹配 Paxos 文件的正则表达式，主要用于清理过期的 Paxos 文件
  private static final List<Pattern> PAXOS_DIR_PURGE_REGEXES =
      ImmutableList.of(Pattern.compile("(\\d+)"));
  //常量 "edits.sync"，指向用于临时存储通过 JournalNodeSyncer 下载的事务日志的目录名
  private static final String STORAGE_EDITS_SYNC = "edits.sync";

  /**
   * @param conf Configuration object
   * @param logDir the path to the directory in which data will be stored
   * @param errorReporter a callback to report errors
   * @throws IOException 
   */
  protected JNStorage(Configuration conf, File logDir, StartupOption startOpt,
      StorageErrorReporter errorReporter) throws IOException {
    super(NodeType.JOURNAL_NODE);
    
    sd = new StorageDirectory(logDir, null, false, new FsPermission(conf.get(
        DFSConfigKeys.DFS_JOURNAL_EDITS_DIR_PERMISSION_KEY,
        DFSConfigKeys.DFS_JOURNAL_EDITS_DIR_PERMISSION_DEFAULT)));
    this.addStorageDir(sd);
    this.fjm = new FileJournalManager(conf, sd, errorReporter);

    analyzeAndRecoverStorage(startOpt);
  }
  
  FileJournalManager getJournalManager() {
    return fjm;
  }

  @Override
  public boolean isPreUpgradableLayout(StorageDirectory sd)
      throws IOException {
    return false;
  }

  /**
   * Find an edits file spanning the given transaction ID range.
   * If no such file exists, an exception is thrown.
   */
  //查找已完成的事务日志文件
  File findFinalizedEditsFile(long startTxId, long endTxId)
      throws IOException {
    File ret = new File(sd.getCurrentDir(),
        NNStorage.getFinalizedEditsFileName(startTxId, endTxId));
    if (!ret.exists()) {
      throw new IOException(
          "No edits file for range " + startTxId + "-" + endTxId);
    }
    return ret;
  }

  /**
   * @return the path for an in-progress edits file starting at the given
   * transaction ID. This does not verify existence of the file. 
   */
  //获取正在进行的事务日志文件
  File getInProgressEditLog(long startTxId) {
    return new File(sd.getCurrentDir(),
        NNStorage.getInProgressEditsFileName(startTxId));
  }
  
  /**
   * @param segmentTxId the first txid of the segment
   * @param epoch the epoch number of the writer which is coordinating
   * recovery
   * @return the temporary path in which an edits log should be stored
   * while it is being downloaded from a remote JournalNode
   */
  //用于获取某个事务日志段在从远程 JournalNode 下载时的临时存储路径。
  // 这个路径在数据同步过程中使用，确保日志在正式保存到 current 目录之前有一个暂存位置
  File getSyncLogTemporaryFile(long segmentTxId, long epoch) {
    String name = NNStorage.getInProgressEditsFileName(segmentTxId) +
        ".epoch=" + epoch; 
    return new File(sd.getCurrentDir(), name);
  }

  /**
   * Directory {@code edits.sync} temporarily holds the log segments
   * downloaded through {@link JournalNodeSyncer} before they are moved to
   * {@code current} directory.
   *
   * @return the directory path
   */
  File getEditsSyncDir() {
    return new File(sd.getRoot(), STORAGE_EDITS_SYNC);
  }

  File getTemporaryEditsFile(long startTxId, long endTxId) {
    return new File(getEditsSyncDir(), String.format("%s_%019d-%019d",
            NNStorage.NameNodeFile.EDITS.getName(), startTxId, endTxId));
  }

  File getFinalizedEditsFile(long startTxId, long endTxId) {
    return new File(sd.getCurrentDir(), String.format("%s_%019d-%019d",
            NNStorage.NameNodeFile.EDITS.getName(), startTxId, endTxId));
  }

  /**
   * @return the path for the file which contains persisted data for the
   * paxos-like recovery process for the given log segment.
   */
  //用于获取与指定事务 ID (segmentTxId) 关联的 Paxos 协议恢复文件路径。
  // 此文件在 JournalNode 处理事务日志时，辅助 Paxos 协议执行恢复操作，确保多个节点之间的一致性和协调性
  File getPaxosFile(long segmentTxId) {
    return new File(getOrCreatePaxosDir(), String.valueOf(segmentTxId));
  }
  
  File getOrCreatePaxosDir() {
    File paxosDir = new File(sd.getCurrentDir(), "paxos");
    if(!paxosDir.exists()) {
      LOG.info("Creating paxos dir: {}", paxosDir.toPath());
      if(!paxosDir.mkdir()) {
        LOG.error("Could not create paxos dir: {}", paxosDir.toPath());
      }
    }
    return paxosDir;
  }
  
  File getRoot() {
    return sd.getRoot();
  }
  
  /**
   * Remove any log files and associated paxos files which are older than
   * the given txid.
   */
  void purgeDataOlderThan(long minTxIdToKeep) throws IOException {
    fjm.purgeLogsOlderThan(minTxIdToKeep);

    purgeMatching(getOrCreatePaxosDir(),
        PAXOS_DIR_PURGE_REGEXES, minTxIdToKeep);
  }
  
  /**
   * Purge files in the given directory which match any of the set of patterns.
   * The patterns must have a single numeric capture group which determines
   * the associated transaction ID of the file. Only those files for which
   * the transaction ID is less than the <code>minTxIdToKeep</code> parameter
   * are removed.
   */
  //用于删除指定目录下符合给定正则表达式模式且事务 ID 小于 minTxIdToKeep 的文件
  //主要用于清理过期或无用的事务日志和 Paxos 文件，防止日志文件无限增长占用存储空间
  private static void purgeMatching(File dir, List<Pattern> patterns,
      long minTxIdToKeep) throws IOException {

    for (File f : FileUtil.listFiles(dir)) {
      if (!f.isFile()) continue;
      
      for (Pattern p : patterns) {
        Matcher matcher = p.matcher(f.getName());
        if (matcher.matches()) {
          // This parsing will always succeed since the group(1) is
          // /\d+/ in the regex itself.
          long txid = Long.parseLong(matcher.group(1));
          if (txid < minTxIdToKeep) {
            LOG.info("Purging no-longer needed file {}", txid);
            if (!f.delete()) {
              LOG.warn("Unable to delete no-longer-needed data {}", f);
            }
            break;
          }
        }
      }
    }
  }

  void format(NamespaceInfo nsInfo, boolean force) throws IOException {
    unlockAll();
    try {
      sd.analyzeStorage(StartupOption.FORMAT, this, !force);
    } finally {
      sd.unlock();
    }
    setStorageInfo(nsInfo);

    LOG.info("Formatting journal {} with nsid: {}", sd, getNamespaceID());
    // Unlock the directory before formatting, because we will
    // re-analyze it after format(). The analyzeStorage() call
    // below is reponsible for re-locking it. This is a no-op
    // if the storage is not currently locked.
    unlockAll();
    sd.clearDirectory();
    writeProperties(sd);
    getOrCreatePaxosDir();
    analyzeStorage();
  }
  
  void analyzeStorage() throws IOException {
    this.state = sd.analyzeStorage(StartupOption.REGULAR, this);
    refreshStorage();
  }

  void refreshStorage() throws IOException {
    if (state == StorageState.NORMAL) {
      readProperties(sd);
    }
  }

  @Override
  protected void setLayoutVersion(Properties props, StorageDirectory sd)
      throws IncorrectVersionException, InconsistentFSStateException {
    int lv = Integer.parseInt(getProperty(props, sd, "layoutVersion"));
    // For journal node, since it now does not decode but just scan through the
    // edits, it can handle edits with future version in most of the cases.
    // Thus currently we may skip the layoutVersion check here.
    layoutVersion = lv;
  }

  void analyzeAndRecoverStorage(StartupOption startOpt) throws IOException {
    this.state = sd.analyzeStorage(startOpt, this);
    final boolean needRecover = state != StorageState.NORMAL
        && state != StorageState.NON_EXISTENT
        && state != StorageState.NOT_FORMATTED;
    if (state == StorageState.NORMAL && startOpt != StartupOption.ROLLBACK) {
      readProperties(sd);
    } else if (needRecover) {
      sd.doRecover(state);
    }
  }

  void checkConsistentNamespace(NamespaceInfo nsInfo)
      throws IOException {
    if (nsInfo.getNamespaceID() != getNamespaceID()) {
      throw new IOException("Incompatible namespaceID for journal " +
          this.sd + ": NameNode has nsId " + nsInfo.getNamespaceID() +
          " but storage has nsId " + getNamespaceID());
    }
    
    if (!nsInfo.getClusterID().equals(getClusterID())) {
      throw new IOException("Incompatible clusterID for journal " +
          this.sd + ": NameNode has clusterId '" + nsInfo.getClusterID() +
          "' but storage has clusterId '" + getClusterID() + "'");
      
    }
  }

  public void close() throws IOException {
    LOG.info("Closing journal storage for {}", sd);
    unlockAll();
  }

  public boolean isFormatted() {
    return state == StorageState.NORMAL;
  }
}
