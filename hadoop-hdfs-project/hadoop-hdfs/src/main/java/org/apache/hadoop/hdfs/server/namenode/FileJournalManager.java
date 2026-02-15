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
package org.apache.hadoop.hdfs.server.namenode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Comparator;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.StorageErrorReporter;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.NNStorageRetentionManager.StoragePurger;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogLoader.EditLogValidation;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.util.Lists;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.ComparisonChain;

/**
 * Journal manager for the common case of edits files being written
 * to a storage directory.
 * 
 * Note: this class is not thread-safe and should be externally
 * synchronized.
 */
//普通文件的JournalManager实现类
@InterfaceAudience.Private
public class FileJournalManager implements JournalManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(FileJournalManager.class);

  private final Configuration conf;//保存了与HDFS相关的配置
  private final StorageDirectory sd;//表示存储目录，负责存储编辑日志文件（edit logs），例如edits和edits_inprogress文件
  private final StorageErrorReporter errorReporter; //用于在出现与存储相关的错误时报告错误
  private int outputBufferCapacity = 512*1024;//缓冲区大小，定义了编辑日志输出流的缓冲区容量，单位是字节，默认值为512KB

  private static final Pattern EDITS_REGEX = Pattern.compile(
    NameNodeFile.EDITS.getName() + "_(\\d+)-(\\d+)");//匹配符合edits_xxxx-yyyy格式的编辑日志文件名
  private static final Pattern EDITS_INPROGRESS_REGEX = Pattern.compile(
    NameNodeFile.EDITS_INPROGRESS.getName() + "_(\\d+)");//匹配符合edits_inprogress_xxxx格式的文件名
  private static final Pattern EDITS_INPROGRESS_STALE_REGEX = Pattern.compile(
      NameNodeFile.EDITS_INPROGRESS.getName() + "_(\\d+).*(\\S+)");//匹配符合edits_inprogress_xxxx_...格式的过时编辑日志文件

  @VisibleForTesting
  File currentInProgress = null;//当前正在进行中的日志文件。保存一个File对象，表示当前正在写入的edits_inprogress文件

  /**
   * A FileJournalManager should maintain the largest Tx ID that has been
   * safely written to its edit log files.
   * It should limit readers to read beyond this ID to avoid potential race
   * with ongoing writers.
   * Initial value indicates that all transactions can be read.
   */
  private long lastReadableTxId = Long.MAX_VALUE; //保存已安全写入的最大事务ID。其初始值是Long.MAX_VALUE，表示可以读取所有事务

  @VisibleForTesting
  StoragePurger purger
    = new NNStorageRetentionManager.DeletionStoragePurger();//负责清理过时的日志文件。通常是通过删除过期的编辑日志文件来释放空间

  //构造方法
  public FileJournalManager(Configuration conf, StorageDirectory sd,
      StorageErrorReporter errorReporter) {
    this.conf = conf;
    this.sd = sd;
    this.errorReporter = errorReporter;
  }

  @Override 
  public void close() throws IOException {}
  
  @Override
  public void format(NamespaceInfo ns, boolean force) throws IOException {
    // Formatting file journals is done by the StorageDirectory
    // format code, since they may share their directory with
    // checkpoints, etc.
    throw new UnsupportedOperationException();
  }
  
  @Override
  public boolean hasSomeData() {
    // Formatting file journals is done by the StorageDirectory
    // format code, since they may share their directory with
    // checkpoints, etc.
    throw new UnsupportedOperationException();
  }
  //开始一个新的日志段，从给定的事务ID (txid) 开始。它会创建一个输出流，写入日志段。layoutVersion用于指定存储格式的版本
  @Override
  synchronized public EditLogOutputStream startLogSegment(long txid,
      int layoutVersion) throws IOException {
    try {
      //获取一个edits_inprogress文件
      currentInProgress = NNStorage.getInProgressEditsFile(sd, txid);
      EditLogOutputStream stm = new EditLogFileOutputStream(conf,
          currentInProgress, outputBufferCapacity);
      stm.create(layoutVersion);
      return stm;
    } catch (IOException e) {
      LOG.warn("Unable to start log segment " + txid +
          " at " + currentInProgress + ": " +
          e.getLocalizedMessage());
      errorReporter.reportErrorOnFile(currentInProgress);
      throw e;
    }
  }
  //用于完成一个日志段，从firstTxId到lastTxId的日志段。将inprogress文件重命名为已完成的日志文件
  @Override
  synchronized public void finalizeLogSegment(long firstTxId, long lastTxId)
      throws IOException {
    //获取inprogress文件
    File inprogressFile = NNStorage.getInProgressEditsFile(sd, firstTxId);
    //创建目标已完成日志文件
    File dstFile = NNStorage.getFinalizedEditsFile(
        sd, firstTxId, lastTxId);
    LOG.info("Finalizing edits file " + inprogressFile + " -> " + dstFile);
    
    Preconditions.checkState(!dstFile.exists(),
        "Can't finalize edits file " + inprogressFile + " since finalized file " +
        "already exists");

    try {
      //使用NativeIO.renameTo将inprogress文件重命名为已完成的文件
      NativeIO.renameTo(inprogressFile, dstFile);
    } catch (IOException e) {
      errorReporter.reportErrorOnFile(dstFile);
      throw new IllegalStateException("Unable to finalize edits file " + inprogressFile, e);
    }

    if (inprogressFile.equals(currentInProgress)) {
      currentInProgress = null;
    }
  }

  @VisibleForTesting
  public StorageDirectory getStorageDirectory() {
    return sd;
  }

  @Override
  synchronized public void setOutputBufferCapacity(int size) {
    this.outputBufferCapacity = size;
  }


  public long getLastReadableTxId() {
    return lastReadableTxId;
  }

  public void setLastReadableTxId(long id) {
    this.lastReadableTxId = id;
  }

  /**
   * Purges the unnecessary edits and edits_inprogress files.
   *
   * Edits files that are ending before the minTxIdToKeep are purged.
   * Edits in progress files that are starting before minTxIdToKeep are purged.
   * Edits in progress files that are marked as empty, trash, corrupted or
   * stale by file extension and starting before minTxIdToKeep are purged.
   * Edits in progress files that are after minTxIdToKeep, but before the
   * current edits in progress files are marked as stale for clarity.
   *
   * In case file removal or rename is failing a warning is logged, but that
   * does not fail the operation.
   *
   * @param minTxIdToKeep the lowest transaction ID that should be retained
   * @throws IOException if listing the storage directory fails.
   */
  //清理比minTxIdToKeep更老的日志文件。删除不再需要的edits文件和edits_inprogress文件
  @Override
  public void purgeLogsOlderThan(long minTxIdToKeep)
      throws IOException {
    LOG.info("Purging logs older than " + minTxIdToKeep);
    //列出存储目录中的所有文件
    File[] files = FileUtil.listFiles(sd.getCurrentDir());
    //匹配编辑日志文件
    List<EditLogFile> editLogs = matchEditLogs(files, true);
    synchronized (this) {
      for (EditLogFile log : editLogs) {//根据文件的事务ID，决定是删除文件还是标记为过时
        if (log.getFirstTxId() < minTxIdToKeep &&
            log.getLastTxId() < minTxIdToKeep) {
          purger.purgeLog(log);
        } else if (isStaleInProgressLog(minTxIdToKeep, log)) {
          purger.markStale(log);
        }
      }
    }
  }

  private boolean isStaleInProgressLog(long minTxIdToKeep, EditLogFile log) {
    return log.isInProgress() &&
        !log.getFile().equals(currentInProgress) &&
        log.getFirstTxId() >= minTxIdToKeep &&
        // at last we check if this segment is not already marked as .trash,
        // .empty or .corrupted, in which case it does not match the strict
        // regex pattern.
        EDITS_INPROGRESS_REGEX.matcher(log.getFile().getName()).matches();
  }

  /**
   * Find all editlog segments starting at or above the given txid.
   * @param firstTxId the txnid which to start looking
   * @param inProgressOk whether or not to include the in-progress edit log 
   *        segment       
   * @return a list of remote edit logs
   * @throws IOException if edit logs cannot be listed.
   */
  public List<RemoteEditLog> getRemoteEditLogs(long firstTxId,
      boolean inProgressOk) throws IOException {
    File currentDir = sd.getCurrentDir();
    List<EditLogFile> allLogFiles = matchEditLogs(currentDir);
    List<RemoteEditLog> ret = Lists.newArrayListWithCapacity(
        allLogFiles.size());
    for (EditLogFile elf : allLogFiles) {
      if (elf.hasCorruptHeader() || (!inProgressOk && elf.isInProgress())) {
        continue;
      }
      if (elf.isInProgress()) {
        try {
          elf.scanLog(getLastReadableTxId(), true);
        } catch (IOException e) {
          LOG.error("got IOException while trying to validate header of " +
              elf + ".  Skipping.", e);
          continue;
        }
      }
      if (elf.getFirstTxId() >= firstTxId) {
        ret.add(new RemoteEditLog(elf.firstTxId, elf.lastTxId,
            elf.isInProgress()));
      } else if (elf.getFirstTxId() < firstTxId && firstTxId <= elf.getLastTxId()) {
        // If the firstTxId is in the middle of an edit log segment. Return this
        // anyway and let the caller figure out whether it wants to use it.
        ret.add(new RemoteEditLog(elf.firstTxId, elf.lastTxId,
            elf.isInProgress()));
      }
    }
    
    Collections.sort(ret);
    
    return ret;
  }
  
  /**
   * Discard all editlog segments whose first txid is greater than or equal to
   * the given txid, by renaming them with suffix ".trash".
   */
  private void discardEditLogSegments(long startTxId) throws IOException {
    File currentDir = sd.getCurrentDir();
    List<EditLogFile> allLogFiles = matchEditLogs(currentDir);
    List<EditLogFile> toTrash = Lists.newArrayList();
    LOG.info("Discard the EditLog files, the given start txid is " + startTxId);
    // go through the editlog files to make sure the startTxId is right at the
    // segment boundary
    for (EditLogFile elf : allLogFiles) {
      if (elf.getFirstTxId() >= startTxId) {
        toTrash.add(elf);
      } else {
        Preconditions.checkState(elf.getLastTxId() < startTxId);
      }
    }

    for (EditLogFile elf : toTrash) {
      // rename these editlog file as .trash
      elf.moveAsideTrashFile(startTxId);
      LOG.info("Trash the EditLog file " + elf);
    }
  }

  /**
   * returns matching edit logs via the log directory. Simple helper function
   * that lists the files in the logDir and calls matchEditLogs(File[])
   * 
   * @param logDir
   *          directory to match edit logs in
   * @return matched edit logs
   * @throws IOException
   *           IOException thrown for invalid logDir
   */
  public static List<EditLogFile> matchEditLogs(File logDir) throws IOException {
    return matchEditLogs(FileUtil.listFiles(logDir));
  }

  static List<EditLogFile> matchEditLogs(File[] filesInStorage) {
    return matchEditLogs(filesInStorage, false);
  }
  //匹配符合edits_xxxx-yyyy格式的编辑日志文件名
  private static List<EditLogFile> matchEditLogs(File[] filesInStorage,
      boolean forPurging) {
    List<EditLogFile> ret = Lists.newArrayList();
    for (File f : filesInStorage) {
      String name = f.getName();
      // Check for edits
      //匹配符合edits_xxxx-yyyy格式的编辑日志文件名
      Matcher editsMatch = EDITS_REGEX.matcher(name);
      if (editsMatch.matches()) {
        try {
          long startTxId = Long.parseLong(editsMatch.group(1));
          long endTxId = Long.parseLong(editsMatch.group(2));
          ret.add(new EditLogFile(f, startTxId, endTxId));
          continue;
        } catch (NumberFormatException nfe) {
          LOG.error("Edits file " + f + " has improperly formatted " +
                    "transaction ID");
          // skip
        }
      }
      
      // Check for in-progress edits
      Matcher inProgressEditsMatch = EDITS_INPROGRESS_REGEX.matcher(name);
      if (inProgressEditsMatch.matches()) {
        try {
          long startTxId = Long.parseLong(inProgressEditsMatch.group(1));
          ret.add(
              new EditLogFile(f, startTxId, HdfsServerConstants.INVALID_TXID, true));
          continue;
        } catch (NumberFormatException nfe) {
          LOG.error("In-progress edits file " + f + " has improperly " +
                    "formatted transaction ID");
          // skip
        }
      }
      if (forPurging) {
        // Check for in-progress stale edits
        Matcher staleInprogressEditsMatch = EDITS_INPROGRESS_STALE_REGEX
            .matcher(name);
        if (staleInprogressEditsMatch.matches()) {
          try {
            long startTxId = Long.parseLong(staleInprogressEditsMatch.group(1));
            ret.add(new EditLogFile(f, startTxId, HdfsServerConstants.INVALID_TXID,
                true));
            continue;
          } catch (NumberFormatException nfe) {
            LOG.error("In-progress stale edits file " + f + " has improperly "
                + "formatted transaction ID");
            // skip
          }
        }
      }
    }
    return ret;
  }

  synchronized public void selectInputStreams(
      Collection<EditLogInputStream> streams,
      long fromTxnId, boolean inProgressOk) throws IOException {
    selectInputStreams(streams, fromTxnId, inProgressOk, false);
  }

  @Override
  synchronized public void selectInputStreams(
      Collection<EditLogInputStream> streams, long fromTxId,
      boolean inProgressOk, boolean onlyDurableTxns)
      throws IOException {
    List<EditLogFile> elfs = matchEditLogs(sd.getCurrentDir());
    if (LOG.isDebugEnabled()) {
      LOG.debug(this + ": selecting input streams starting at " + fromTxId +
          (inProgressOk ? " (inProgress ok) " : " (excluding inProgress) ") +
          "from among " + elfs.size() + " candidate file(s)");
    }
    addStreamsToCollectionFromFiles(elfs, streams, fromTxId,
        getLastReadableTxId(), inProgressOk);
  }
  
  static void addStreamsToCollectionFromFiles(Collection<EditLogFile> elfs,
      Collection<EditLogInputStream> streams, long fromTxId,
      long maxTxIdToScan, boolean inProgressOk) {
    for (EditLogFile elf : elfs) {
      if (elf.isInProgress()) {
        if (!inProgressOk) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("passing over " + elf + " because it is in progress " +
                "and we are ignoring in-progress logs.");
          }
          continue;
        }
        try {
          elf.scanLog(maxTxIdToScan, true);
        } catch (IOException e) {
          LOG.error("got IOException while trying to validate header of " +
              elf + ".  Skipping.", e);
          continue;
        }
      }
      if (elf.lastTxId < fromTxId) {
        assert elf.lastTxId != HdfsServerConstants.INVALID_TXID;
        if (LOG.isDebugEnabled()) {
          LOG.debug("passing over " + elf + " because it ends at " +
              elf.lastTxId + ", but we only care about transactions " +
              "as new as " + fromTxId);
        }
        continue;
      }
      EditLogFileInputStream elfis = new EditLogFileInputStream(elf.getFile(),
            elf.getFirstTxId(), elf.getLastTxId(), elf.isInProgress());
      LOG.debug("selecting edit log stream " + elf);
      streams.add(elfis);
    }
  }

  @Override
  synchronized public void recoverUnfinalizedSegments() throws IOException {
    File currentDir = sd.getCurrentDir();
    LOG.info("Recovering unfinalized segments in " + currentDir);
    List<EditLogFile> allLogFiles = matchEditLogs(currentDir);

    for (EditLogFile elf : allLogFiles) {
      if (elf.getFile().equals(currentInProgress)) {
        continue;
      }
      if (elf.isInProgress()) {
        // If the file is zero-length, we likely just crashed after opening the
        // file, but before writing anything to it. Safe to delete it.
        if (elf.getFile().length() == 0) {
          LOG.info("Deleting zero-length edit log file " + elf);
          if (!elf.getFile().delete()) {
            throw new IOException("Unable to delete file " + elf.getFile());
          }
          continue;
        }

        elf.scanLog(getLastReadableTxId(), true);

        if (elf.hasCorruptHeader()) {
          elf.moveAsideCorruptFile();
          throw new CorruptionException("In-progress edit log file is corrupt: "
              + elf);
        }
        if (elf.getLastTxId() == HdfsServerConstants.INVALID_TXID) {
          // If the file has a valid header (isn't corrupt) but contains no
          // transactions, we likely just crashed after opening the file and
          // writing the header, but before syncing any transactions. Safe to
          // delete the file.
          LOG.info("Moving aside edit log file that seems to have zero " +
              "transactions " + elf);
          elf.moveAsideEmptyFile();
          continue;
        }
        finalizeLogSegment(elf.getFirstTxId(), elf.getLastTxId());
      }
    }
  }
  //匹配符合edits_xxxx-yyyy格式的编辑日志文件名
  public List<EditLogFile> getLogFiles(long fromTxId) throws IOException {
    File currentDir = sd.getCurrentDir();
    List<EditLogFile> allLogFiles = matchEditLogs(currentDir);
    List<EditLogFile> logFiles = Lists.newArrayList();
    
    for (EditLogFile elf : allLogFiles) {
      if (fromTxId <= elf.getFirstTxId() ||
          elf.containsTxId(fromTxId)) {
        logFiles.add(elf);
      }
    }
    
    Collections.sort(logFiles, EditLogFile.COMPARE_BY_START_TXID);

    return logFiles;
  }
  
  public EditLogFile getLogFile(long startTxId) throws IOException {
    return getLogFile(sd.getCurrentDir(), startTxId, true);
  }

  public EditLogFile getLogFile(long startTxId, boolean inProgressOk)
      throws IOException {
    return getLogFile(sd.getCurrentDir(), startTxId, inProgressOk);
  }

  public static EditLogFile getLogFile(File dir, long startTxId)
      throws IOException {
    return getLogFile(dir, startTxId, true);
  }

  public static EditLogFile getLogFile(File dir, long startTxId,
      boolean inProgressOk) throws IOException {
    List<EditLogFile> files = matchEditLogs(dir);
    List<EditLogFile> ret = Lists.newLinkedList();
    for (EditLogFile elf : files) {
      if (elf.getFirstTxId() == startTxId) {
        if (inProgressOk || !elf.isInProgress()) {
          ret.add(elf);
        }
      }
    }
    
    if (ret.isEmpty()) {
      // no matches
      return null;
    } else if (ret.size() == 1) {
      return ret.get(0);
    } else {
      throw new IllegalStateException("More than one log segment in " + 
          dir + " starting at txid " + startTxId + ": " +
          Joiner.on(", ").join(ret));
    }
  }

  @Override
  public String toString() {
    return String.format("FileJournalManager(root=%s)", sd.getRoot());
  }

  /** 表示 HDFS 中的编辑日志文件（Edit Log）。HDFS 使用编辑日志来记录对文件系统的修改操作（如创建文件、删除文件等）。
   * 这个类封装了单个编辑日志文件的元数据信息，包括文件路径、事务 ID 范围、是否处于写入状态（in-progress）等
   * Record of an edit log that has been located and had its filename parsed.
   */
  @InterfaceAudience.Private
  public static class EditLogFile {
    //记录编辑日志的文件对象，指向具体的 Edit Log 文件路径
    private File file;
    //该日志文件包含的第一个事务 ID（Transaction ID），用于标识事务的起始编号
    private final long firstTxId;
    //该日志文件包含的最后一个事务 ID，标识事务的结束编号。
    private long lastTxId;
    //标志此日志文件的头部是否损坏，默认为 false。
    private boolean hasCorruptHeader = false;
    //标志此日志文件是否是一个正在写入的日志（in-progress 文件）
    private final boolean isInProgress;

    final static Comparator<EditLogFile> COMPARE_BY_START_TXID 
      = new Comparator<EditLogFile>() {
      @Override
      public int compare(EditLogFile a, EditLogFile b) {
        return ComparisonChain.start()
        .compare(a.getFirstTxId(), b.getFirstTxId())
        .compare(a.getLastTxId(), b.getLastTxId())
        .result();
      }
    };

    EditLogFile(File file,
        long firstTxId, long lastTxId) {
      this(file, firstTxId, lastTxId, false);
      assert (lastTxId != HdfsServerConstants.INVALID_TXID)
        && (lastTxId >= firstTxId);
    }
    
    EditLogFile(File file, long firstTxId, 
                long lastTxId, boolean isInProgress) { 
      assert (lastTxId == HdfsServerConstants.INVALID_TXID && isInProgress)
        || (lastTxId != HdfsServerConstants.INVALID_TXID && lastTxId >= firstTxId);
      assert (firstTxId > 0) || (firstTxId == HdfsServerConstants.INVALID_TXID);
      assert file != null;
      
      Preconditions.checkArgument(!isInProgress ||
          lastTxId == HdfsServerConstants.INVALID_TXID);
      
      this.firstTxId = firstTxId;
      this.lastTxId = lastTxId;
      this.file = file;
      this.isInProgress = isInProgress;
    }
    
    public long getFirstTxId() {
      return firstTxId;
    }
    
    public long getLastTxId() {
      return lastTxId;
    }
    
    boolean containsTxId(long txId) {
      return firstTxId <= txId && txId <= lastTxId;
    }

    /** 
     * Find out where the edit log ends.
     * This will update the lastTxId of the EditLogFile or
     * mark it as corrupt if it is.
     * @param maxTxIdToScan Maximum Tx ID to try to scan.
     *                      The scan returns after reading this or a higher ID.
     *                      The file portion beyond this ID is potentially being
     *                      updated.
     * @param verifyVersion Whether the scan should verify the layout version
     */
    //maxTxIdToScan：扫描的最大事务 ID，超过该 ID 便停止扫描
    public void scanLog(long maxTxIdToScan, boolean verifyVersion)
        throws IOException {
      EditLogValidation val = EditLogFileInputStream.scanEditLog(file,
          maxTxIdToScan, verifyVersion);
      this.lastTxId = val.getEndTxId();
      this.hasCorruptHeader = val.hasCorruptHeader();
    }

    public boolean isInProgress() {
      return isInProgress;
    }

    public File getFile() {
      return file;
    }
    
    boolean hasCorruptHeader() {
      return hasCorruptHeader;
    }

    void moveAsideCorruptFile() throws IOException {
      assert hasCorruptHeader;
      renameSelf(".corrupt");
    }

    void moveAsideTrashFile(long markerTxid) throws IOException {
      assert this.getFirstTxId() >= markerTxid;
      renameSelf(".trash");
    }

    public void moveAsideEmptyFile() throws IOException {
      assert lastTxId == HdfsServerConstants.INVALID_TXID;
      renameSelf(".empty");
    }

    public void moveAsideStaleInprogressFile() throws IOException {
      assert isInProgress;
      renameSelf(".stale");
    }

    private void renameSelf(String newSuffix) throws IOException {
      File src = file;
      File dst = new File(src.getParent(), src.getName() + newSuffix);
      // renameTo fails on Windows if the destination file already exists.
      try {
        if (dst.exists()) {
          if (!dst.delete()) {
            throw new IOException("Couldn't delete " + dst);
          }
        }
        NativeIO.renameTo(src, dst);
      } catch (IOException e) {
        throw new IOException(
            "Couldn't rename log " + src + " to " + dst, e);
      }
      file = dst;
    }

    @Override
    public String toString() {
      return String.format("EditLogFile(file=%s,first=%019d,last=%019d,"
                           +"inProgress=%b,hasCorruptHeader=%b)",
                           file.toString(), firstTxId, lastTxId,
                           isInProgress(), hasCorruptHeader);
    }
  }
  
  @Override
  public void doPreUpgrade() throws IOException {
    LOG.info("Starting upgrade of edits directory " + sd.getRoot());
    try {
     NNUpgradeUtil.doPreUpgrade(conf, sd);
    } catch (IOException ioe) {
     LOG.error("Failed to move aside pre-upgrade storage " +
         "in image directory " + sd.getRoot(), ioe);
     throw ioe;
    }
  }
  
  /**
   * This method assumes that the fields of the {@link Storage} object have
   * already been updated to the appropriate new values for the upgrade.
   */
  @Override
  public void doUpgrade(Storage storage) throws IOException {
    NNUpgradeUtil.doUpgrade(sd, storage);
  }
  
  @Override
  public void doFinalize() throws IOException {
    NNUpgradeUtil.doFinalize(sd);
  }

  @Override
  public boolean canRollBack(StorageInfo storage, StorageInfo prevStorage,
      int targetLayoutVersion) throws IOException {
    return NNUpgradeUtil.canRollBack(sd, storage,
        prevStorage, targetLayoutVersion);
  }

  @Override
  public void doRollback() throws IOException {
    NNUpgradeUtil.doRollBack(sd);
  }

  @Override
  public void discardSegments(long startTxid) throws IOException {
    discardEditLogSegments(startTxid);
  }

  @Override
  public long getJournalCTime() throws IOException {
    StorageInfo sInfo = new StorageInfo((NodeType)null);
    sInfo.readProperties(sd);
    return sInfo.getCTime();
  }
}
