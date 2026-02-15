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
package org.apache.hadoop.hdfs.server.common;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.io.nativeio.NativeIOException;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.VersionInfo;

import org.apache.hadoop.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Storage information file.
 * <p>
 * Local storage information is stored in a separate file VERSION.
 * It contains type of the node, 
 * the storage layout version, the namespace id, and 
 * the fs state creation time.
 * <p>
 * Local storage can reside in multiple directories. 
 * Each directory should contain the same VERSION file as the others.
 * During startup Hadoop servers (name-node and data-nodes) read their local 
 * storage information from them.
 * <p>
 * The servers hold a lock for each storage directory while they run so that 
 * other nodes were not able to startup sharing the same storage.
 * The locks are released when the servers stop (normally or abnormally).
 * 
 */
@InterfaceAudience.Private
public abstract class Storage extends StorageInfo {

  public static final Logger LOG = LoggerFactory
      .getLogger(Storage.class.getName());

  // last layout version that did not support upgrades
  //这是不支持升级的最后一个布局版本，意味着该版本之前的 HDFS 不能直接升级到更新版本
  public static final int LAST_PRE_UPGRADE_LAYOUT_VERSION = -3;
  
  // this corresponds to Hadoop-0.18
  //这是支持升级的最后一个布局版本，意味着从该版本及之后的布局可以直接升级到更高版本
  public static final int LAST_UPGRADABLE_LAYOUT_VERSION = -16;
  protected static final String LAST_UPGRADABLE_HADOOP_VERSION = "Hadoop-0.18";
  
  /** Layout versions of 0.20.203 release */
  public static final int[] LAYOUT_VERSIONS_203 = {-19, -31};

  public    static final String STORAGE_FILE_LOCK     = "in_use.lock";//HDFS 节点在启动时会在存储目录中创建此锁文件，防止其他节点同时使用相同的存储目录
  public    static final String STORAGE_DIR_CURRENT   = "current";//存储当前文件系统状态的目录，包含元数据文件（如 VERSION 文件、fsimage、edits 等）
  public    static final String STORAGE_DIR_PREVIOUS  = "previous";//在 HDFS 升级期间用于备份旧版本数据的目录，升级成功后此目录仍然保留，供回滚使用
  public    static final String STORAGE_TMP_REMOVED   = "removed.tmp";//临时目录，升级或回滚过程中被移除的数据会存放在此目录中
  public    static final String STORAGE_TMP_PREVIOUS  = "previous.tmp";//临时目录，表示升级过程中将 previous 目录重命名为 previous.tmp，防止升级失败时数据损坏
  public    static final String STORAGE_TMP_FINALIZED = "finalized.tmp";//临时目录，升级完成后，临时数据会被移动到此目录，最终用于确认升级已完成
  public    static final String STORAGE_TMP_LAST_CKPT = "lastcheckpoint.tmp";//临时目录，存放最后一次检查点的临时数据，通常用于 NameNode 的 fsimage 保存
  public    static final String STORAGE_PREVIOUS_CKPT = "previous.checkpoint";//保存上一次检查点数据的目录，主要用于在升级期间进行回滚或恢复
  
  /**
   * The blocksBeingWritten directory which was used in some 1.x and earlier
   * releases.
   */
  public static final String STORAGE_1_BBW = "blocksBeingWritten";
  
  public enum StorageState {
    NON_EXISTENT, //存储目录不存在
    NOT_FORMATTED, //存储目录未格式化
    COMPLETE_UPGRADE, //升级完成状态，表示存储目录已成功完成升级操作
    RECOVER_UPGRADE, //恢复升级状态，表示升级过程中发生异常，需要恢复到升级前的状态
    COMPLETE_FINALIZE,//完成升级终结状态，表示 HDFS 升级已最终确认，不再保留回滚数据
    COMPLETE_ROLLBACK,//回滚完成状态，表示已成功将 HDFS 恢复到升级前的状态
    RECOVER_ROLLBACK, //恢复回滚状态，表示回滚过程中发生异常，需要进行恢复操作
    COMPLETE_CHECKPOINT,//检查点完成状态，表示 HDFS 已成功完成 Checkpoint 操作
    RECOVER_CHECKPOINT,//恢复检查点状态，表示 Checkpoint 操作失败，需要恢复到之前的状态
    NORMAL;//正常状态，表示存储目录处于正常工作状态，没有异常或正在进行的升级、回滚等操作
  }
  
  /**
   * An interface to denote storage directory type
   * Implementations can define a type for storage directory by implementing
   * this interface.
   */
  @InterfaceAudience.Private
  public interface StorageDirType {
    public StorageDirType getStorageDirType();
    public boolean isOfType(StorageDirType type);
  }
  //存储对应的目录列表
  private final List<StorageDirectory> storageDirs =
      new CopyOnWriteArrayList<>();

  //用于遍历 HDFS 存储目录（StorageDirectory）的内部类，实现了 Java 标准的 Iterator 接口，可以按需过滤特定类型的存储目录
  private class DirIterator implements Iterator<StorageDirectory> {
    final StorageDirType dirType;//目标存储目录的类型，类型为 StorageDirType（例如 IMAGE, EDITS, IMAGE_AND_EDITS）
    final boolean includeShared;//是否包括共享目录，布尔类型
    int prevIndex; // for remove()  上一个已返回的目录索引，主要用于 remove() 方法删除当前目录时恢复迭代状态
    int nextIndex; // for next() 下一个即将返回的目录索引，控制遍历的进度
    
    DirIterator(StorageDirType dirType, boolean includeShared) {
      this.dirType = dirType;
      this.nextIndex = 0;
      this.prevIndex = 0;
      this.includeShared = includeShared;
    }
    
    @Override
    public boolean hasNext() {
      //如果 storageDirs 为空，或 nextIndex 超过列表长度，直接返回 false
      if (storageDirs.isEmpty() || nextIndex >= storageDirs.size())
        return false;
      if (dirType != null || !includeShared) {
        while (nextIndex < storageDirs.size()) {
          if (shouldReturnNextDir())
            break;
          nextIndex++;
        }
        if (nextIndex >= storageDirs.size())
         return false;
      }
      return true;
    }
    
    @Override
    public StorageDirectory next() {
      StorageDirectory sd = getStorageDir(nextIndex);
      prevIndex = nextIndex;
      nextIndex++;
      if (dirType != null || !includeShared) {
        while (nextIndex < storageDirs.size()) {
          if (shouldReturnNextDir())
            break;
          nextIndex++;
        }
      }
      return sd;
    }
    
    @Override
    public void remove() {
      nextIndex = prevIndex; // restore previous state
      storageDirs.remove(prevIndex); // remove last returned element
      hasNext(); // reset nextIndex to correct place
    }
    
    private boolean shouldReturnNextDir() {
      StorageDirectory sd = getStorageDir(nextIndex);
      return (dirType == null || sd.getStorageDirType().isOfType(dirType)) &&
          (includeShared || !sd.isShared());
    }
  }
  
  /**
   * @return A list of the given File in every available storage directory,
   * regardless of whether it might exist.
   */
  //根据给定的目录类型 (dirType) 和文件名 (fileName)，
  // 在所有匹配的存储目录中生成对应文件路径列表。即使目标文件不存在，方法也会返回路径列表
  public List<File> getFiles(StorageDirType dirType, String fileName) {
    ArrayList<File> list = new ArrayList<File>();
    Iterator<StorageDirectory> it =
      (dirType == null) ? dirIterator() : dirIterator(dirType);
    for ( ;it.hasNext(); ) {
      File currentDir = it.next().getCurrentDir();
      if (currentDir != null) {
        list.add(new File(currentDir, fileName));
      }
    }
    return list;
  }


  /**
   * Return default iterator
   * This iterator returns all entries in storageDirs
   */
  public Iterator<StorageDirectory> dirIterator() {
    return dirIterator(null);
  }
  
  /**
   * Return iterator based on Storage Directory Type
   * This iterator selects entries in storageDirs of type dirType and returns
   * them via the Iterator
   */
  public Iterator<StorageDirectory> dirIterator(StorageDirType dirType) {
    return dirIterator(dirType, true);
  }
  
  /**
   * Return all entries in storageDirs, potentially excluding shared dirs.
   * @param includeShared whether or not to include shared dirs.
   * @return an iterator over the configured storage dirs.
   */
  public Iterator<StorageDirectory> dirIterator(boolean includeShared) {
    return dirIterator(null, includeShared);
  }
  
  /**
   * @param dirType all entries will be of this type of dir
   * @param includeShared true to include any shared directories,
   *        false otherwise
   * @return an iterator over the configured storage dirs.
   */
  public Iterator<StorageDirectory> dirIterator(StorageDirType dirType,
      boolean includeShared) {
    return new DirIterator(dirType, includeShared);
  }
  
  public Iterable<StorageDirectory> dirIterable(final StorageDirType dirType) {
    return new Iterable<StorageDirectory>() {
      @Override
      public Iterator<StorageDirectory> iterator() {
        return dirIterator(dirType);
      }
    };
  }
  
  
  /**
   * generate storage list (debug line)
   */
  public String listStorageDirectories() {
    StringBuilder buf = new StringBuilder();
    for (StorageDirectory sd : storageDirs) {
      buf.append(sd.getRoot() + "(" + sd.getStorageDirType() + ");");
    }
    return buf.toString();
  }
  
  /**
   * One of the storage directories.
   */
  @InterfaceAudience.Private
  public static class StorageDirectory implements FormatConfirmable {
    final File root;              // root directory 存储目录的根目录，代表HDFS元数据存储的位置
    // whether or not this dir is shared between two separate NNs for HA, or
    // between multiple block pools in the case of federation.
    final boolean isShared; //表示该存储目录是否在高可用（HA）模式下被多个NameNode共享，或在联邦（Federation）模式下被多个Block Pool共享
    final StorageDirType dirType; // storage dir type 存储目录的类型，通常是StorageDirType枚举值，例如CURRENT、PREVIOUS等
    FileLock lock;                // storage lock 负责对存储目录进行锁定，防止多个进程同时访问
    private final FsPermission permission; //目录的文件权限，使用FsPermission来管理文件的读写执行权限

    private String storageUuid = null;      // Storage directory identifier. 存储目录的唯一标识符，通常用于区分不同的存储目录
    
    private final StorageLocation location; //存储目录的位置信息，可能是本地文件系统路径或远程存储URI
    //构造函数
    public StorageDirectory(File dir) {
      this(dir, null, false);
    }
    
    public StorageDirectory(StorageLocation location) {
      this(null, false, location);
    }

    public StorageDirectory(File dir, StorageDirType dirType) {
      this(dir, dirType, false);
    }
    //设置存储目录的uuid
    public void setStorageUuid(String storageUuid) {
      this.storageUuid = storageUuid;
    }

    public String getStorageUuid() {
      return storageUuid;
    }

    /**
     * Constructor
     * @param dir directory corresponding to the storage
     * @param dirType storage directory type
     * @param isShared whether or not this dir is shared between two NNs. true
     *          disables locking on the storage directory, false enables locking
     */
    public StorageDirectory(File dir, StorageDirType dirType, boolean isShared) {
      this(dir, dirType, isShared, null);
    }

    public StorageDirectory(File dir, StorageDirType dirType,
                            boolean isShared, FsPermission permission) {
      this(dir, dirType, isShared, null, permission);
    }

    /**
     * Constructor
     * @param dirType storage directory type
     * @param isShared whether or not this dir is shared between two NNs. true
     *          disables locking on the storage directory, false enables locking
     * @param location the {@link StorageLocation} for this directory
     */
    public StorageDirectory(StorageDirType dirType, boolean isShared,
        StorageLocation location) {
      this(getStorageLocationFile(location), dirType, isShared, location, null);
    }

    /**
     * Constructor
     * @param bpid the block pool id
     * @param dirType storage directory type
     * @param isShared whether or not this dir is shared between two NNs. true
     *          disables locking on the storage directory, false enables locking
     * @param location the {@link StorageLocation} for this directory
     */
    public StorageDirectory(String bpid, StorageDirType dirType,
        boolean isShared, StorageLocation location) {
      this(getBlockPoolCurrentDir(bpid, location), dirType,
          isShared, location, null);
    }
    //bpid：String 类型，表示 Block Pool ID，用于唯一标识一个数据块池（Block Pool）。
    // 在 HDFS 联邦（Federation）模式下，多个 NameNode 共享不同的 Block Pool，每个 Block Pool 都有一个唯一的标识符
    //获取特定 Block Pool 在存储设备中 current 目录的路径
    private static File getBlockPoolCurrentDir(String bpid,
        StorageLocation location) {
      if (location == null ||
          location.getStorageType() == StorageType.PROVIDED) {
        return null;
      } else {
        //生成 Block Pool 的 current 目录路径
        return new File(location.getBpURI(bpid, STORAGE_DIR_CURRENT));
      }
    }

    private StorageDirectory(File dir, StorageDirType dirType,
        boolean isShared, StorageLocation location, FsPermission permission) {
      this.root = dir;
      this.lock = null;
      // default dirType is UNDEFINED
      this.dirType = (dirType == null ? NameNodeDirType.UNDEFINED : dirType);
      this.isShared = isShared;
      this.location = location;
      this.permission = permission;
      assert location == null || dir == null ||
          dir.getAbsolutePath().startsWith(
              new File(location.getUri()).getAbsolutePath()):
            "The storage location and directory should be equal";
    }

    private static File getStorageLocationFile(StorageLocation location) {
      if (location == null ||
          location.getStorageType() == StorageType.PROVIDED) {
        return null;
      }
      try {
        return new File(location.getUri());
      } catch (IllegalArgumentException e) {
        //if location does not refer to a File
        return null;
      }
    }

    /**
     * Get root directory of this storage
     */
    public File getRoot() {
      return root;
    }

    /**
     * Get storage directory type
     */
    public StorageDirType getStorageDirType() {
      return dirType;
    }    

    /**获取存储目录的大小
     * Get storage directory size.
     */
    public long getDirecorySize() {
      try {
        if (!isShared() && root != null && root.exists()) {
          return FileUtils.sizeOfDirectory(root);
        }
      } catch (Exception e) {
        LOG.warn("Failed to get directory size : {}", root, e);
      }
      return 0;
    }
    //从文件读取配置信息，然后设置到storage里面
    public void read(File from, Storage storage) throws IOException {
      Properties props = readPropertiesFile(from);
      storage.setFieldsFromProperties(props, this);
    }

    /**
     * Clear and re-create storage directory.
     * <p>
     * Removes contents of the current directory and creates an empty directory.
     * 
     * This does not fully format storage directory. 
     * It cannot write the version file since it should be written last after  
     * all other storage type dependent files are written.
     * Derived storage is responsible for setting specific storage values and
     * writing the version file to disk.
     * 清理并重新创建当前存储目录
     * @throws IOException
     */
    public void clearDirectory() throws IOException {
      File curDir = this.getCurrentDir();
      if (curDir == null) {
        // if the directory is null, there is nothing to do.
        return;
      }
      if (curDir.exists()) {
        File[] files = FileUtil.listFiles(curDir);
        LOG.info("Will remove files: {}", Arrays.toString(files));
        if (!(FileUtil.fullyDelete(curDir)))
          throw new IOException("Cannot remove current directory: " + curDir);
      }
      if (!curDir.mkdirs()) {
        throw new IOException("Cannot create directory " + curDir);
      }
      if (permission != null) {
        try {
          Set<PosixFilePermission> permissions =
              PosixFilePermissions.fromString(permission.toString());
          Files.setPosixFilePermissions(curDir.toPath(), permissions);
        } catch (UnsupportedOperationException uoe) {
          // Default to FileUtil for non posix file systems
          FileUtil.setPermission(curDir, permission);
        }
      }
    }

    /**
     * Directory {@code current} contains latest files defining
     * the file system meta-data.
     * 存储系统的当前目录
     * @return the directory path
     */
    public File getCurrentDir() {
      if (root == null) {
        return null;
      }
      return new File(root, STORAGE_DIR_CURRENT);
    }

    /**
     * File {@code VERSION} contains the following fields:
     * <ol>
     * <li>node type</li>
     * <li>layout version</li>
     * <li>namespaceID</li>
     * <li>fs state creation time</li>
     * <li>other fields specific for this node type</li>
     * </ol>
     * The version file is always written last during storage directory updates.
     * The existence of the version file indicates that all other files have
     * been successfully written in the storage directory, the storage is valid
     * and does not need to be recovered.
     * 存储目录的当前版本文件
     * @return the version file path
     */
    public File getVersionFile() {
      if (root == null) {
        return null;
      }
      return new File(new File(root, STORAGE_DIR_CURRENT), STORAGE_FILE_VERSION);
    }

    /**
     * File {@code VERSION} from the {@code previous} directory.
     * 获取前一个版本文件
     * @return the previous version file path
     */
    public File getPreviousVersionFile() {
      if (root == null) {
        return null;
      }
      return new File(new File(root, STORAGE_DIR_PREVIOUS), STORAGE_FILE_VERSION);
    }

    /**
     * Directory {@code previous} contains the previous file system state,
     * which the system can be rolled back to.
     * 获取前一个目录
     * @return the directory path
     */
    public File getPreviousDir() {
      if (root == null) {
        return null;
      }
      return new File(root, STORAGE_DIR_PREVIOUS);
    }

    /**
     * {@code previous.tmp} is a transient directory, which holds
     * current file system state while the new state is saved into the new
     * {@code current} during upgrade.
     * If the saving succeeds {@code previous.tmp} will be moved to
     * {@code previous}, otherwise it will be renamed back to 
     * {@code current} by the recovery procedure during startup.
     * 获取前一个临时文件
     * @return the directory path
     */
    public File getPreviousTmp() {
      if (root == null) {
        return null;
      }
      return new File(root, STORAGE_TMP_PREVIOUS);
    }

    /**
     * {@code removed.tmp} is a transient directory, which holds
     * current file system state while the previous state is moved into
     * {@code current} during rollback.
     * If the moving succeeds {@code removed.tmp} will be removed,
     * otherwise it will be renamed back to 
     * {@code current} by the recovery procedure during startup.
     * 删除临时文件
     * @return the directory path
     */
    public File getRemovedTmp() {
      if (root == null) {
        return null;
      }
      return new File(root, STORAGE_TMP_REMOVED);
    }

    /**
     * {@code finalized.tmp} is a transient directory, which holds
     * the {@code previous} file system state while it is being removed
     * in response to the finalize request.
     * Finalize operation will remove {@code finalized.tmp} when completed,
     * otherwise the removal will resume upon the system startup.
     * 
     * @return the directory path
     */
    public File getFinalizedTmp() {
      if (root == null) {
        return null;
      }
      return new File(root, STORAGE_TMP_FINALIZED);
    }

    /**
     * {@code lastcheckpoint.tmp} is a transient directory, which holds
     * current file system state while the new state is saved into the new
     * {@code current} during regular namespace updates.
     * If the saving succeeds {@code lastcheckpoint.tmp} will be moved to
     * {@code previous.checkpoint}, otherwise it will be renamed back to 
     * {@code current} by the recovery procedure during startup.
     * 
     * @return the directory path
     */
    public File getLastCheckpointTmp() {
      if (root == null) {
        return null;
      }
      return new File(root, STORAGE_TMP_LAST_CKPT);
    }

    /**
     * {@code previous.checkpoint} is a directory, which holds the previous
     * (before the last save) state of the storage directory.
     * The directory is created as a reference only, it does not play role
     * in state recovery procedures, and is recycled automatically, 
     * but it may be useful for manual recovery of a stale state of the system.
     * 
     * @return the directory path
     */
    public File getPreviousCheckpoint() {
      if (root == null) {
        return null;
      }
      return new File(root, STORAGE_PREVIOUS_CKPT);
    }

    /**
     * Check to see if current/ directory is empty. This method is used
     * before determining to format the directory.
     *
     * @throws InconsistentFSStateException if not empty.
     * @throws IOException if unable to list files under the directory.
     */
    private void checkEmptyCurrent() throws InconsistentFSStateException,
        IOException {
      File currentDir = getCurrentDir();
      if(currentDir == null || !currentDir.exists()) {
        // if current/ does not exist, it's safe to format it.
        return;
      }
      try(DirectoryStream<java.nio.file.Path> dirStream =
          Files.newDirectoryStream(currentDir.toPath())) {
        if (dirStream.iterator().hasNext()) {
          throw new InconsistentFSStateException(root,
              "Can't format the storage directory because the current "
                  + "directory is not empty.");
        }
      }
    }

    /**
     * Check consistency of the storage directory.
     *
     * @param startOpt a startup option.
     * @param storage The Storage object that manages this StorageDirectory.
     *
     * @return state {@link StorageState} of the storage directory
     * @throws InconsistentFSStateException if directory state is not
     * consistent and cannot be recovered.
     * @throws IOException
     */
    public StorageState analyzeStorage(StartupOption startOpt, Storage storage)
        throws IOException {
      return analyzeStorage(startOpt, storage, false);
    }

    /**
     * Check consistency of the storage directory.
     * 主要是检查存储目录的一致性
     * @param startOpt a startup option.
     * @param storage The Storage object that manages this StorageDirectory.
     * @param checkCurrentIsEmpty if true, make sure current/ directory
     *                            is empty before determining to format it.
     *  
     * @return state {@link StorageState} of the storage directory 
     * @throws InconsistentFSStateException if directory state is not 
     * consistent and cannot be recovered.
     * @throws IOException
     */
    public StorageState analyzeStorage(StartupOption startOpt, Storage storage,
        boolean checkCurrentIsEmpty)
        throws IOException {
      //远程目录，直接返回正常
      if (location != null &&
          location.getStorageType() == StorageType.PROVIDED) {
        // currently we assume that PROVIDED storages are always NORMAL
        return StorageState.NORMAL;
      }

      assert root != null : "root is null";
      boolean hadMkdirs = false;
      String rootPath = root.getCanonicalPath();
      try { // check that storage exists
        if (!root.exists()) {//文件目录不存在
          // storage directory does not exist
          if (startOpt != StartupOption.FORMAT &&
              startOpt != StartupOption.HOTSWAP) {
            LOG.warn("Storage directory {} does not exist", rootPath);
            return StorageState.NON_EXISTENT;
          }
          LOG.info("{} does not exist. Creating ...", rootPath);
          if (!root.mkdirs()) {
            throw new IOException("Cannot create directory " + rootPath);
          }
          hadMkdirs = true;
        }
        // or is inaccessible
        if (!root.isDirectory()) {//root不是目录对象
          LOG.warn("{} is not a directory", rootPath);
          return StorageState.NON_EXISTENT;
        }
        if (!FileUtil.canWrite(root)) {//没有写的权限
          LOG.warn("Cannot access storage directory {}", rootPath);
          return StorageState.NON_EXISTENT;
        }
      } catch(SecurityException ex) {
        LOG.warn("Cannot access storage directory {}", rootPath, ex);
        return StorageState.NON_EXISTENT;
      }

      this.lock(); // lock storage if it exists

      // If startOpt is HOTSWAP, it returns NOT_FORMATTED for empty directory,
      // while it also checks the layout version.
      if (startOpt == HdfsServerConstants.StartupOption.FORMAT ||
          (startOpt == StartupOption.HOTSWAP && hadMkdirs)) {
        if (checkCurrentIsEmpty) {
          checkEmptyCurrent();
        }
        return StorageState.NOT_FORMATTED;
      }

      if (startOpt != HdfsServerConstants.StartupOption.IMPORT) {
        storage.checkOldLayoutStorage(this);
      }

      // check whether current directory is valid
      File versionFile = getVersionFile();
      boolean hasCurrent = versionFile.exists();

      // check which directories exist
      boolean hasPrevious = getPreviousDir().exists();
      boolean hasPreviousTmp = getPreviousTmp().exists();
      boolean hasRemovedTmp = getRemovedTmp().exists();
      boolean hasFinalizedTmp = getFinalizedTmp().exists();
      boolean hasCheckpointTmp = getLastCheckpointTmp().exists();

      if (!(hasPreviousTmp || hasRemovedTmp
          || hasFinalizedTmp || hasCheckpointTmp)) {
        // no temp dirs - no recovery
        if (hasCurrent)
          return StorageState.NORMAL;
        if (hasPrevious)
          throw new InconsistentFSStateException(root,
                              "version file in current directory is missing.");
        if (checkCurrentIsEmpty) {
          checkEmptyCurrent();
        }
        return StorageState.NOT_FORMATTED;
      }

      if ((hasPreviousTmp?1:0) + (hasRemovedTmp?1:0)
          + (hasFinalizedTmp?1:0) + (hasCheckpointTmp?1:0) > 1)
        // more than one temp dirs
        throw new InconsistentFSStateException(root,
                                               "too many temporary directories.");

      // # of temp dirs == 1 should either recover or complete a transition
      if (hasCheckpointTmp) {
        return hasCurrent ? StorageState.COMPLETE_CHECKPOINT
                          : StorageState.RECOVER_CHECKPOINT;
      }

      if (hasFinalizedTmp) {
        if (hasPrevious)
          throw new InconsistentFSStateException(root,
                                                 STORAGE_DIR_PREVIOUS + " and " + STORAGE_TMP_FINALIZED
                                                 + "cannot exist together.");
        return StorageState.COMPLETE_FINALIZE;
      }

      if (hasPreviousTmp) {
        if (hasPrevious)
          throw new InconsistentFSStateException(root,
                                                 STORAGE_DIR_PREVIOUS + " and " + STORAGE_TMP_PREVIOUS
                                                 + " cannot exist together.");
        if (hasCurrent)
          return StorageState.COMPLETE_UPGRADE;
        return StorageState.RECOVER_UPGRADE;
      }
      
      assert hasRemovedTmp : "hasRemovedTmp must be true";
      if (!(hasCurrent ^ hasPrevious))
        throw new InconsistentFSStateException(root,
                                               "one and only one directory " + STORAGE_DIR_CURRENT 
                                               + " or " + STORAGE_DIR_PREVIOUS 
                                               + " must be present when " + STORAGE_TMP_REMOVED
                                               + " exists.");
      if (hasCurrent)
        return StorageState.COMPLETE_ROLLBACK;
      return StorageState.RECOVER_ROLLBACK;
    }

    /**
     * Complete or recover storage state from previously failed transition.
     * 从之前失败的状态恢复存储目录
     * @param curState specifies what/how the state should be recovered
     * @throws IOException
     */
    public void doRecover(StorageState curState) throws IOException {
      File curDir = getCurrentDir();
      if (curDir == null || root == null) {
        // at this point, we do not support recovery on PROVIDED storages
        return;
      }
      String rootPath = root.getCanonicalPath();
      switch(curState) {
      case COMPLETE_UPGRADE:  // mv previous.tmp -> previous
        LOG.info("Completing previous upgrade for storage directory {}",
            rootPath);
        rename(getPreviousTmp(), getPreviousDir());
        return;
      case RECOVER_UPGRADE:   // mv previous.tmp -> current
        LOG.info("Recovering storage directory {} from previous upgrade",
            rootPath);
        deleteAsync(curDir);
        rename(getPreviousTmp(), curDir);
        return;
      case COMPLETE_ROLLBACK: // rm removed.tmp
        LOG.info("Completing previous rollback for storage directory {}",
            rootPath);
        deleteDir(getRemovedTmp());
        return;
      case RECOVER_ROLLBACK:  // mv removed.tmp -> current
        LOG.info("Recovering storage directory {} from previous rollback",
            rootPath);
        rename(getRemovedTmp(), curDir);
        return;
      case COMPLETE_FINALIZE: // rm finalized.tmp
        LOG.info("Completing previous finalize for storage directory {}",
            rootPath);
        deleteAsync(getFinalizedTmp());
        return;
      case COMPLETE_CHECKPOINT: // mv lastcheckpoint.tmp -> previous.checkpoint
        LOG.info("Completing previous checkpoint for storage directory {}",
            rootPath);
        File prevCkptDir = getPreviousCheckpoint();
        deleteAsync(prevCkptDir);
        rename(getLastCheckpointTmp(), prevCkptDir);
        return;
      case RECOVER_CHECKPOINT:  // mv lastcheckpoint.tmp -> current
        LOG.info("Recovering storage directory {} from failed checkpoint",
            rootPath);
        deleteAsync(curDir);
        rename(getLastCheckpointTmp(), curDir);
        return;
      default:
        throw new IOException("Unexpected FS state: " + curState
            + " for storage directory: " + rootPath);
      }
    }

    /**
     * Rename the curDir to curDir.tmp and delete the curDir.tmp parallely.
     * @throws IOException
     */
    private void deleteAsync(File curDir) throws IOException {
      if (curDir.exists()) {
        File curTmp = new File(curDir.getParent(), curDir.getName() + ".tmp");
        if (curTmp.exists()) {
          deleteDir(curTmp);
        }
        rename(curDir, curTmp);
        new Thread("Async Delete Current.tmp") {
          public void run() {
            try {
              deleteDir(curTmp);
            } catch (IOException e) {
              LOG.warn("Deleting storage directory {} failed", curTmp);
            }
          }
        }.start();
      }
    }

    /**
     * @return true if the storage directory should prompt the user prior
     * to formatting (i.e if the directory appears to contain some data)
     * @throws IOException if the SD cannot be accessed due to an IO error
     */
    @Override
    public boolean hasSomeData() throws IOException {
      // Its alright for a dir not to exist, or to exist (properly accessible)
      // and be completely empty.
      if (!root.exists()) return false;
      
      if (!root.isDirectory()) {
        // a file where you expect a directory should not cause silent
        // formatting
        return true;
      }
      
      if (FileUtil.listFiles(root).length == 0) {
        // Empty dir can format without prompt.
        return false;
      }
      
      return true;
    }
    
    public boolean isShared() {
      return isShared;
    }


    /**
     * Lock storage to provide exclusive access.
     * 
     * <p> Locking is not supported by all file systems.
     * E.g., NFS does not consistently support exclusive locks.
     * 
     * <p> If locking is supported we guarantee exclusive access to the
     * storage directory. Otherwise, no guarantee is given.
     * 
     * @throws IOException if locking fails
     */
    public void lock() throws IOException {
      if (isShared()) {
        LOG.info("Locking is disabled for {}", this.root);
        return;
      }
      FileLock newLock = tryLock();
      if (newLock == null) {
        String msg = "Cannot lock storage " + this.root
          + ". The directory is already locked";
        LOG.info(msg);
        throw new IOException(msg);
      }
      // Don't overwrite lock until success - this way if we accidentally
      // call lock twice, the internal state won't be cleared by the second
      // (failed) lock attempt
      lock = newLock;
    }

    /**
     * Attempts to acquire an exclusive lock on the storage.
     * 
     * @return A lock object representing the newly-acquired lock or
     * <code>null</code> if storage is already locked.
     * @throws IOException if locking fails.
     */
    @SuppressWarnings("resource")
    FileLock tryLock() throws IOException {
      boolean deletionHookAdded = false;
      File lockF = new File(root, STORAGE_FILE_LOCK);
      if (!lockF.exists()) {
        lockF.deleteOnExit();
        deletionHookAdded = true;
      }
      RandomAccessFile file = new RandomAccessFile(lockF, "rws");
      String jvmName = ManagementFactory.getRuntimeMXBean().getName();
      FileLock res = null;
      try {
        res = file.getChannel().tryLock();
        if (null == res) {
          LOG.error("Unable to acquire file lock on path {}", lockF);
          throw new OverlappingFileLockException();
        }
        file.write(jvmName.getBytes(StandardCharsets.UTF_8));
        LOG.info("Lock on {} acquired by nodename {}", lockF, jvmName);
      } catch(OverlappingFileLockException oe) {
        // Cannot read from the locked file on Windows.
        String lockingJvmName = Path.WINDOWS ? "" : (" " + file.readLine());
        LOG.error("It appears that another node {} has already locked the "
            + "storage directory: {}", lockingJvmName, root, oe);
        file.close();
        return null;
      } catch(IOException e) {
        LOG.error("Failed to acquire lock on {}. If this storage directory is"
            + " mounted via NFS, ensure that the appropriate nfs lock services"
            + " are running.", lockF, e);
        file.close();
        throw e;
      }
      if (!deletionHookAdded) {
        // If the file existed prior to our startup, we didn't
        // call deleteOnExit above. But since we successfully locked
        // the dir, we can take care of cleaning it up.
        lockF.deleteOnExit();
      }
      return res;
    }

    /**
     * Unlock storage.
     * 
     * @throws IOException
     */
    public void unlock() throws IOException {
      if (this.lock == null)
        return;
      this.lock.release();
      lock.channel().close();
      lock = null;
    }
    
    @Override
    public String toString() {
      return "Storage Directory root= " + this.root +
          "; location= " + this.location;
    }

    /**
     * Check whether underlying file system supports file locking.
     * 
     * @return <code>true</code> if exclusive locks are supported or
     *         <code>false</code> otherwise.
     * @throws IOException
     * @see StorageDirectory#lock()
     */
    public boolean isLockSupported() throws IOException {
      FileLock firstLock = null;
      FileLock secondLock = null;
      try {
        firstLock = lock;
        if(firstLock == null) {
          firstLock = tryLock();
          if(firstLock == null)
            return true;
        }
        secondLock = tryLock();
        if(secondLock == null)
          return true;
      } finally {
        if(firstLock != null && firstLock != lock) {
          firstLock.release();
          firstLock.channel().close();
        }
        if(secondLock != null) {
          secondLock.release();
          secondLock.channel().close();
        }
      }
      return false;
    }

    public StorageLocation getStorageLocation() {
      return location;
    }
  }

  /**
   * Create empty storage info of the specified type
   */
  protected Storage(NodeType type) {
    super(type);
  }
  
  protected Storage(StorageInfo storageInfo) {
    super(storageInfo);
  }
  //返回存储对应的目录数
  public int getNumStorageDirs() {
    return storageDirs.size();
  }

  public List<StorageDirectory> getStorageDirs() {
    return storageDirs;
  }
  //获取第idx个目录
  public StorageDirectory getStorageDir(int idx) {
    return storageDirs.get(idx);
  }
  
  /** 获取单独的存储目录
   * @return the storage directory, with the precondition that this storage
   * has exactly one storage directory
   */
  public StorageDirectory getSingularStorageDir() {
    Preconditions.checkState(storageDirs.size() == 1);
    return storageDirs.get(0);
  }
  //添加存储目录
  protected void addStorageDir(StorageDirectory sd) {
    storageDirs.add(sd);
  }

  /**
   * Returns true if the storage directory on the given directory is already
   * loaded.
   * @param root the root directory of a {@link StorageDirectory}
   * @throws IOException if failed to get canonical path.
   */
  //判断该存储是否包含指定的目录root
  protected boolean containsStorageDir(File root) throws IOException {
    for (StorageDirectory sd : storageDirs) {
      if (sd.getRoot().getCanonicalPath().equals(root.getCanonicalPath())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns true if the storage directory on the given directory is already
   * loaded.
   * @param location the {@link StorageLocation}
   * @throws IOException if failed to get canonical path.
   */
  protected boolean containsStorageDir(StorageLocation location)
      throws IOException {
    for (StorageDirectory sd : storageDirs) {
      if (location.matchesStorageDirectory(sd)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns true if the storage directory on the given location is already
   * loaded.
   * @param location the {@link StorageLocation}
   * @param bpid the block pool id
   * @return true if the location matches to any existing storage directories
   * @throws IOException IOException if failed to read location
   * or storage directory path
   */
  protected boolean containsStorageDir(StorageLocation location, String bpid)
      throws IOException {
    for (StorageDirectory sd : storageDirs) {
      if (location.matchesStorageDirectory(sd, bpid)) {
        return true;
      }
    }
    return false;
  }

  public NamespaceInfo getNamespaceInfo() {
    return new NamespaceInfo(this);
  }

  /**
   * Return true if the layout of the given storage directory is from a version
   * of Hadoop prior to the introduction of the "current" and "previous"
   * directories which allow upgrade and rollback.
   */
  public abstract boolean isPreUpgradableLayout(StorageDirectory sd)
  throws IOException;

  /**
   * Check if the given storage directory comes from a version of Hadoop
   * prior to when the directory layout changed (ie 0.13). If this is
   * the case, this method throws an IOException.
   */
  private void checkOldLayoutStorage(StorageDirectory sd) throws IOException {
    if (isPreUpgradableLayout(sd)) {
      checkVersionUpgradable(0);
    }
  }

  /**
   * Checks if the upgrade from {@code oldVersion} is supported.
   * @param oldVersion the version of the metadata to check with the current
   *                   version
   * @throws IOException if upgrade is not supported
   */
  public static void checkVersionUpgradable(int oldVersion) 
                                     throws IOException {
    if (oldVersion > LAST_UPGRADABLE_LAYOUT_VERSION) {
      String msg = "*********** Upgrade is not supported from this " +
                   " older version " + oldVersion + 
                   " of storage to the current version." + 
                   " Please upgrade to " + LAST_UPGRADABLE_HADOOP_VERSION +
                   " or a later version and then upgrade to current" +
                   " version. Old layout version is " + 
                   (oldVersion == 0 ? "'too old'" : (""+oldVersion)) +
                   " and latest layout version this software version can" +
                   " upgrade from is " + LAST_UPGRADABLE_LAYOUT_VERSION +
                   ". ************";
      LOG.error(msg);
      throw new IOException(msg); 
    }
    
  }
  
  /**
   * Iterate over each of the {@link FormatConfirmable} objects,
   * potentially checking with the user whether it should be formatted.
   * 
   * If running in interactive mode, will prompt the user for each
   * directory to allow them to format anyway. Otherwise, returns
   * false, unless 'force' is specified.
   * 
   * @param force format regardless of whether dirs exist
   * @param interactive prompt the user when a dir exists
   * @return true if formatting should proceed
   * @throws IOException if some storage cannot be accessed
   */
  public static boolean confirmFormat(
      Iterable<? extends FormatConfirmable> items,
      boolean force, boolean interactive) throws IOException {
    for (FormatConfirmable item : items) {
      if (!item.hasSomeData())
        continue;
      if (force) { // Don't confirm, always format.
        System.err.println(
            "Data exists in " + item + ". Formatting anyway.");
        continue;
      }
      if (!interactive) { // Don't ask - always don't format
        System.err.println(
            "Running in non-interactive mode, and data appears to exist in " +
            item + ". Not formatting.");
        return false;
      }
      if (!ToolRunner.confirmPrompt("Re-format filesystem in " + item + " ?")) {
        System.err.println("Format aborted in " + item);
        return false;
      }
    }
    
    return true;
  }
  
  /**
   * Interface for classes which need to have the user confirm their
   * formatting during NameNode -format and other similar operations.
   * 
   * This is currently a storage directory or journal manager.
   */
  @InterfaceAudience.Private
  //用于需要在 NameNode 格式化操作等类似操作中进行格式确认的类
  public interface FormatConfirmable {
    /**
     * @return true if the storage seems to have some valid data in it,
     * and the user should be required to confirm the format. Otherwise,
     * false.
     * @throws IOException if the storage cannot be accessed at all.
     */
    //如果存储中存在一些有效数据，返回 true，表示用户在执行格式化时需要确认
    public boolean hasSomeData() throws IOException;
    
    /**
     * @return a string representation of the formattable item, suitable
     * for display to the user inside a prompt
     */
    //目的是提供一个字符串表示形式，适用于在用户提示中显示。具体来说，该字符串应该描述可格式化的项，以便在进行格式化操作时，
    // 系统可以通过字符串向用户展示该项的相关信息，帮助用户做出是否继续格式化的决策
    @Override
    public String toString();
  }
  
  /**
   * Set common storage fields into the given properties object.
   * Should be overloaded if additional fields need to be set.
   * 使用存储目录的属性设置props属性
   * @param props the Properties object to write into
   */
  protected void setPropertiesFromFields(Properties props, 
                                         StorageDirectory sd)
      throws IOException {
    props.setProperty("layoutVersion", String.valueOf(layoutVersion));
    props.setProperty("storageType", storageType.toString());
    props.setProperty("namespaceID", String.valueOf(namespaceID));
    // Set clusterID in version with federation support
    if (versionSupportsFederation(getServiceLayoutFeatureMap())) {
      props.setProperty("clusterID", clusterID);
    }
    props.setProperty("cTime", String.valueOf(cTime));
  }

  /**把存储目录相关的属性写入版本文件
   * Write properties to the VERSION file in the given storage directory.
   */
  public void writeProperties(StorageDirectory sd) throws IOException {
    writeProperties(sd.getVersionFile(), sd);
  }
  //把存储相关的配置写入版本文件
  public void writeProperties(File to, StorageDirectory sd) throws IOException {
    if (to == null) {
      return;
    }
    Properties props = new Properties();
    setPropertiesFromFields(props, sd); //设置属性文件的属性
    writeProperties(to, props); //写入版本文件
  }

  public static void writeProperties(File to, Properties props)
      throws IOException {
    try (RandomAccessFile file = new RandomAccessFile(to, "rws");
        FileOutputStream out = new FileOutputStream(file.getFD())) {
      file.seek(0);
      /*
       * If server is interrupted before this line,
       * the version file will remain unchanged.
       */
      props.store(out, null);
      /*
       * Now the new fields are flushed to the head of the file, but file
       * length can still be larger then required and therefore the file can
       * contain whole or corrupted fields from its old contents in the end.
       * If server is interrupted here and restarted later these extra fields
       * either should not effect server behavior or should be handled
       * by the server correctly.
       */
      file.setLength(out.getChannel().position());
    }
  }

  public static void rename(File from, File to) throws IOException {
    try {
      NativeIO.renameTo(from, to);
    } catch (NativeIOException e) {
      throw new IOException("Failed to rename " + from.getCanonicalPath()
        + " to " + to.getCanonicalPath() + " due to failure in native rename. "
        + e.toString());
    }
  }

  /**
   * Copies a file (usually large) to a new location using native unbuffered IO.
   * <p>
   * This method copies the contents of the specified source file
   * to the specified destination file using OS specific unbuffered IO.
   * The goal is to avoid churning the file system buffer cache when copying
   * large files.
   *
   * We can't use FileUtils#copyFile from apache-commons-io because it
   * is a buffered IO based on FileChannel#transferFrom, which uses MmapByteBuffer
   * internally.
   *
   * The directory holding the destination file is created if it does not exist.
   * If the destination file exists, then this method will delete it first.
   * <p>
   * <strong>Note:</strong> Setting <code>preserveFileDate</code> to
   * {@code true} tries to preserve the file's last modified
   * date/times using {@link File#setLastModified(long)}, however it is
   * not guaranteed that the operation will succeed.
   * If the modification operation fails, no indication is provided.
   *
   * @param srcFile  an existing file to copy, must not be {@code null}
   * @param destFile  the new file, must not be {@code null}
   * @param preserveFileDate  true if the file date of the copy
   *  should be the same as the original
   *
   * @throws NullPointerException if source or destination is {@code null}
   * @throws IOException if source or destination is invalid
   * @throws IOException if an IO error occurs during copying
   */
  public static void nativeCopyFileUnbuffered(File srcFile, File destFile,
      boolean preserveFileDate) throws IOException {
    if (srcFile == null) {
      throw new NullPointerException("Source must not be null");
    }
    if (destFile == null) {
      throw new NullPointerException("Destination must not be null");
    }
    if (srcFile.exists() == false) {
      throw new FileNotFoundException("Source '" + srcFile + "' does not exist");
    }
    if (srcFile.isDirectory()) {
      throw new IOException("Source '" + srcFile + "' exists but is a directory");
    }
    if (srcFile.getCanonicalPath().equals(destFile.getCanonicalPath())) {
      throw new IOException("Source '" + srcFile + "' and destination '" +
          destFile + "' are the same");
    }
    File parentFile = destFile.getParentFile();
    if (parentFile != null) {
      if (!parentFile.mkdirs() && !parentFile.isDirectory()) {
        throw new IOException("Destination '" + parentFile
            + "' directory cannot be created");
      }
    }
    if (destFile.exists()) {
      if (FileUtil.canWrite(destFile) == false) {
        throw new IOException("Destination '" + destFile
            + "' exists but is read-only");
      } else {
        if (destFile.delete() == false) {
          throw new IOException("Destination '" + destFile
              + "' exists but cannot be deleted");
        }
      }
    }
    try {
      NativeIO.copyFileUnbuffered(srcFile, destFile);
    } catch (NativeIOException e) {
      throw new IOException("Failed to copy " + srcFile.getCanonicalPath()
          + " to " + destFile.getCanonicalPath()
          + " due to failure in NativeIO#copyFileUnbuffered(). "
          + e.toString());
    }
    if (srcFile.length() != destFile.length()) {
      throw new IOException("Failed to copy full contents from '" + srcFile
          + "' to '" + destFile + "'");
    }
    if (preserveFileDate) {
      if (destFile.setLastModified(srcFile.lastModified()) == false) {
        LOG.debug("Failed to preserve last modified date from'{}' to '{}'",
            srcFile, destFile);
      }
    }
  }

  /**
   * Recursively delete all the content of the directory first and then 
   * the directory itself from the local filesystem.
   * @param dir The directory to delete
   * @throws IOException
   */
  //删除目录
  public static void deleteDir(File dir) throws IOException {
    if (!FileUtil.fullyDelete(dir))
      throw new IOException("Failed to delete " + dir.getCanonicalPath());
  }
  
  /**
   * Write all data storage files.
   * @throws IOException
   */
  public void writeAll() throws IOException {
    this.layoutVersion = getServiceLayoutVersion();
    for (Iterator<StorageDirectory> it = storageDirs.iterator(); it.hasNext();) {
      writeProperties(it.next());
    }
  }

  /**
   * Unlock all storage directories.
   * @throws IOException
   */
  public void unlockAll() throws IOException {
    for (Iterator<StorageDirectory> it = storageDirs.iterator(); it.hasNext();) {
      it.next().unlock();
    }
  }

  public static String getBuildVersion() {
    return VersionInfo.getRevision();
  }

  public static String getRegistrationID(StorageInfo storage) {
    return "NS-" + Integer.toString(storage.getNamespaceID())
      + "-" + storage.getClusterID()
      + "-" + Long.toString(storage.getCTime());
  }
  
  public static boolean is203LayoutVersion(int layoutVersion) {
    for (int lv203 : LAYOUT_VERSIONS_203) {
      if (lv203 == layoutVersion) {
        return true;
      }
    }
    return false;
  }
}
