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

package org.apache.hadoop.hdfs.server.datanode;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Matcher;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.datanode.checker.Checkable;
import org.apache.hadoop.hdfs.server.datanode.checker.VolumeCheckResult;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.StringUtils;


/**
 * Encapsulates the URI and storage medium that together describe a
 * storage directory.
 * The default storage medium is assumed to be DISK, if none is specified.
 * 管理 HDFS 数据节点的存储目录信息
 */
@InterfaceAudience.Private
public class StorageLocation
    implements Checkable<StorageLocation.CheckContext, VolumeCheckResult>,
               Comparable<StorageLocation> {
  private final StorageType storageType;//表示存储类型，例如 DISK、SSD、ARCHIVE 等，默认值为 DISK
  private final URI baseURI;//表示存储位置的 URI，用于唯一标识该存储目录
  /** Regular expression that describes a storage uri with a storage type.
   *  e.g. [Disk]/storages/storage1/
   */
  private static final Pattern STORAGE_LOCATION_REGEX =
      Pattern.compile("^\\[(\\w*)\\](.+)$");//匹配带有存储类型的存储路径字符串

  /** Regular expression for the capacity ratio of a storage volume (uri).
   *  This is useful when configuring multiple
   *  storage types on same disk mount (same-disk-tiering).
   *  e.g. [0.3]/disk1/archive/
   */
  private static final Pattern CAPACITY_RATIO_REGEX =
      Pattern.compile("^\\[([0-9.]*)\\](.+)$"); //用于匹配带有存储容量比率的存储路径字符串

  private StorageLocation(StorageType storageType, URI uri) {
    this.storageType = storageType;
    if (uri.getScheme() == null || uri.getScheme().equals("file")) {
      // make sure all URIs that point to a file have the same scheme
      uri = normalizeFileURI(uri);
    }
    baseURI = uri;
  }

  public static URI normalizeFileURI(URI uri) {
    try {
      File uriFile = new File(uri.getPath());
      String uriStr = uriFile.toURI().normalize().toString();
      if (uriStr.endsWith("/")) {
        uriStr = uriStr.substring(0, uriStr.length() - 1);
      }
      return new URI(uriStr);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
              "URI: " + uri + " is not in the expected format");
    }
  }

  public StorageType getStorageType() {
    return this.storageType;
  }

  public URI getUri() {
    return baseURI;
  }

  public URI getNormalizedUri() {
    return baseURI.normalize();
  }

  public boolean matchesStorageDirectory(StorageDirectory sd)
      throws IOException {
    return this.equals(sd.getStorageLocation());
  }

  public boolean matchesStorageDirectory(StorageDirectory sd,
      String bpid) throws IOException {
    if (sd.getStorageLocation().getStorageType() == StorageType.PROVIDED &&
        storageType == StorageType.PROVIDED) {
      return matchesStorageDirectory(sd);
    }
    if (sd.getStorageLocation().getStorageType() == StorageType.PROVIDED ||
        storageType == StorageType.PROVIDED) {
      // only one PROVIDED storage directory can exist; so this cannot match!
      return false;
    }
    // both storage directories are local
    return this.getBpURI(bpid, Storage.STORAGE_DIR_CURRENT).normalize()
        .equals(sd.getRoot().toURI().normalize());
  }

  /**
   * Attempt to parse a storage uri with storage class and URI. The storage
   * class component of the uri is case-insensitive.
   *
   * @param rawLocation Location string of the format [type]uri, where [type] is
   *                    optional.
   * @return A StorageLocation object if successfully parsed, null otherwise.
   *         Does not throw any exceptions.
   */
  public static StorageLocation parse(String rawLocation)
      throws IOException, SecurityException {
    Matcher matcher = STORAGE_LOCATION_REGEX.matcher(rawLocation);
    StorageType storageType = StorageType.DEFAULT;
    String location = rawLocation;

    if (matcher.matches()) {
      String classString = matcher.group(1);
      location = matcher.group(2).trim();
      if (!classString.isEmpty()) {
        storageType =
            StorageType.valueOf(StringUtils.toUpperCase(classString));
      }
    }
    //do Path.toURI instead of new URI(location) as this ensures that
    //"/a/b" and "/a/b/" are represented in a consistent manner
    return new StorageLocation(storageType, new Path(location).toUri());
  }

  /**
   * Attempt to parse the storage capacity ratio and related volume directory
   * out of the capacity ratio config string.
   *
   * @param capacityRatioConf Config string of the capacity ratio
   * @return Map of URI of the volume and capacity ratio.
   * @throws SecurityException when format is incorrect or ratio is not
   *         between 0 - 1.
   */
  public static Map<URI, Double> parseCapacityRatio(String capacityRatioConf)
      throws SecurityException {
    Map<URI, Double> result = new HashMap<>();
    capacityRatioConf = capacityRatioConf.replaceAll("\\s", "");
    if (capacityRatioConf.isEmpty()) {
      return result;
    }
    String[] capacityRatios = capacityRatioConf.split(",");
    for (String ratio : capacityRatios) {
      Matcher matcher = CAPACITY_RATIO_REGEX.matcher(ratio);
      if (matcher.matches()) {
        String capacityString = matcher.group(1).trim();
        String location = matcher.group(2).trim();
        double capacityRatio = Double.parseDouble(capacityString);
        if (capacityRatio > 1 || capacityRatio < 0) {
          throw new IllegalArgumentException("Capacity ratio" + capacityRatio
              + " is not between 0 to 1: " + ratio);
        }
        result.put(new Path(location).toUri(), capacityRatio);
      } else {
        throw new IllegalArgumentException(
            "Capacity ratio config is not with correct format: "
                + capacityRatioConf
        );
      }
    }
    return result;
  }

  @Override
  public String toString() {
    return "[" + storageType + "]" + baseURI.normalize();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof StorageLocation)) {
      return false;
    }
    int comp = compareTo((StorageLocation) obj);
    return comp == 0;
  }

  @Override
  public int hashCode() {
    return toString().hashCode();
  }

  @Override
  public int compareTo(StorageLocation obj) {
    if (obj == this) {
      return 0;
    } else if (obj == null) {
      return -1;
    }

    StorageLocation otherStorage = (StorageLocation) obj;
    if (this.getNormalizedUri() != null &&
        otherStorage.getNormalizedUri() != null) {
      return this.getNormalizedUri().compareTo(
          otherStorage.getNormalizedUri());
    } else if (this.getNormalizedUri() == null &&
        otherStorage.getNormalizedUri() == null) {
      return this.storageType.compareTo(otherStorage.getStorageType());
    } else if (this.getNormalizedUri() == null) {
      return -1;
    } else {
      return 1;
    }

  }
  //bpid（Block Pool ID）：块池的唯一标识符，HDFS 中每个命名空间的存储会对应一个块池 (Block Pool)
  //currentStorageDir：存储目录名称，通常为 current，指向 HDFS 数据节点中当前使用的存储路径
  //返回值，根据 bpid 和 currentStorageDir 拼接得到的块池目录的 URI
  public URI getBpURI(String bpid, String currentStorageDir) {
    try {
      //第一层嵌套：new File(localFile, currentStorageDir)  在基础路径下追加 currentStorageDir 子目录
      //第二层嵌套：new File(..., bpid)  在 currentStorageDir 下再追加块池 ID（bpid）

      File localFile = new File(getUri());
      return new File(new File(localFile, currentStorageDir), bpid).toURI();
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  /**
   * Create physical directory for block pools on the data node.
   *
   * @param blockPoolID
   *          the block pool id
   * @param conf
   *          Configuration instance to use.
   * @throws IOException on errors
   */
  //在数据节点 (DataNode) 上为指定的 块池 (Block Pool) 创建物理存储目录。如果目录不存在或不可用，进行必要的检查和创建
  //blockPoolID：块池的唯一标识符，HDFS 中每个命名空间对应一个块池 (Block Pool)
  public void makeBlockPoolDir(String blockPoolID,
      Configuration conf) throws IOException {

    if (conf == null) {
      conf = new HdfsConfiguration();
    }
    //如果存储类型是 PROVIDED，表示该存储位置由外部提供（如远程挂载），不需要在本地创建目录，直接记录日志并返回
    if (storageType == StorageType.PROVIDED) {
      // skip creation if the storage type is PROVIDED
      Storage.LOG.info("Skipping creating directory for block pool "
          + blockPoolID + " for PROVIDED storage location " + this);
      return;
    }

    LocalFileSystem localFS = FileSystem.getLocal(conf);
    FsPermission permission = new FsPermission(conf.get(
        DFSConfigKeys.DFS_DATANODE_DATA_DIR_PERMISSION_KEY,
        DFSConfigKeys.DFS_DATANODE_DATA_DIR_PERMISSION_DEFAULT));
    File data = new File(getBpURI(blockPoolID, Storage.STORAGE_DIR_CURRENT));
    try {
      DiskChecker.checkDir(localFS, new Path(data.toURI()), permission);
    } catch (IOException e) {
      DataStorage.LOG.warn("Invalid directory in: " + data.getCanonicalPath() +
          ": " + e.getMessage());
    }
  }

  @Override  // Checkable
  public VolumeCheckResult check(CheckContext context) throws IOException {
    // assume provided storage locations are always healthy,
    // and check only for local storages.
    if (storageType != StorageType.PROVIDED) {
      DiskChecker.checkDir(
          context.localFileSystem,
          new Path(baseURI),
          context.expectedPermission);
    }
    return VolumeCheckResult.HEALTHY;
  }

  /**
   * Class to hold the parameters for running a {@link #check}.
   */
  public static final class CheckContext {
    private final LocalFileSystem localFileSystem;
    private final FsPermission expectedPermission;

    public CheckContext(LocalFileSystem localFileSystem,
                        FsPermission expectedPermission) {
      this.localFileSystem = localFileSystem;
      this.expectedPermission = expectedPermission;
    }
  }
}
