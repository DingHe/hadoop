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

package org.apache.hadoop.fs;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;

/**
 * Defines the types of supported storage media. The default storage
 * medium is assumed to be DISK.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public enum StorageType {
  RAM_DISK(true, true), //表示 RAM 磁盘存储类型，即数据存储在 RAM 中
  SSD(false, false), //表示固态硬盘（Solid State Drive）存储类型
  DISK(false, false), //表示传统的硬盘存储类型，即机械硬盘（HDD）
  ARCHIVE(false, false), //表示归档存储类型，通常用于长期存储不常访问的数据
  PROVIDED(false, false), //表示由用户自定义的存储类型。该类型是预定义的，但由用户提供实际的存储实现
  NVDIMM(false, true); //表示非易失性内存（Non-Volatile Dual In-line Memory Module）存储类型

  private final boolean isTransient; //表示该存储类型是否是“临时的”存储类型。如果为 true，则该存储类型为临时存储（例如，RAM_DISK）
  private final boolean isRAM;//表示该存储类型是否是基于 RAM 的存储，例如，RAM_DISK 类型就是一个基于 RAM 的存储类型

  public static final StorageType DEFAULT = DISK;//默认的存储类型，默认为 DISK

  public static final StorageType[] EMPTY_ARRAY = {};//通常用来表示没有存储类型或作为默认值

  private static final StorageType[] VALUES = values();//储所有 StorageType 枚举值的数组，通过 values() 方法获得，表示所有支持的存储类型

  StorageType(boolean isTransient, boolean isRAM) {
    this.isTransient = isTransient;
    this.isRAM = isRAM;
  }

  public boolean isTransient() {
    return isTransient;
  }

  public boolean isRAM() {
    return isRAM;
  }
  //检查当前存储类型是否支持配额。临时存储类型不支持配额，因此如果 isTransient 为 true，返回 false；否则返回 true
  public boolean supportTypeQuota() {
    return !isTransient;
  }
  //检查当前存储类型是否可以移动。临时存储类型通常不能移动
  public boolean isMovable() {
    return !isTransient;
  }

  public static List<StorageType> asList() {
    return Arrays.asList(VALUES);
  }
  //返回所有可以移动的存储类型（非临时存储类型）
  public static List<StorageType> getMovableTypes() {
    return getNonTransientTypes();
  }
  //回所有支持配额的存储类型（非临时存储类型
  public static List<StorageType> getTypesSupportingQuota() {
    return getNonTransientTypes();
  }
  //根据索引 i 返回对应的 StorageType 枚举常量
  public static StorageType parseStorageType(int i) {
    return VALUES[i];
  }

  public static StorageType parseStorageType(String s) {
    return StorageType.valueOf(StringUtils.toUpperCase(s));
  }
  //检查是否允许相同磁盘层次的存储类型。对于 DISK 和 ARCHIVE 类型，允许使用相同的磁盘层次；否则不允许
  public static boolean allowSameDiskTiering(StorageType storageType) {
    return storageType == StorageType.DISK
        || storageType == StorageType.ARCHIVE;
  }
  //返回所有非临时存储类型的列表。即筛选出 isTransient == false 的所有存储类型
  private static List<StorageType> getNonTransientTypes() {
    List<StorageType> nonTransientTypes = new ArrayList<>();
    for (StorageType t : VALUES) {
      if ( t.isTransient == false ) {
        nonTransientTypes.add(t);
      }
    }
    return nonTransientTypes;
  }

  // The configuration header for different StorageType.
  public static final String CONF_KEY_HEADER =
      "dfs.datanode.storagetype.";

  /**
   * Get the configured values for different StorageType.
   * @param conf - absolute or fully qualified path
   * @param t - the StorageType  t: 存储类型（StorageType 类型），用于指定不同的存储类型
   * @param name - the sub-name of key   配置项的子名称（String 类型），用于指定配置的具体项
   * @return the file system of the path
   */
  public static String getConf(Configuration conf,
                               StorageType t, String name) {
    return conf.get(CONF_KEY_HEADER + t.toString() + "." + name);
  }
}
