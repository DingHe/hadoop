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

import org.apache.hadoop.hdfs.server.namenode.INodeWithAdditionalFields.PermissionStatusFormat;
import org.apache.hadoop.hdfs.util.LongBitFormat;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/** Manage name-to-serial-number maps for various string tables. */
// 用于管理和维护 字符串（如用户、用户组、扩展属性名等）与序列号之间的映射关系的类。
// 它使用枚举类型定义了不同类别的字符串映射，如用户、用户组等，并将它们编码为整数以便高效存储和操作
public enum SerialNumberManager {
  GLOBAL(), //全局序列号管理器，适用于通用字符串映射
  USER(PermissionStatusFormat.USER, AclEntryStatusFormat.NAME), //用于管理 用户（与 HDFS 文件系统中用户相关的字符串）
  GROUP(PermissionStatusFormat.GROUP, AclEntryStatusFormat.NAME),//用于管理 用户组（与 HDFS 文件系统中用户组相关的字符串）
  XATTR(XAttrFormat.NAME);//用于管理 扩展属性（HDFS 支持的自定义扩展属性名）
  //存储了 SerialNumberManager 枚举的所有实例，方便在静态代码块中初始化和遍历
  private static final SerialNumberManager[] values = values();
  //各类别序列号最大占用位数，由 Integer.numberOfLeadingZeros(values.length) 计算
  private static final int maxEntryBits;
  //个序列号最大值，计算方式为 (1 << maxEntryBits) - 1，即 2^maxEntryBits - 1
  private static final int maxEntryNumber;
  //用于生成序列号掩码的位数，即 Integer.SIZE - maxEntryBits，用于区分不同 SerialNumberManager 类型的序列号
  private static final int maskBits;
  //用于存储 字符串到序列号 的映射，SerialNumberMap 是一个泛型映射类
  private SerialNumberMap<String> serialMap;
  //该管理器中 序列号所占的位数，默认为 32 位
  private int bitLength = Integer.SIZE;

  static {
    maxEntryBits = Integer.numberOfLeadingZeros(values.length);
    maxEntryNumber = (1 << maxEntryBits) - 1;
    maskBits = Integer.SIZE - maxEntryBits;
    for (SerialNumberManager snm : values) {
      // account for string table mask bits.
      snm.updateLength(maxEntryBits);
      snm.serialMap = new SerialNumberMap<String>(snm);
      FSDirectory.LOG.info(snm + " serial map: bits=" + snm.getLength() +
          " maxEntries=" + snm.serialMap.getMax());
    }
  }

  SerialNumberManager(LongBitFormat.Enum... elements) {
    // compute the smallest bit length registered with the serial manager.
    for (LongBitFormat.Enum element : elements) {
      updateLength(element.getLength());
    }
  }

  int getLength() {
    return bitLength;
  }
  //maxLength：当前字段需要的位数
  //更新 bitLength，确保该 SerialNumberManager 使用最小可能的位数存储数据
  private void updateLength(int maxLength) {
    bitLength = Math.min(bitLength, maxLength);
  }
  //u：需要转换为序列号的字符串
  //该方法在将 用户名、用户组或扩展属性名 转换为 序列号 时使用
  public int getSerialNumber(String u) {
    return serialMap.get(u);
  }
  //id：需要解析为字符串的序列号
  //根据序列号在 serialMap 中查找并返回对应的字符串
  public String getString(int id) {
    return serialMap.get(id);
  }
  //如果提供了 stringTable，则从中查找字符串，否则使用默认映射
  public String getString(int id, StringTable stringTable) {
    return (stringTable != null)
        ? stringTable.get(this, id) : getString(id);
  }
  //bits：掩码所需的位数
  private int getMask(int bits) {
    return ordinal() << (Integer.SIZE - bits);
  }

  private static int getMaskBits() {
    return maskBits;
  }

  private int size() {
    return serialMap.size();
  }

  private Iterable<Entry<Integer, String>> entrySet() {
    return serialMap.entrySet();
  }

  // returns snapshot of current values for a save.
  public static StringTable getStringTable() {
    // approximate size for capacity.
    int size = 0;
    for (final SerialNumberManager snm : values) {
      size += snm.size();
    }
    int tableMaskBits = getMaskBits();
    StringTable map = new StringTable(size, tableMaskBits);
    for (final SerialNumberManager snm : values) {
      final int mask = snm.getMask(tableMaskBits);
      for (Entry<Integer, String> entry : snm.entrySet()) {
        map.put(entry.getKey() | mask, entry.getValue());
      }
    }
    return map;
  }

  // returns an empty table for load.
  public static StringTable newStringTable(int size, int bits) {
    if (bits > maskBits) {
      throw new IllegalArgumentException(
        "String table bits " + bits + " > " + maskBits);
    }
    return new StringTable(size, bits);
  }
  //主要用于管理 序列号（id）与字符串（String） 之间的映射关系
  public static class StringTable implements Iterable<Entry<Integer, String>> {
    //用于存储字符串的 掩码位数，表示该 StringTable 中序列号的有效位数
    private final int tableMaskBits;
    //存储 序列号 → 字符串 映射
    private final Map<Integer,String> map;
    //size：预估的字符串映射的最大容量
    //oadingMaskBits：表的掩码位数
    private StringTable(int size, int loadingMaskBits) {
      this.tableMaskBits = loadingMaskBits;
      this.map = new HashMap<>(size);
    }
    //snm：指定的 SerialNumberManager 实例，表示哪个字符串表
    //id：需要查找的序列号
    private String get(SerialNumberManager snm, int id) {
      if (tableMaskBits != 0) {
        if (id > maxEntryNumber) {
          throw new IllegalStateException(
              "serial id " + id + " > " + maxEntryNumber);
        }
        id |= snm.getMask(tableMaskBits);
      }
      return map.get(id);
    }

    public void put(int id, String str) {
      map.put(id, str);
    }

    public Iterator<Entry<Integer, String>> iterator() {
      return map.entrySet().iterator();
    }

    public int size() {
      return map.size();
    }

    public int getMaskBits() {
      return tableMaskBits;
    }
  }
}