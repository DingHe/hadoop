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

import org.apache.hadoop.hdfs.util.EnumCounters;

/** Quota types. */
public enum Quota {
  //表示“命名空间使用量”，即HDFS中文件系统对象的数量。每个文件、目录和链接等都会占用命名空间，
  // 因此它用于限制HDFS中可以创建的文件或目录的数量
  /** The namespace usage, i.e. the number of name objects. */
  NAMESPACE,
  /** The storage space usage in bytes including replication. */
  // 表示“存储空间使用量”，即HDFS中存储数据的总字节数，包括数据的副本（即每个数据块的多个副本）。
  // 存储空间限制用于控制HDFS集群上文件的实际存储容量
  STORAGESPACE;

  /** Counters for quota counts. */
  //Counts 类继承了 EnumCounters<Quota>，用于存储每种配额类型（NAMESPACE 和 STORAGESPACE）的实际使用情况。
  // 它是一个计数器类，用来跟踪和存储配额的实际使用值
  public static class Counts extends EnumCounters<Quota> {
    /** @return a new counter with the given namespace and storagespace usages. */
    public static Counts newInstance(long namespace, long storagespace) {
      final Counts c = new Counts();
      c.set(NAMESPACE, namespace);
      c.set(STORAGESPACE, storagespace);
      return c;
    }

    public static Counts newInstance() {
      return newInstance(0, 0);
    }

    Counts() {
      super(Quota.class);
    }
  }

  /**
   * Is quota violated?
   * The quota is violated if quota is set and usage &gt; quota.
   */
  //quota：配额值，表示该资源（命名空间或存储空间）的最大允许使用量
  //usage：实际的资源使用量
  //该方法用于检查是否违反了配额
  public static boolean isViolated(final long quota, final long usage) {
    return quota >= 0 && usage > quota;
  }

  /**该方法用于检查在增加 delta 量后的资源使用量是否违反了配额
   * Is quota violated?
   * The quota is violated if quota is set, delta &gt; 0 and
   * usage + delta &gt; quota.
   */
  static boolean isViolated(final long quota, final long usage,
      final long delta) {
    return quota >= 0 && delta > 0 && usage > quota - delta;
  }
}