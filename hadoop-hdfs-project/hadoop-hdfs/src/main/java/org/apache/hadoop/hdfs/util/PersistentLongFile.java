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
package org.apache.hadoop.hdfs.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.IOUtils;

/**
 * Class that represents a file on disk which persistently stores
 * a single <code>long</code> value. The file is updated atomically
 * and durably (i.e fsynced). 
 */
//于在磁盘上持久化存储一个 long 类型的值。该类通过文件系统来存储和读取该值，
// 确保文件更新操作是原子性的，并且对文件的写入是持久化的（即文件会被 fsync 操作同步）。
// 它通过文件来实现该 long 值的持久化存储，并在程序启动时加载该值。该类支持对存储值的读取与写入，保证数据一致性和持久性
@InterfaceAudience.Private
public class PersistentLongFile {
  private static final Logger LOG = LoggerFactory.getLogger(
      PersistentLongFile.class);
  //表示存储 long 值的目标文件对象。该文件用于持久化存储值，PersistentLongFile 类会根据该文件来读取和更新存储的值
  private final File file;
  // 默认值。如果文件不存在或者无法读取值时，将使用该默认值
  private final long defaultVal;
  //当前持久化的 long 值。这个值是类的核心数据，get() 方法用来读取当前的值，set() 方法用来更新该值
  private long value;
  //表示当前 value 是否已经从文件中加载过。默认值为 false，当调用 get() 方法时，如果还没有加载过数据，会从文件中读取
  private boolean loaded = false;
  
  public PersistentLongFile(File file, long defaultVal) {
    this.file = file;
    this.defaultVal = defaultVal;
  }
  
  public long get() throws IOException {
    if (!loaded) {
      value = readFile(file, defaultVal);
      loaded = true;
    }
    return value;
  }
  
  public void set(long newVal) throws IOException {
    if (value != newVal || !loaded) {
      writeFile(file, newVal);
    }
    value = newVal;
    loaded = true;
  }

  /**
   * Atomically write the given value to the given file, including fsyncing.
   *
   * @param file destination file
   * @param val value to write
   * @throws IOException if the file cannot be written
   */
  public static void writeFile(File file, long val) throws IOException {
    AtomicFileOutputStream fos = new AtomicFileOutputStream(file);
    try {
      fos.write(String.valueOf(val).getBytes(StandardCharsets.UTF_8));
      fos.write('\n');
      fos.close();
      fos = null;
    } finally {
      if (fos != null) {
        fos.abort();        
      }
    }
  }

  public static long readFile(File file, long defaultVal) throws IOException {
    long val = defaultVal;
    if (file.exists()) {
      BufferedReader br = 
          new BufferedReader(new InputStreamReader(new FileInputStream(
              file), StandardCharsets.UTF_8));
      try {
        val = Long.parseLong(br.readLine());
        br.close();
        br = null;
      } catch (NumberFormatException e) {
        throw new IOException(e);
      } finally {
        IOUtils.cleanupWithLogger(LOG, br);
      }
    }
    return val;
  }
}
