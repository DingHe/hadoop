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

package org.apache.hadoop.mapreduce.lib.join;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * This InputSplit contains a set of child InputSplits. Any InputSplit inserted
 * into this collection must have a public default constructor.
 */
//代码是 Hadoop MapReduce 库中用于连接（Join）操作的一个关键类：CompositeInputSplit
// 作用是作为 多个 InputSplit 的容器，将多个数据源的输入分块（Split）逻辑上组合成一个单一的 Split。
// 这个类主要用在 Hadoop MapReduce Join 框架 (mapreduce.lib.join) 中，特别是 Reduce-Side Join 或 CompositeInputFormat 的场景。
// 核心功能包括：
// 逻辑分组： 允许一个 Map Task 处理来自多个不同底层文件或数据源的输入分块。
// 聚合元数据： 它计算所有内部 Split 的总长度，并聚合所有 Split 的位置信息（主机名），以便于 MapReduce 调度器进行数据本地性优化。
@InterfaceAudience.Public
@InterfaceStability.Stable
public class CompositeInputSplit extends InputSplit implements Writable {
  // 当前已添加到 splits 数组中的子 InputSplit 的数量。作为添加 Split 时的计数器和索引。
  private int fill = 0;
  // 所有已添加子 InputSplit 的总长度（字节数）。用于快速返回 getLength() 的结果。
  private long totsize = 0L;
  // 存储所有被组合在一起的子 InputSplit 对象的数组。
  private InputSplit[] splits;
  private Configuration conf = new Configuration();

  public CompositeInputSplit() { }

  public CompositeInputSplit(int capacity) {
    splits = new InputSplit[capacity];
  }

  /**
   * Add an InputSplit to this collection.
   * @throws IOException If capacity was not specified during construction
   *                     or if capacity has been reached.
   */
  public void add(InputSplit s) throws IOException, InterruptedException {
    if (null == splits) {
      throw new IOException("Uninitialized InputSplit");
    }
    if (fill == splits.length) {
      throw new IOException("Too many splits");
    }
    splits[fill++] = s;
    totsize += s.getLength();
  }

  /**
   * Get ith child InputSplit.
   */
  public InputSplit get(int i) {
    return splits[i];
  }

  /**
   * Return the aggregate length of all child InputSplits currently added.
   */
  public long getLength() throws IOException {
    return totsize;
  }

  /**
   * Get the length of ith child InputSplit.
   */
  public long getLength(int i) throws IOException, InterruptedException {
    return splits[i].getLength();
  }

  /**
   * Collect a set of hosts from all child InputSplits.
   */
  public String[] getLocations() throws IOException, InterruptedException {
    HashSet<String> hosts = new HashSet<String>();
    for (InputSplit s : splits) {
      String[] hints = s.getLocations();
      if (hints != null && hints.length > 0) {
        for (String host : hints) {
          hosts.add(host);
        }
      }
    }
    return hosts.toArray(new String[hosts.size()]);
  }

  /**
   * getLocations from ith InputSplit.
   */
  public String[] getLocation(int i) throws IOException, InterruptedException {
    return splits[i].getLocations();
  }

  /**
   * Write splits in the following format.
   * {@code
   * <count><class1><class2>...<classn><split1><split2>...<splitn>
   * }
   */
  @SuppressWarnings("unchecked")
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, splits.length);
    for (InputSplit s : splits) {
      Text.writeString(out, s.getClass().getName());
    }
    for (InputSplit s : splits) {
      SerializationFactory factory = new SerializationFactory(conf);
      Serializer serializer = 
        factory.getSerializer(s.getClass());
      serializer.open((DataOutputStream)out);
      serializer.serialize(s);
    }
  }

  /**
   * {@inheritDoc}
   * @throws IOException If the child InputSplit cannot be read, typically
   *                     for failing access checks.
   */
  @SuppressWarnings("unchecked")  // Generic array assignment
  public void readFields(DataInput in) throws IOException {
    int card = WritableUtils.readVInt(in);
    if (splits == null || splits.length != card) {
      splits = new InputSplit[card];
    }
    Class<? extends InputSplit>[] cls = new Class[card];
    try {
      for (int i = 0; i < card; ++i) {
        cls[i] =
          Class.forName(Text.readString(in)).asSubclass(InputSplit.class);
      }
      for (int i = 0; i < card; ++i) {
        splits[i] = ReflectionUtils.newInstance(cls[i], null);
        SerializationFactory factory = new SerializationFactory(conf);
        Deserializer deserializer = factory.getDeserializer(cls[i]);
        deserializer.open((DataInputStream)in);
        splits[i] = (InputSplit)deserializer.deserialize(splits[i]);
      }
    } catch (ClassNotFoundException e) {
      throw new IOException("Failed split init", e);
    }
  }
}
