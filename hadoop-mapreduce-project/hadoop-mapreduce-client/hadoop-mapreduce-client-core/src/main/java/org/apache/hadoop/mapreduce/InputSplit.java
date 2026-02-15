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

package org.apache.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.mapred.SplitLocationInfo;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;

/**
 * <code>InputSplit</code> represents the data to be processed by an 
 * individual {@link Mapper}. 
 *
 * <p>Typically, it presents a byte-oriented view on the input and is the 
 * responsibility of {@link RecordReader} of the job to process this and present
 * a record-oriented view.
 * 
 * @see InputFormat
 * @see RecordReader
 *///描述 MapReduce 作业中每个 Mapper 需要处理的输入数据片段（Split）
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class InputSplit {
  /**
   * Get the size of the split, so that the input splits can be sorted by size.
   * @return the number of bytes in the split
   * @throws IOException
   * @throws InterruptedException
   *///获取切片的大小，即当前 InputSplit 对象所代表的数据量（以字节为单位）
  public abstract long getLength() throws IOException, InterruptedException;

  /**
   * Get the list of nodes by name where the data for the split would be local.
   * The locations do not need to be serialized.
   * 
   * @return a new array of the node nodes.
   * @throws IOException
   * @throws InterruptedException
   *///获取存储该数据切片的节点信息（通常是 HDFS DataNode 节点）
  public abstract 
    String[] getLocations() throws IOException, InterruptedException;
  
  /**
   * Gets info about which nodes the input split is stored on and how it is
   * stored at each location.
   * 
   * @return list of <code>SplitLocationInfo</code>s describing how the split
   *    data is stored at each location. A null value indicates that all the
   *    locations have the data stored on disk.
   * @throws IOException
   *///获取切片的详细存储信息，包括数据在每个节点上的存储形式（如磁盘、内存、缓存）
  @Evolving
  public SplitLocationInfo[] getLocationInfo() throws IOException {
    return null;
  }
}