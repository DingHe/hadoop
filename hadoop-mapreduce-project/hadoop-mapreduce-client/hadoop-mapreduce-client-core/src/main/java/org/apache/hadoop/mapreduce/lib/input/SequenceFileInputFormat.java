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

package org.apache.hadoop.mapreduce.lib.input;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/** An {@link InputFormat} for {@link SequenceFile}s. */
// Hadoop MapReduce 中专用于读取 SequenceFile 格式文件的 InputFormat
// SequenceFile 是一种 Hadoop 特有的扁平化 (flat)、面向记录 (record-oriented) 的文件格式，它将键值对序列化成二进制格式存储。与普通的文本文件（如 TextInputFormat 处理的文件）相比，SequenceFile 具有以下主要优势：
// 高效 I/O： 二进制存储比文本存储更节省空间，并且解析速度更快。
//支持数据类型： 它能够存储任意实现了 Hadoop Writable 接口的键值对类型（而不仅仅是 LongWritable 和 Text）。
// 可切分性 (Splittability) 保证： SequenceFile 包含同步标记 (SYNC_INTERVAL)。这些标记使得 MapReduce 能够可靠地将文件切分成独立的 InputSplit，即使文件经过压缩，也可以确保切分到块边界而不丢失记录。
// 因此，SequenceFileInputFormat 的作用就是：提供一种机制，使得 MapReduce 作业能够高效、可靠地以键值对的方式读取存储在 SequenceFile 和 MapFile 中的二进制数据。
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SequenceFileInputFormat<K, V> extends FileInputFormat<K, V> {
  // 创建记录读取器
  @Override
  public RecordReader<K, V> createRecordReader(InputSplit split,
                                               TaskAttemptContext context
                                               ) throws IOException {
    // 这个特定的 RecordReader 知道如何打开 Split 对应的 SequenceFile 部分，跳到正确的同步标记，并按 SequenceFile 的键值对格式读取记录
    return new SequenceFileRecordReader<K,V>();
  }
  //返回 SequenceFile.SYNC_INTERVAL。这是 SequenceFile 文件格式中定义的同步标记之间的固定字节间隔（通常是 1MB 或 4MB）。
  // 这个值是 SequenceFile 可切分的最小单位
  @Override
  protected long getFormatMinSplitSize() {
    return SequenceFile.SYNC_INTERVAL;
  }
  // 获取输入文件的状态列表
  //重写自 FileInputFormat 的方法，目的是为了特殊处理 MapFile 格式
  @Override
  protected List<FileStatus> listStatus(JobContext job
                                        )throws IOException {

    List<FileStatus> files = super.listStatus(job);
    int len = files.size();
    for(int i=0; i < len; ++i) {
      //识别目录： 如果遇到的是一个目录（file.isDirectory()），
      // 则假定它是一个 MapFile 结构（MapFile 在文件系统中表现为一个目录，内部包含 data 和 index 文件）
      FileStatus file = files.get(i);
      if (file.isDirectory()) {     // it's a MapFile
        Path p = file.getPath();
        FileSystem fs = p.getFileSystem(job.getConfiguration());
        // use the data file
        files.set(i, fs.getFileStatus(new Path(p, MapFile.DATA_FILE_NAME)));
      }
    }
    return files;
  }
}

