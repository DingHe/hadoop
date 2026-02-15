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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Input format that is a <code>CombineFileInputFormat</code>-equivalent for
 * <code>TextInputFormat</code>.
 *
 * @see CombineFileInputFormat
 */
// Hadoop MapReduce 中专门用来处理大量小文本文件的 InputFormat
  //解决小文件问题： 它继承自抽象类 CombineFileInputFormat，这意味着它会合并多个小文件或文件块，将它们打包成一个大的 CombineFileSplit，从而显著减少 Map Task 的数量，降低作业启动和管理的开销。
  //兼容文本处理： 它是 TextInputFormat 的合并版本。它确保合并后的每个 Split 仍然能够以行为单位进行读取和处理。
@InterfaceAudience.Public
@InterfaceStability.Stable
public class CombineTextInputFormat
  extends CombineFileInputFormat<LongWritable,Text> {
  // 创建记录读取器
  public RecordReader<LongWritable,Text> createRecordReader(InputSplit split,
    TaskAttemptContext context) throws IOException {
    return new CombineFileRecordReader<LongWritable,Text>(
      (CombineFileSplit)split, context, TextRecordReaderWrapper.class);
  }

  /**
   * A record reader that may be passed to <code>CombineFileRecordReader</code>
   * so that it can be used in a <code>CombineFileInputFormat</code>-equivalent
   * for <code>TextInputFormat</code>.
   *
   * @see CombineFileRecordReader
   * @see CombineFileInputFormat
   * @see TextInputFormat
   */
  private static class TextRecordReaderWrapper
    extends CombineFileRecordReaderWrapper<LongWritable,Text> {
    // this constructor signature is required by CombineFileRecordReader
    public TextRecordReaderWrapper(CombineFileSplit split,
      TaskAttemptContext context, Integer idx)
      throws IOException, InterruptedException {
      super(new TextInputFormat(), split, context, idx);
    }
  }
}
