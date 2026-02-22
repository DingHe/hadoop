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
package org.apache.hadoop.io.erasurecode.rawcoder;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.erasurecode.ErasureCodeConstants;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;

/**
 * A raw coder factory for the new raw Reed-Solomon coder in Java.
 */
// HDFS 纠删码体系中纯 Java 版 Reed-Solomon（RS）算法的入口点
// 主要作用是提供基于 Java 实现的 RS 编解码能力：
// 默认实现提供者：当 Hadoop 环境中没有安装本地驱动库（如 Intel ISA-L）或者 CPU 不支持某些指令集时，HDFS 会回退到使用这个类来创建编解码器。
// 跨平台兼容性：由于它是纯 Java 编写的，因此可以在任何运行 JVM 的系统上运行，无需担忧动态链接库（.so 或 .dll）的兼容性问题。
// 实例生成器：它专门负责将 RSRawEncoder（编码器）和 RSRawDecoder（解码器）这两个具体的功能组件与底层的数学配置（如 $k$ 个数据块，$m$ 个校验块）关联起来。
@InterfaceAudience.Private
public class RSRawErasureCoderFactory implements RawErasureCoderFactory {

  public static final String CODER_NAME = "rs_java";

  @Override
  public RawErasureEncoder createEncoder(ErasureCoderOptions coderOptions) {
    return new RSRawEncoder(coderOptions);
  }

  @Override
  public RawErasureDecoder createDecoder(ErasureCoderOptions coderOptions) {
    return new RSRawDecoder(coderOptions);
  }

  @Override
  public String getCoderName() {
    return CODER_NAME;
  }

  @Override
  public String getCodecName() {
    return ErasureCodeConstants.RS_CODEC_NAME;
  }
}
