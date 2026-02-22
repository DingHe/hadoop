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
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;

/**
 * Raw erasure coder factory that can be used to create raw encoder and decoder.
 * It helps in configuration since only one factory class is needed to be
 * configured.
 */
// 主要作用是作为工厂（Factory Pattern），将具体的纠删码算法实现（如 Reed-Solomon, XOR）与上层调用逻辑解耦：
// 统一管理：它将“编码器（Encoder）”和“解码器（Decoder）”的创建逻辑封装在一起。由于 HDFS 需要同时进行数据的写入（编码）和丢失恢复（解码），通过一个工厂类管理可以确保两者使用的算法逻辑、数学域（Finite Field）等完全匹配。
// 支持插件化：它是上一条回复中提到的 CodecRegistry 所管理的最小单位。通过实现这个接口，开发者可以引入不同的底层实现（如基于 Java 的实现、基于 Intel ISA-L 库的 Native 实现等）
// 配置简化：在 HDFS 的配置中，只需要指定一个工厂类，系统就能同时获得编解码能力，而不需要分别配置。

@InterfaceAudience.Private
public interface RawErasureCoderFactory {

  /**
   * Create raw erasure encoder.
   * @param coderOptions the options used to create the encoder
   * @return raw erasure encoder
   */
  // 创建原始编码器。
  // 根据提供的选项（如数据块数量、校验块数量等），实例化一个专门负责计算校验数据（Parity Data）的对象。
  RawErasureEncoder createEncoder(ErasureCoderOptions coderOptions);

  /**
   * Create raw erasure decoder.
   * @param coderOptions the options used to create the encoder
   * @return raw erasure decoder
   */
  // 创建原始解码器。
  // 实例化一个专门负责在数据丢失或损坏时，利用剩余数据块和校验块还原（Recover）出原始数据的对象。
  RawErasureDecoder createDecoder(ErasureCoderOptions coderOptions);

  /**
   * Get the name of the coder.
   * @return coder name
   */
  // 获取具体实现名称。
  // 用于区分同一算法下的不同底层方案。例如，对于 RS 算法，可能返回 "rs_java"（纯 Java 实现）或 "rs_native"（C++ 本地加速实现）。
  String getCoderName();

  /**
   * Get the name of its codec.
   * @return codec name
   */
  // 获取算法族（Codec）名称。
  // 返回该工厂所属的基础算法类别。例如，所有 Reed-Solomon 相关的工厂都会返回 "rs"，所有 XOR 相关的工厂返回 "xor"。
  String getCodecName();
}
