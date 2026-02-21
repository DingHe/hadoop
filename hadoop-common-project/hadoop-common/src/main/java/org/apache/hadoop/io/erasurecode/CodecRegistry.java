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
package org.apache.hadoop.io.erasurecode;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.erasurecode.rawcoder.NativeRSRawErasureCoderFactory;
import org.apache.hadoop.io.erasurecode.rawcoder.NativeXORRawErasureCoderFactory;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureCoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class registers all coder implementations.
 *
 * {@link CodecRegistry} maps codec names to coder factories. All coder
 * factories are dynamically identified and loaded using ServiceLoader.
 */
// CodecRegistry 是 HDFS 纠删码（Erasure Coding）子系统中的核心组件，负责管理和发现系统中所有的编解码器（Codec）实现。
// 主要作用可以概括为：
// 服务发现与加载：利用 Java 的 SPI (ServiceLoader) 机制，在运行时动态扫描类路径下所有的 RawErasureCoderFactory 实现，并将其加载到内存中。
// 映射管理：建立 Codec 名称（如 rs）到 Coder 工厂列表（如 rs-native, rs-legacy, rs-java）之间的映射关系。
// 优先级排序：默认将高性能的 Native（本地 C/C++） 编解码工厂放在列表首位，确保系统默认优先选择硬件加速的实现。
// 元数据支持：为 NameNode 提供当前系统支持的编解码器清单，以便进行策略校验和 RPC 响应。
@InterfaceAudience.Private
public final class CodecRegistry {

  private static final Logger LOG =
      LoggerFactory.getLogger(CodecRegistry.class);
  // 单例对象：整个 JVM 进程中只维护一个注册表实例。
  private static CodecRegistry instance = new CodecRegistry();

  public static CodecRegistry getInstance() {
    return instance;
  }
  // 核心索引：键是 Codec 名称（如 "rs"），值是该算法下所有可用工厂的列表（按优先级排序）
  private Map<String, List<RawErasureCoderFactory>> coderMap;
  // 名称映射缓存：键是 Codec 名称，值是对应所有 Coder 实现名称的字符串数组（如 ["isa-l", "java"]）。
  private Map<String, String[]> coderNameMap;

  // Protobuffer 2.5.0 doesn't support map<String, String[]> type well, so use
  // the compact value instead
  // 紧凑版映射：将 Coder 名称列表拼接成以逗号分隔的字符串。注释说明：这是为了解决旧版 Protobuf 对数组类型 map 支持不佳的问题。
  private HashMap<String, String> coderNameCompactMap;

  private CodecRegistry() {
    coderMap = new HashMap<>();
    coderNameMap = new HashMap<>();
    coderNameCompactMap = new HashMap<>();
    // 会扫描 META-INF/services/ 目录下的配置文件，自动识别第三方或内置的 EC 扩展。
    final ServiceLoader<RawErasureCoderFactory> coderFactories =
        ServiceLoader.load(RawErasureCoderFactory.class);
    updateCoders(coderFactories);
  }

  /**
   * Update coderMap and coderNameMap with iterable type of coder factories.
   * @param coderFactories
   */
  // 注册表的逻辑核心
  @VisibleForTesting
  void updateCoders(Iterable<RawErasureCoderFactory> coderFactories) {
    for (RawErasureCoderFactory coderFactory : coderFactories) {
      //历加载到的工厂
      String codecName = coderFactory.getCodecName();
      List<RawErasureCoderFactory> coders = coderMap.get(codecName);
      if (coders == null) {
        coders = new ArrayList<>();
        coders.add(coderFactory);
        coderMap.put(codecName, coders);
        LOG.debug("Codec registered: codec = {}, coder = {}",
            coderFactory.getCodecName(), coderFactory.getCoderName());
      } else {
        // 如果两个工厂的 CoderName 相同，则记录错误并跳过。
        Boolean hasConflit = false;
        for (RawErasureCoderFactory coder : coders) {
          if (coder.getCoderName().equals(coderFactory.getCoderName())) {
            hasConflit = true;
            LOG.error("Coder {} cannot be registered because its coder name " +
                "{} has conflict with {}", coderFactory.getClass().getName(),
                coderFactory.getCoderName(), coder.getClass().getName());
            break;
          }
        }
        if (!hasConflit) {
          // set native coders as default if user does not
          // specify a fallback order
          // 优先级排序：如果工厂是 NativeRSRawErasureCoderFactory 或 NativeXORRawErasureCoderFactory（基于 ISA-L 等库），
          // 则将其插入到列表的 0 号位（最前面）；普通的 Java 实现则加在末尾。
          if (coderFactory instanceof NativeRSRawErasureCoderFactory ||
                  coderFactory instanceof NativeXORRawErasureCoderFactory) {
            coders.add(0, coderFactory);
          } else {
            coders.add(coderFactory);
          }
          LOG.debug("Codec registered: codec = {}, coder = {}",
              coderFactory.getCodecName(), coderFactory.getCoderName());
        }
      }
    }

    // update coderNameMap accordingly
    coderNameMap.clear();
    for (Map.Entry<String, List<RawErasureCoderFactory>> entry :
        coderMap.entrySet()) {
      String codecName = entry.getKey();
      List<RawErasureCoderFactory> coders = entry.getValue();
      coderNameMap.put(codecName, coders.stream().
          map(RawErasureCoderFactory::getCoderName).
          collect(Collectors.toList()).toArray(new String[0]));
      coderNameCompactMap.put(codecName, coders.stream().
          map(RawErasureCoderFactory::getCoderName)
          .collect(Collectors.joining(", ")));
    }
  }

  /**
   * Get all coder names of the given codec.
   * @param codecName the name of codec
   * @return an array of all coder names, null if not exist
   */
  public String[] getCoderNames(String codecName) {
    String[] coderNames = coderNameMap.get(codecName);
    return coderNames;
  }

  /**
   * Get all coder factories of the given codec.
   * @param codecName the name of codec
   * @return a list of all coder factories, null if not exist
   */
  public List<RawErasureCoderFactory> getCoders(String codecName) {
    List<RawErasureCoderFactory> coders = coderMap.get(codecName);
    return coders;
  }

  /**
   * Get all codec names.
   * @return a set of all codec names
   */
  public Set<String> getCodecNames() {
    return coderMap.keySet();
  }

  /**
   * Get a specific coder factory defined by codec name and coder name.
   * @param codecName name of the codec
   * @param coderName name of the coder
   * @return the specific coder, null if not exist
   */
  public RawErasureCoderFactory getCoderByName(
      String codecName, String coderName) {
    List<RawErasureCoderFactory> coders = getCoders(codecName);

    // find the RawErasureCoderFactory with the name of coderName
    for (RawErasureCoderFactory coder : coders) {
      if (coder.getCoderName().equals(coderName)) {
        return coder;
      }
    }
    return null;
  }

  /**
   * Get all codec names and their corresponding coder list.
   * @return a map of all codec names, and their corresponding code list
   * separated by ','.
   */
  public Map<String, String> getCodec2CoderCompactMap() {
    return coderNameCompactMap;
  }
}
