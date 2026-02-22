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
import org.apache.hadoop.io.erasurecode.ECChunk;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * An abstract raw erasure encoder that's to be inherited by new encoders.
 *
 * Raw erasure coder is part of erasure codec framework, where erasure coder is
 * used to encode/decode a group of blocks (BlockGroup) according to the codec
 * specific BlockGroup layout and logic. An erasure coder extracts chunks of
 * data from the blocks and can employ various low level raw erasure coders to
 * perform encoding/decoding against the chunks.
 *
 * To distinguish from erasure coder, here raw erasure coder is used to mean the
 * low level constructs, since it only takes care of the math calculation with
 * a group of byte buffers.
 *
 * Note it mainly provides encode() calls, which should be stateless and may be
 * made thread-safe in future.
 */
// 定义了所有底层原始编码器的行为标准。
// 数学运算的抽象：与高层的 ErasureCoder 不同，Raw 编码器不关心 HDFS 的块（Block）逻辑，它只专注于纯粹的数学计算。它接收一组字节缓冲区（Byte Buffers）作为输入，计算并输出校验数据。
// 统一接口：它为不同的底层实现（如 Java 矩阵运算或 Native 的 ISA-L 指令集）提供了统一的 API 调用方式。
// 状态与线程安全：该类设计为无状态的（Stateless），旨在未来能够支持线程安全并发调用。
// 内存模式切换：它负责协调**堆内内存（on-heap/byte arrays）和直接内存（direct buffers）**之间的逻辑切换。

@InterfaceAudience.Private
public abstract class RawErasureEncoder {
  // 编码器配置项。
  // 存储了关键的策略信息：如数据块数量（$k$）、校验块数量（$m$）、是否允许修改输入、是否开启冗余调试日志等。
  private final ErasureCoderOptions coderOptions;

  public RawErasureEncoder(ErasureCoderOptions coderOptions) {
    this.coderOptions = coderOptions;
  }

  /**
   * Encode with inputs and generates outputs.
   *
   * Note, for both inputs and outputs, no mixing of on-heap buffers and direct
   * buffers are allowed.
   *
   * If the coder option ALLOW_CHANGE_INPUTS is set true (false by default), the
   * content of input buffers may change after the call, subject to concrete
   * implementation. Anyway the positions of input buffers will move forward.
   *
   * @param inputs input buffers to read data from. The buffers' remaining will
   *               be 0 after encoding
   * @param outputs output buffers to put the encoded data into, ready to read
   *                after the call
   * @throws IOException if the encoder is closed.
   */
  // 负责将一组数据缓冲区（Inputs）转换为一组校验缓冲区（Outputs）。它的核心逻辑在于内存类型的自动适配和缓冲区状态的管理。
  public void encode(ByteBuffer[] inputs, ByteBuffer[] outputs)
      throws IOException {
    // 根据传入的输入输出缓冲区数组，创建一个临时的状态快照对象。
    // 构造时进行一系列校验，例如检查输入/输出数组长度是否符合 EC 策略（如 6+3），以及确保这些缓冲区要么全是堆外内存（DirectBuffer），要么全是堆内存（HeapBuffer），不允许混用。
    ByteBufferEncodingState bbeState = new ByteBufferEncodingState(
        this, inputs, outputs);
    // 标记当前缓冲区是否为直接内存。这决定了后续是调用 Java 矩阵逻辑还是调用 Native（如 ISA-L）库。
    boolean usingDirectBuffer = bbeState.usingDirectBuffer;
    int dataLen = bbeState.encodeLength;
    if (dataLen == 0) {
      return;
    }
    // 在执行计算前，手动保存每个输入缓冲区的当前 position 指针。
    int[] inputPositions = new int[inputs.length];
    for (int i = 0; i < inputPositions.length; i++) {
      if (inputs[i] != null) {
        inputPositions[i] = inputs[i].position();
      }
    }
    // 路由到具体算法实现
    if (usingDirectBuffer) {
      doEncode(bbeState);
    } else {
      ByteArrayEncodingState baeState = bbeState.convertToByteArrayState();
      doEncode(baeState);
    }
    // 关键的收尾工作。
    // 将每个输入缓冲区的 position 向后移动 dataLen 个字节。
    for (int i = 0; i < inputs.length; i++) {
      if (inputs[i] != null) {
        // dataLen bytes consumed
        inputs[i].position(inputPositions[i] + dataLen);
      }
    }
  }

  /**
   * Perform the real encoding work using direct ByteBuffer.
   * @param encodingState the encoding state.
   * @throws IOException raised on errors performing I/O.
   */
  protected abstract void doEncode(ByteBufferEncodingState encodingState)
      throws IOException;

  /**
   * Encode with inputs and generates outputs. More see above.
   *
   * @param inputs input buffers to read data from
   * @param outputs output buffers to put the encoded data into, read to read
   *                after the call
   * @throws IOException raised on errors performing I/O.
   */
  // 专门处理原生字节数组的编码方法。
  public void encode(byte[][] inputs, byte[][] outputs) throws IOException {
    ByteArrayEncodingState baeState = new ByteArrayEncodingState(
        this, inputs, outputs);

    int dataLen = baeState.encodeLength;
    if (dataLen == 0) {
      return;
    }

    doEncode(baeState);
  }

  /**
   * Perform the real encoding work using bytes array, supporting offsets
   * and lengths.
   * @param encodingState the encoding state
   * @throws IOException  raised on errors performing I/O.
   */
  protected abstract void doEncode(ByteArrayEncodingState encodingState)
      throws IOException;

  /**
   * Encode with inputs and generates outputs. More see above.
   *
   * @param inputs input buffers to read data from
   * @param outputs output buffers to put the encoded data into, read to read
   *                after the call
   * @throws IOException if the encoder is closed.
   */
  // 高级包装方法，它将 HDFS 内部的 ECChunk 对象转换为 ByteBuffer 数组后，再调用前面的 encode 逻辑。
  public void encode(ECChunk[] inputs, ECChunk[] outputs) throws IOException {
    ByteBuffer[] newInputs = ECChunk.toBuffers(inputs);
    ByteBuffer[] newOutputs = ECChunk.toBuffers(outputs);
    encode(newInputs, newOutputs);
  }
  // 分别返回数据块数量（$k$）、校验块数量（$m$）和总块数（$k+m$）。
  public int getNumDataUnits() {
    return coderOptions.getNumDataUnits();
  }

  public int getNumParityUnits() {
    return coderOptions.getNumParityUnits();
  }

  public int getNumAllUnits() {
    return coderOptions.getNumAllUnits();
  }

  /**
   * Tell if direct buffer is preferred or not. It's for callers to
   * decide how to allocate coding chunk buffers, using DirectByteBuffer or
   * bytes array. It will return false by default.
   * @return true if native buffer is preferred for performance consideration,
   * otherwise false.
   */
  // 默认返回 false。子类如果是 Native 实现（如 ISA-L），通常会重写此方法返回 true，以减少内存拷贝开销。
  public boolean preferDirectBuffer() {
    return false;
  }

  /**
   * Allow change into input buffers or not while perform encoding/decoding.
   * @return true if it's allowed to change inputs, false otherwise
   */
  // 标识编码过程中是否允许修改输入缓冲区的内容。出于性能考虑，某些算法可能会在原地处理数据。
  public boolean allowChangeInputs() {
    return coderOptions.allowChangeInputs();
  }

  /**
   * Allow to dump verbose info during encoding/decoding.
   * @return true if it's allowed to do verbose dump, false otherwise.
   */
  // 控制是否在计算过程中打印详细的数学运算过程（通常用于故障排查）。
  public boolean allowVerboseDump() {
    return coderOptions.allowVerboseDump();
  }

  /**
   * Should be called when release this coder. Good chance to release encoding
   * or decoding buffers
   */
  // 用于释放资源。虽然在该抽象类中是空实现，但对于需要管理 C++ 指针的 Native 编码器来说，重写此方法以防止内存泄漏至关重要
  public void release() {
    // Nothing to do here.
  }
}
