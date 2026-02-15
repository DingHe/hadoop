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

import java.io.Serializable;


/** 该类用于在 long 类型的 64 位中，以位操作的方式存储和解析多个字段。其核心思想是通过位偏移和位掩码，
 * 将多个值组合到一个 long 类型的记录中，类似于 C/C++ 中的 bit field，常用于高效存储、解析和修改数据
 * Bit format in a long.
 */
public class LongBitFormat implements Serializable {
  private static final long serialVersionUID = 1L;
 //主要目的是为表示位字段的枚举类型提供一个统一的规范，
 // 要求实现此接口的枚举类必须提供 getLength() 方法，返回字段所占用的 位长度
  public interface Enum {
    int getLength();
  }

  private final String NAME;//标识当前字段的名称，便于调试、日志输出或异常信息
  /** Bit offset */
  private final int OFFSET;//位偏移量，表示该字段在 long 类型的起始位置（第几位开始），若 previous 字段不为空，则等于前一个字段的起始偏移 + 前一个字段的长度
  /** Bit length */
  private final int LENGTH;//该字段所占的位数，决定了该字段的取值范围
  /** Minimum value */
  private final long MIN;//该字段的最小取值，用于校验输入值是否符合要求
  /** Maximum value */
  private final long MAX;//该字段的最大取值，计算方法为MAX = ((-1L) >>> (64 - LENGTH))，无符号右移（>>>），不保留符号位，始终用 0 填充高位，无论原始数的符号如何，结果始终为非负数
  /** Bit mask */
  private final long MASK;//位掩码，用于从 long 类型的记录中提取该字段的值，MAX << OFFSET

  public LongBitFormat(String name, LongBitFormat previous, int length,
                       long min) {
    NAME = name;
    OFFSET = previous == null? 0: previous.OFFSET + previous.LENGTH;
    LENGTH = length;
    MIN = min;
    MAX = ((-1L) >>> (64 - LENGTH));//无符号右移
    MASK = MAX << OFFSET;
  }

  /** Retrieve the value from the record. */
  //record：long 类型的输入值，通常是一个包含多个字段的二进制数据记录
  //从一个 long 类型的记录中提取（解析）特定字段的值
  public long retrieve(long record) {
    //使用掩码 (MASK) 将目标字段对应的位保留下来，其他位清零
    //将目标字段的值向右移动到最低有效位（第 0 位），得到字段的实际值
    return (record & MASK) >>> OFFSET;
  }

  /** Combine the value to the record. */
  //将一个字段的值嵌入到 long 类型的记录中，并返回更新后的记录值
  //该方法适用于将多个值编码到一个 long 类型中，按位设置其中某个字段的值
  //value：要设置的字段值，类型为 long，需要符合该字段的取值范围 (MIN 和 MAX)
  //record：目标记录，类型为 long，包含多个字段，方法会将 value 设置到对应字段
  public long combine(long value, long record) {
    if (value < MIN) {
      throw new IllegalArgumentException(
          "Illagal value: " + NAME + " = " + value + " < MIN = " + MIN);
    }
    if (value > MAX) {
      throw new IllegalArgumentException(
          "Illagal value: " + NAME + " = " + value + " > MAX = " + MAX);
    }
    //(record & ~MASK)  使用掩码 MASK 将目标字段对应的位清零，其他位保持不变
    //(value << OFFSET)  将 value 左移到目标字段的位置
    //合并新旧记录，使用按位或 | 操作，把新值嵌入到目标位置，完成字段更新
    return (record & ~MASK) | (value << OFFSET);
  }

  public long getMin() {
    return MIN;
  }

  public int getLength() {
    return LENGTH;
  }
}
