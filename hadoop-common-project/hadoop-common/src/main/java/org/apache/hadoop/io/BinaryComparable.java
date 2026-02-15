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

package org.apache.hadoop.io;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/** 用于比较二进制数据的抽象类，主要用于实现二进制数据的排序、比较和哈希计算。它实现了 Comparable<BinaryComparable> 接口，允许对象按字节序进行比较
 * Interface supported by {@link org.apache.hadoop.io.WritableComparable}
 * types supporting ordering/permutation by a representative set of bytes.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class BinaryComparable implements Comparable<BinaryComparable> {

  /**
   * Return n st bytes 0..n-1 from {#getBytes()} are valid.
   *
   * @return length.
   */
  public abstract int getLength(); // 返回有效字节长度

  /**
   * Return representative byte array for this instance.
   *
   * @return getBytes.
   */
  public abstract byte[] getBytes(); // 返回字节数组

  /**
   * Compare bytes from {#getBytes()}.
   * @see org.apache.hadoop.io.WritableComparator#compareBytes(byte[],int,int,byte[],int,int)
   */
  @Override
  public int compareTo(BinaryComparable other) {
    if (this == other)  // 同一个对象，直接返回相等
      return 0;
    return WritableComparator.compareBytes(getBytes(), 0, getLength(),
             other.getBytes(), 0, other.getLength());  //比较当前对象的字节数组和另一个对象的字节数组
  }

  /**
   * Compare bytes from {#getBytes()} to those provided.
   *  允许与普通的字节数组进行比较，灵活处理不同来源的数据
   * @param other other.
   * @param off off.
   * @param len len.
   * @return compareBytes.
   */
  public int compareTo(byte[] other, int off, int len) {
    return WritableComparator.compareBytes(getBytes(), 0, getLength(),
             other, off, len);
  }

  /**
   * Return true if bytes from {#getBytes()} match.
   */
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof BinaryComparable))
      return false;
    BinaryComparable that = (BinaryComparable)other;
    if (this.getLength() != that.getLength())
      return false;
    return this.compareTo(that) == 0;
  }

  /**
   * Return a hash of the bytes returned from {#getBytes()}.
   * @see org.apache.hadoop.io.WritableComparator#hashBytes(byte[],int)
   */
  @Override
  public int hashCode() {
    return WritableComparator.hashBytes(getBytes(), getLength());
  }

}
