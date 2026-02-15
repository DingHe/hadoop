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

import org.apache.hadoop.util.Preconditions;
import org.apache.commons.lang3.ArrayUtils;

import java.util.Arrays;

/**
 * Counters for an enum type.
 * 
 * For example, suppose there is an enum type
 * <pre>
 * enum Fruit { APPLE, ORANGE, GRAPE }
 * </pre>
 * An {@link EnumCounters} object can be created for counting the numbers of
 * APPLE, ORANGE and GRAPE.
 *
 * @param <E> the enum type
 */
public class EnumCounters<E extends Enum<E>> {
  /** The class of the enum. */
  private final Class<E> enumClass;//存储了泛型 E 的 Class 对象，E 是枚举类型
  /** An array of longs corresponding to the enum type. */
  //存储了每个枚举常量对应的计数值。数组的大小与枚举类型的常量数目一致，每个数组元素对应一个枚举常量的计数
  private final long[] counters;

  /**构造函数用于根据传入的枚举类型 enumClass 创建一个计数器。
   * 它通过反射获取枚举类型的常量数组（enumClass.getEnumConstants()），
   * 并初始化计数器数组 counters 的大小为枚举常量的数量
   * Construct counters for the given enum constants.
   * @param enumClass the enum class of the counters.
   */
  public EnumCounters(final Class<E> enumClass) {
    final E[] enumConstants = enumClass.getEnumConstants();
    Preconditions.checkNotNull(enumConstants);
    this.enumClass = enumClass;
    this.counters = new long[enumConstants.length];
  }
  //与上一个构造函数类似，除了初始化计数器外，它还会将计数器的初始值设置为 defaultVal，
  // 通过调用 reset(long val) 方法将所有计数器初始化为该默认值
  public EnumCounters(final Class<E> enumClass, long defaultVal) {
    final E[] enumConstants = enumClass.getEnumConstants();
    Preconditions.checkNotNull(enumConstants);
    this.enumClass = enumClass;
    this.counters = new long[enumConstants.length];
    reset(defaultVal);
  }
  
  /** @return the value of counter e. */
  //e：需要获取计数值的枚举常量
  //返回指定枚举常量 e 的计数值
  public final long get(final E e) {
    return counters[e.ordinal()];
  }
  //返回计数器数组的一个副本。为了避免外部修改原始数据，使用 ArrayUtils.clone(counters) 创建并返回数组的副本
  /** @return the values of counter as a shadow copy of array*/
  public long[] asArray() {
    return ArrayUtils.clone(counters);
  }

  /** Negate all counters. 将所有计数器的值取反，即每个计数器的值乘以 -1*/
  public void negation() {
    for(int i = 0; i < counters.length; i++) {
      counters[i] = -counters[i];
    }
  }
  
  /** Set counter e to the given value. */
  public void set(final E e, final long value) {
    counters[e.ordinal()] = value;
  }

  /** Set this counters to that counters. */
  public void set(final EnumCounters<E> that) {
    for(int i = 0; i < counters.length; i++) {
      this.counters[i] = that.counters[i];
    }
  }

  /** Reset all counters to zero. */
  public void reset() {
    reset(0L);
  }

  /** Add the given value to counter e. */
  public void add(final E e, final long value) {
    counters[e.ordinal()] += value;
  }

  /** Add that counters to this counters. */
  public void add(final EnumCounters<E> that) {
    for(int i = 0; i < counters.length; i++) {
      this.counters[i] += that.counters[i];
    }
  }

  /** Subtract the given value from counter e. */
  public void subtract(final E e, final long value) {
    counters[e.ordinal()] -= value;
  }

  /** Subtract this counters from that counters. */
  public void subtract(final EnumCounters<E> that) {
    for(int i = 0; i < counters.length; i++) {
      this.counters[i] -= that.counters[i];
    }
  }
  
  /** @return the sum of all counters. */
  public long sum() {
    long sum = 0;
    for(int i = 0; i < counters.length; i++) {
      sum += counters[i];
    }
    return sum;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    } else if (!(obj instanceof EnumCounters)) {
      return false;
    }
    final EnumCounters<?> that = (EnumCounters<?>)obj;
    return this.enumClass == that.enumClass
        && Arrays.equals(this.counters, that.counters);
  }

  /**
   * Return a deep copy of EnumCounter.
   */
  public EnumCounters<E> deepCopyEnumCounter() {
    EnumCounters<E> newCounter = new EnumCounters<>(enumClass);
    newCounter.set(this);
    return newCounter;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(counters);
  }

  @Override
  public String toString() {
    final E[] enumConstants = enumClass.getEnumConstants();
    final StringBuilder b = new StringBuilder();
    for(int i = 0; i < counters.length; i++) {
      final String name = enumConstants[i].name();
      b.append(name).append("=").append(counters[i]).append(", ");
    }
    return b.substring(0, b.length() - 2);
  }

  public void reset(long val) {
    for(int i = 0; i < counters.length; i++) {
      this.counters[i] = val;
    }
  }

  public boolean allLessOrEqual(long val) {
    for (long c : counters) {
      if (c > val) {
        return false;
      }
    }
    return true;
  }

  public boolean anyGreaterOrEqual(long val) {
    for (long c: counters) {
      if (c >= val) {
        return true;
      }
    }
    return false;
  }
}
