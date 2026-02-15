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
package org.apache.hadoop.hdfs.server.namenode;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Map object to serial number.
 * 
 * <p>It allows to get the serial number of an object, if the object doesn't
 * exist in the map, a new serial number increased by 1 is generated to
 * map to the object. The mapped object can also be got through the serial
 * number.
 *  主要作用是实现一个 线程安全的双向映射，用于在对象与其对应的 序列号（Serial Number） 之间建立映射关系
 * <p>The map is thread-safe.
 */
@InterfaceAudience.Private
public class SerialNumberMap<T> {
  private String name; //标识 SerialNumberMap 的名称，通常与 SerialNumberManager 枚举的名称对应
  private final int max;//序列号的 最大值，通过 bitLength 计算，max = (1 << bitLength) - 1
  private final AtomicInteger current = new AtomicInteger(1);//表示当前分配到的最大序列号，初始值为 1，序列号从 1 开始递增
  private final ConcurrentMap<T, Integer> t2i =
      new ConcurrentHashMap<T, Integer>();//对象 → 序列号 的映射
  private final ConcurrentMap<Integer, T> i2t =
      new ConcurrentHashMap<Integer, T>();

  SerialNumberMap(SerialNumberManager snm) {
    this(snm.name(), snm.getLength());
  }

  SerialNumberMap(String name, int bitLength) {
    this.name = name;
    this.max = (1 << bitLength) - 1;
  }
  //t：要获取序列号的对象，泛型类型 T

  public int get(T t) {
    if (t == null) {
      return 0;
    }
    Integer sn = t2i.get(t);
    if (sn == null) { //如果没获取到，生成新的序列号
      synchronized (this) {
        sn = t2i.get(t);
        if (sn == null) {
          sn = current.getAndIncrement();
          if (sn > max) {
            current.getAndDecrement();
            throw new IllegalStateException(name + ": serial number map is full");
          }
          Integer old = t2i.putIfAbsent(t, sn);
          if (old != null) {
            current.getAndDecrement();
            return old;
          }
          i2t.put(sn, t);
        }
      }
    }
    return sn;
  }

  public T get(int i) {
    if (i == 0) {
      return null;
    }
    T t = i2t.get(i);
    if (t == null) {
      throw new IllegalStateException(
          name + ": serial number " + i + " does not exist");
    }
    return t;
  }

  int getMax() {
    return max;
  }

  Set<Map.Entry<Integer, T>> entrySet() {
    return new HashSet<>(i2t.entrySet());
  }

  public int size() {
    return i2t.size();
  }

  @Override
  public String toString() {
    return "current=" + current + ",\n" +
           "max=" + max + ",\n  t2i=" + t2i + ",\n  i2t=" + i2t;
  }
}