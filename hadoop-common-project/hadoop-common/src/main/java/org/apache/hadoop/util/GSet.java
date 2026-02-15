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
package org.apache.hadoop.util;

import java.util.Collection;

import org.apache.hadoop.classification.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link GSet} is set,
 * which supports the {@link #get(Object)} operation.
 * The {@link #get(Object)} operation uses a key to lookup an element.
 * 
 * Null element is not supported.
 * 
 * @param <K> The type of the keys.
 * @param <E> The type of the elements, which must be a subclass of the keys.
 */
@InterfaceAudience.Private
public interface GSet<K, E extends K> extends Iterable<E> {
  Logger LOG = LoggerFactory.getLogger(GSet.class);

  /** 返回 GSet 集合的大小，即集合中元素的个数
   * @return The size of this set.
   */
  int size();

  /** 如果集合中包含与给定 key 相等的元素，返回 true；否则返回 false
   * Does this set contain an element corresponding to the given key?
   * @param key The given key.
   * @return true if the given key equals to a stored element.
   *         Otherwise, return false.
   * @throws NullPointerException if key == null.
   */
  boolean contains(K key);

  /** 返回与给定 key 相等的元素（即存储在集合中的元素）；如果不存在返回 null
   * Return the stored element which is equal to the given key.
   * This operation is similar to {@link java.util.Map#get(Object)}.
   * @param key The given key.
   * @return The stored element if it exists.
   *         Otherwise, return null.
   * @throws NullPointerException if key == null.
   */
  E get(K key);

  /**
   * Add/replace an element.
   * If the element does not exist, add it to the set.
   * Otherwise, replace the existing element.
   *
   * Note that this operation
   * is similar to {@link java.util.Map#put(Object, Object)}
   * but is different from {@link java.util.Set#add(Object)}
   * which does not replace the existing element if there is any.
   * 添加一个元素到集合中，如果集合中已经有与 element 相等的元素，替换掉该元素并返回原来的元素。如果没有相同元素，则将 element 添加到集合中并返回 null
   * @param element The element being put.
   * @return the previous stored element if there is any.
   *         Otherwise, return null.
   * @throws NullPointerException if element == null.
   */
  E put(E element);

  /** 根据给定的 key 移除集合中对应的元素。如果移除成功，则返回被移除的元素；否则返回 null
   * Remove the element corresponding to the given key. 
   * This operation is similar to {@link java.util.Map#remove(Object)}.
   * @param key The key of the element being removed.
   * @return If such element exists, return it.
   *         Otherwise, return null. 
    * @throws NullPointerException if key == null.
  */
  E remove(K key);

  /** 清空集合中的所有元素
   * Clear the set.
   */
  void clear();

  /**
   * Returns a {@link Collection} view of the values contained in this set.
   * The collection is backed by the set, so changes to the set are
   * reflected in the collection, and vice-versa.
   * 返回集合中元素的视图
   * @return the collection of values.
   */
  Collection<E> values();
}
