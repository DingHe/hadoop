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

import java.io.PrintStream;
import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * A low memory footprint {@link GSet} implementation,
 * which uses an array for storing the elements
 * and linked lists for collision resolution.
 *
 * No rehash will be performed.
 * Therefore, the internal array will never be resized.
 *
 * This class does not support null element.
 *
 * This class is not thread safe.
 *
 * @param <K> Key type for looking up the elements
 * @param <E> Element type, which must be
 *       (1) a subclass of K, and
 *       (2) implementing {@link LinkedElement} interface.
 */
// 一个高性能、低内存占用的哈希集合实现，专门为处理海量元数据（如 NameNode 中的数据块 Block 管理）而设计。
// 在 HDFS NameNode 中，需要管理数以亿计的对象（如 Block、Inode）。标准的 java.util.HashSet 会为每个元素创建一个 HashMap.Node 对象，这会带来巨大的内存开销。
// LightWeightGSet 的核心设计目标是：极简内存消耗。
//消除额外对象：它不使用内部的 Entry 对象。相反，它要求存储的元素本身必须实现 LinkedElement 接口（即元素自身持有 next 指针）。
// 固定大小：初始化后数组长度固定，不进行 rehash（扩容）。这避免了扩容期间的内存抖动和性能开销。
// 链地址法：使用简单的数组 + 单向链表来解决哈希冲突。
@InterfaceAudience.Private
public class LightWeightGSet<K, E extends K> implements GSet<K, E> {
  /**
   * Elements of {@link LightWeightGSet}.
   */
  // 定义在 LightWeightGSet 集合中存储的元素。
  // 它规定了每个元素必须实现的基本操作，以便这些元素能够在集合的哈希桶中形成链表结构，从而解决哈希冲突
  // 通过让数据对象直接实现这个接口，GSet 就可以把数据对象直接串成链表，省去了包装类的内存。
  public interface LinkedElement {
    /**
     * Set the next element.
     * @param next inputNext.
     */
    // 设置当前元素的下一个元素，使得当前元素能够指向链表中的下一个元素
    void setNext(LinkedElement next);

    /** @return Get the next element. */
    //返回当前元素的下一个元素
    LinkedElement getNext();
  }
  // 定义内部数组允许的最大（$2^{30}$）和最小（1）长度，防止溢出。
  static final int MAX_ARRAY_LENGTH = 1 << 30; //prevent int overflow problem
  static final int MIN_ARRAY_LENGTH = 1;

  /**
   * An internal array of entries, which are the rows of the hash table.
   * The size must be a power of two.
   */
  //内部数组，用于存储集合中的元素，数组长度必须是 2 的幂
  protected LinkedElement[] entries;
  /** A mask for computing the array index from the hash value of an element. */
  // 哈希掩码，等于 entries.length - 1。
  // 由于数组长度是 2 的幂，通过 hashCode & hash_mask 可以快速定位索引。
  protected int hash_mask;
  /** The size of the set (not the entry array). */
  // 集合中当前存储的元素总数。
  protected int size = 0;
  /** Modification version for fail-fast.
   * @see ConcurrentModificationException
   */
  // 修改计数器，用于实现 fail-fast 机制（在迭代时如果集合被修改则抛出异常）。
  protected int modification = 0;

  // 返回一个 Collection<E> 视图，方便使用增强 for 循环。
  private Collection<E> values;

  protected LightWeightGSet() {
  }

  /**
   * @param recommended_length Recommended size of the internal array.
   */
  // 构造函数。
  // 它会调用 actualArrayLength 将推荐长度向上取整为最接近的 2 的幂。
  public LightWeightGSet(final int recommended_length) {
    final int actual = actualArrayLength(recommended_length);
    if (LOG.isDebugEnabled()) {
      LOG.debug("recommended=" + recommended_length + ", actual=" + actual);
    }
    entries = new LinkedElement[actual];
    hash_mask = entries.length - 1;
  }

  // compute actual length
  // 确保返回的值是 2 的幂且在合法范围内。
  protected static int actualArrayLength(int recommended) {
    if (recommended > MAX_ARRAY_LENGTH) {
      return MAX_ARRAY_LENGTH;
    } else if (recommended < MIN_ARRAY_LENGTH) {
      return MIN_ARRAY_LENGTH;
    } else {
      final int a = Integer.highestOneBit(recommended);
      return a == recommended? a: a << 1;
    }
  }

  @Override
  public int size() {
    return size;
  }

  // 计算 Key 对应的数组下标。
  // 使用位运算 & hash_mask 代替取模运算 %，性能更高。
  protected int getIndex(final K key) {
    return key.hashCode() & hash_mask;
  }

  protected E convert(final LinkedElement e){
    @SuppressWarnings("unchecked")
    final E r = (E)e;
    return r;
  }
  // 通过哈希定位桶，然后遍历单向链表，通过 equals 方法查找匹配的元素。
  @Override
  public E get(final K key) {
    //validate key
    if (key == null) {
      throw new NullPointerException("key == null");
    }

    //find element
    final int index = getIndex(key);
    for(LinkedElement e = entries[index]; e != null; e = e.getNext()) {
      if (e.equals(key)) {
        return convert(e);
      }
    }
    //element not found
    return null;
  }

  @Override
  public boolean contains(final K key) {
    return get(key) != null;
  }


  @Override
  public E put(final E element) {
    // validate element
    // 验证元素不为 null 且实现了 LinkedElement。
    if (element == null) {
      throw new NullPointerException("Null element is not supported.");
    }
    LinkedElement e = null;
    try {
      e = (LinkedElement)element;
    } catch (ClassCastException ex) {
      throw new HadoopIllegalArgumentException(
          "!(element instanceof LinkedElement), element.getClass()="
          + element.getClass());
    }

    // find index
    final int index = getIndex(element);

    // remove if it already exists
    // 调用 remove 检查并删除已存在的同名元素（替换逻辑）。
    final E existing = remove(index, element);

    // insert the element to the head of the linked list
    modification++;
    size++;
    e.setNext(entries[index]);
    entries[index] = e;

    return existing;
  }

  /**
   * Remove the element corresponding to the key,
   * given key.hashCode() == index.
   *
   * @param key key.
   * @param index index.
   * @return If such element exists, return it.
   *         Otherwise, return null.
   */
  protected E remove(final int index, final K key) {
    if (entries[index] == null) {
      return null;
    } else if (entries[index].equals(key)) {
      //remove the head of the linked list
      modification++;
      size--;
      final LinkedElement e = entries[index];
      entries[index] = e.getNext();
      e.setNext(null);
      return convert(e);
    } else {
      //head != null and key is not equal to head
      //search the element
      LinkedElement prev = entries[index];
      for(LinkedElement curr = prev.getNext(); curr != null; ) {
        if (curr.equals(key)) {
          //found the element, remove it
          modification++;
          size--;
          prev.setNext(curr.getNext());
          curr.setNext(null);
          return convert(curr);
        } else {
          prev = curr;
          curr = curr.getNext();
        }
      }
      //element not found
      return null;
    }
  }

  @Override
  public E remove(final K key) {
    //validate key
    if (key == null) {
      throw new NullPointerException("key == null");
    }
    return remove(getIndex(key), key);
  }

  @Override
  public Collection<E> values() {
    if (values == null) {
      values = new Values();
    }
    return values;
  }

  private final class Values extends AbstractCollection<E> {

    @Override
    public Iterator<E> iterator() {
      return LightWeightGSet.this.iterator();
    }

    @Override
    public int size() {
      return size;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean contains(Object o) {
      return LightWeightGSet.this.contains((K)o);
    }

    @Override
    public void clear() {
      LightWeightGSet.this.clear();
    }
  }

  @Override
  public Iterator<E> iterator() {
    return new SetIterator();
  }

  @Override
  public String toString() {
    final StringBuilder b = new StringBuilder(getClass().getSimpleName());
    b.append("(size=").append(size)
     .append(String.format(", %08x", hash_mask))
     .append(", modification=").append(modification)
     .append(", entries.length=").append(entries.length)
     .append(")");
    return b.toString();
  }

  /**
   * Print detailed information of this object.
   *
   * @param out out.
   */
  // 打印哈希表的详细分布情况，
  // 包括每个桶位有多少个元素，常用于分析哈希冲突是否严重。
  public void printDetails(final PrintStream out) {
    out.print(this + ", entries = [");
    for(int i = 0; i < entries.length; i++) {
      if (entries[i] != null) {
        LinkedElement e = entries[i];
        out.print("\n  " + i + ": " + e);
        for(e = e.getNext(); e != null; e = e.getNext()) {
          out.print(" -> " + e);
        }
      }
    }
    out.println("\n]");
  }
  // 返回自定义的 SetIterator。
  public class SetIterator implements Iterator<E> {
    /** The starting modification for fail-fast. */
    private int iterModification = modification;
    /** The current index of the entry array. */
    private int index = -1;
    private LinkedElement cur = null;
    private LinkedElement next = nextNonemptyEntry();
    private boolean trackModification = true;

    /** Find the next nonempty entry starting at (index + 1). */
    private LinkedElement nextNonemptyEntry() {
      for(index++; index < entries.length && entries[index] == null; index++);
      return index < entries.length? entries[index]: null;
    }

    private void ensureNext() {
      if (trackModification && modification != iterModification) {
        throw new ConcurrentModificationException("modification=" + modification
            + " != iterModification = " + iterModification);
      }
      if (next != null) {
        return;
      }
      if (cur == null) {
        return;
      }
      next = cur.getNext();
      if (next == null) {
        next = nextNonemptyEntry();
      }
    }

    @Override
    public boolean hasNext() {
      ensureNext();
      return next != null;
    }

    @Override
    public E next() {
      ensureNext();
      if (next == null) {
        throw new IllegalStateException("There are no more elements");
      }
      cur = next;
      next = null;
      return convert(cur);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void remove() {
      ensureNext();
      if (cur == null) {
        throw new IllegalStateException("There is no current element " +
            "to remove");
      }
      LightWeightGSet.this.remove((K)cur);
      iterModification++;
      cur = null;
    }

    public void setTrackModification(boolean trackModification) {
      this.trackModification = trackModification;
    }
  }
  
  /**
   * Let t = percentage of max memory.
   * Let e = round(log_2 t).
   * Then, we choose capacity = 2^e/(size of reference),
   * unless it is outside the close interval [1, 2^30].
   *
   * @param mapName mapName.
   * @param percentage percentage.
   * @return compute capacity.
   */
  // 根据 JVM 最大可用内存的一定比例（percentage）来计算最合适的集合容量。
  // 它会检测 JVM 是 32 位还是 64 位（影响指针大小），从而估算出能存放多少个引用。
  public static int computeCapacity(double percentage, String mapName) {
    return computeCapacity(Runtime.getRuntime().maxMemory(), percentage,
        mapName);
  }
  
  @VisibleForTesting
  static int computeCapacity(long maxMemory, double percentage,
      String mapName) {
    if (percentage > 100.0 || percentage < 0.0) {
      throw new HadoopIllegalArgumentException("Percentage " + percentage
          + " must be greater than or equal to 0 "
          + " and less than or equal to 100");
    }
    if (maxMemory < 0) {
      throw new HadoopIllegalArgumentException("Memory " + maxMemory
          + " must be greater than or equal to 0");
    }
    if (percentage == 0.0 || maxMemory == 0) {
      return 0;
    }
    //VM detection
    //See http://java.sun.com/docs/hotspot/HotSpotFAQ.html#64bit_detection
    final String vmBit = System.getProperty("sun.arch.data.model");

    //Percentage of max memory
    final double percentDivisor = 100.0/percentage;
    final double percentMemory = maxMemory/percentDivisor;
    
    //compute capacity
    final int e1 = (int)(Math.log(percentMemory)/Math.log(2.0) + 0.5);
    final int e2 = e1 - ("32".equals(vmBit)? 2: 3);
    final int exponent = e2 < 0? 0: e2 > 30? 30: e2;
    final int c = 1 << exponent;

    LOG.info("Computing capacity for map " + mapName);
    LOG.info("VM type       = " + vmBit + "-bit");
    LOG.info(percentage + "% max memory "
        + StringUtils.TraditionalBinaryPrefix.long2String(maxMemory, "B", 1)
        + " = "
        + StringUtils.TraditionalBinaryPrefix.long2String((long) percentMemory,
            "B", 1));
    LOG.info("capacity      = 2^" + exponent + " = " + c + " entries");
    return c;
  }
  
  public void clear() {
    modification++;
    Arrays.fill(entries, null);
    size = 0;
  }
}
