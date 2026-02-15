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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * The difference between the current state and a previous state of a list.
 * 
 * Given a previous state of a set and a sequence of create, delete and modify
 * operations such that the current state of the set can be obtained by applying
 * the operations on the previous state, the following algorithm construct the
 * difference between the current state and the previous state of the set.
 * 
 * <pre>
 * Two lists are maintained in the algorithm:
 * - c-list for newly created elements
 * - d-list for the deleted elements
 *
 * Denote the state of an element by the following
 *   (0, 0): neither in c-list nor d-list
 *   (c, 0): in c-list but not in d-list
 *   (0, d): in d-list but not in c-list
 *   (c, d): in both c-list and d-list
 *
 * For each case below, ( , ) at the end shows the result state of the element.
 *
 * Case 1. Suppose the element i is NOT in the previous state.           (0, 0)
 *   1.1. create i in current: add it to c-list                          (c, 0)
 *   1.1.1. create i in current and then create: impossible
 *   1.1.2. create i in current and then delete: remove it from c-list   (0, 0)
 *   1.1.3. create i in current and then modify: replace it in c-list    (c', 0)
 *
 *   1.2. delete i from current: impossible
 *
 *   1.3. modify i in current: impossible
 *
 * Case 2. Suppose the element i is ALREADY in the previous state.       (0, 0)
 *   2.1. create i in current: impossible
 *
 *   2.2. delete i from current: add it to d-list                        (0, d)
 *   2.2.1. delete i from current and then create: add it to c-list      (c, d)
 *   2.2.2. delete i from current and then delete: impossible
 *   2.2.2. delete i from current and then modify: impossible
 *
 *   2.3. modify i in current: put it in both c-list and d-list          (c, d)
 *   2.3.1. modify i in current and then create: impossible
 *   2.3.2. modify i in current and then delete: remove it from c-list   (0, d)
 *   2.3.3. modify i in current and then modify: replace it in c-list    (c', d)
 * </pre>
 *
 * @param <K> The key type.
 * @param <E> The element type, which must implement {@link Element} interface.
 */
//核心目的是计算并管理两个列表之间的差异，支持创建、删除和修改操作的记录与撤销。
// 通过维护 created 和 deleted 列表，可以追踪列表中元素的变化
public class Diff<K, E extends Diff.Element<K>> {
  /** An interface for the elements in a {@link Diff}. */
  //存储元素的接口
  public static interface Element<K> extends Comparable<K> {
    /** @return the key of this object. */
    public K getKey();
  }

  /** An interface for passing a method in order to process elements. */
  //处理元素的接口
  public static interface Processor<E> {
    /** Process the given element. */
    public void process(E element);
  }

  /** Containing exactly one element. */
  //只有一个元素的容器
  public static class Container<E> {
    private final E element;

    private Container(E element) {
      this.element = element;
    }

    /** @return the element. */
    public E getElement() {
      return element;
    }
  }
  
  /** 
   * Undo information for some operations such as delete(E)
   * and {@link Diff#modify(Element, Element)}.
   */
  public static class UndoInfo<E> {
    private final int createdInsertionPoint;//表示元素插入或创建的位置。如果需要撤销操作，可以用它来追踪元素的原始位置
    private final E trashed;//保存被删除或丢弃的元素。通过 getTrashedElement() 方法可以获取该元素
    private final Integer deletedInsertionPoint;//表示元素被删除的位置（如果适用）。由于可能没有删除位置，所以它是一个 Integer 类型（可以为 null）
    
    private UndoInfo(final int createdInsertionPoint, final E trashed,
        final Integer deletedInsertionPoint) {
      this.createdInsertionPoint = createdInsertionPoint;
      this.trashed = trashed;
      this.deletedInsertionPoint = deletedInsertionPoint;
    }
    
    public E getTrashedElement() {
      return trashed;
    }
  }

  private static final int DEFAULT_ARRAY_INITIAL_CAPACITY = 4;

  /**
   * Search the element from the list.
   * @return -1 if the list is null; otherwise, return the insertion point
   *    defined in {@link Collections#binarySearch(List, Object)}.
   *    Note that, when the list is null, -1 is the correct insertion point.
   */
  //elements: 要搜索的元素列表
  //name: 要查找的元素的键
  //使用 Collections.binarySearch 方法对给定的列表进行二分查找。如果元素不存在，则返回 -1，否则返回插入点
  protected static <K, E extends Comparable<K>> int search(
      final List<E> elements, final K name) {
    return elements == null? -1: Collections.binarySearch(elements, name);
  }
  //根据索引 i 从列表中移除元素，并确保被移除的元素与期望的元素相同。如果不相同，抛出异常
  private static <E> void remove(final List<E> elements, final int i,
      final E expected) {
    final E removed = elements.remove(-i - 1);
    Preconditions.checkState(removed == expected,
        "removed != expected=%s, removed=%s.", expected, removed);
  }

  /** c-list: element(s) created in current. */
  //用于存储在当前状态中被“创建”的元素。即，当前列表中存在，但在前一个状态中不存在的元素
  //也就是新建的元素
  private List<E> created;
  /** d-list: element(s) deleted from current. */
  //用于存储在当前状态中被“删除”的元素。即，当前列表中不存在，但在前一个状态中存在的元素
  //也就是删除的元素
  private List<E> deleted;
   //构造方法
  protected Diff() {}

  protected Diff(final List<E> created, final List<E> deleted) {
    this.created = created;
    this.deleted = deleted;
  }
  //返回不可修改的 created 列表。如果 created 列表为空或为 null，则返回一个空列表
  public List<E> getCreatedUnmodifiable() {
    return created != null? Collections.unmodifiableList(created)
        : Collections.emptyList();
  }
  //更新 created 列表中的元素，如果元素的键不同，则抛出异常
  public E setCreated(int index, E element) {
    final E old = created.set(index, element);
    if (old.compareTo(element.getKey()) != 0) {
      throw new AssertionError("Element mismatched: element=" + element
          + " but old=" + old);
    }
    return old;
  }
  //从 created 列表中移除给定的元素，并返回是否成功移除
  public boolean removeCreated(final E element) {
    if (created != null) {
      final int i = search(created, element.getKey());
      if (i >= 0 && created.get(i) == element) {
        created.remove(i);
        return true;
      }
    }
    return false;
  }
  //清空 created 列表
  public void clearCreated() {
    if (created != null) {
      created.clear();
    }
  }
  //返回不可修改的 deleted 列表。如果 deleted 列表为空或为 null，则返回一个空列表
  public List<E> getDeletedUnmodifiable() {
    return deleted != null? Collections.unmodifiableList(deleted)
        : Collections.emptyList();
  }
  //检查 deleted 列表中是否包含指定的元素键
  public boolean containsDeleted(final K key) {
    if (deleted != null) {
      return search(deleted, key) >= 0;
    }
    return false;
  }
  //检查 deleted 列表中是否包含指定的元素
  public boolean containsDeleted(final E element) {
    return getDeleted(element.getKey()) == element;
  }

  /**
   * @return null if the element is not found;
   *         otherwise, return the element in the deleted list.
   */
  //如果在 deleted 列表中找到对应的元素，则返回该元素；否则返回 null
  public E getDeleted(final K key) {
    if (deleted != null) {
      final int c = search(deleted, key);
      if (c >= 0) {
        return deleted.get(c);
      }
    }
    return null;
  }
  //从 deleted 列表中移除给定的元素，并返回是否成功移除
  public boolean removeDeleted(final E element) {
    if (deleted != null) {
      final int i = search(deleted, element.getKey());
      if (i >= 0 && deleted.get(i) == element) {
        deleted.remove(i);
        return true;
      }
    }
    return false;
  }
  //清空 deleted 列表
  public void clearDeleted() {
    if (deleted != null) {
      deleted.clear();
    }
  }
  //如果 created 和 deleted 列表都为空或为 null，则返回 true，表示没有任何变化；否则返回 false
  /** @return true if no changes contained in the diff */
  public boolean isEmpty() {
    return (created == null || created.isEmpty())
        && (deleted == null || deleted.isEmpty());
  }
  
  /**
   * Add the given element to the created list,
   * provided the element does not exist, i.e. i < 0.
   *
   * @param i the insertion point defined
   *          in {@link Collections#binarySearch(List, Object)}
   * @throws AssertionError if i >= 0.
   */
  //将元素添加到 created 列表中，前提是该元素不已经存在。如果插入点 i >= 0，则抛出异常
  private void addCreated(final E element, final int i) {
    if (i >= 0) {
      throw new AssertionError("Element already exists: element=" + element
          + ", created=" + created);
    }
    if (created == null) {
      created = new ArrayList<>(DEFAULT_ARRAY_INITIAL_CAPACITY);
    }
    created.add(-i - 1, element);
  }

  /** Similar to {@link #addCreated(Element, int)} but for the deleted list. */
  //将元素添加到 deleted 列表中，前提是该元素不已经存在。如果插入点 i >= 0，则抛出异常
  private void addDeleted(final E element, final int i) {
    if (i >= 0) {
      throw new AssertionError("Element already exists: element=" + element
          + ", deleted=" + deleted);
    }
    if (deleted == null) {
      deleted = new ArrayList<>(DEFAULT_ARRAY_INITIAL_CAPACITY);
    }
    deleted.add(-i - 1, element);
  }


  /**
   * Create an element in current state.
   * @return the c-list insertion point for undo.
   */
  //将元素添加到 created 列表中，并返回插入点
  public int create(final E element) {
    final int c = search(created, element.getKey());
    addCreated(element, c);
    return c;
  }

  /**
   * Undo the previous create(E) operation. Note that the behavior is
   * undefined if the previous operation is not create(E).
   */
  //根据给定的插入点撤销 create 操作，将元素从 created 列表中移除
  public void undoCreate(final E element, final int insertionPoint) {
    remove(created, insertionPoint, element);
  }

  /**
   * Delete an element from current state.
   * @return the undo information.
   */
  //从 created 列表中移除元素。如果元素不在 created 列表中，则将其添加到 deleted 列表中
  public UndoInfo<E> delete(final E element) {
    final int c = search(created, element.getKey());
    E previous = null;
    Integer d = null;
    if (c >= 0) {
      // remove a newly created element
      previous = created.remove(c);
    } else {
      // not in c-list, it must be in previous
      d = search(deleted, element.getKey());
      addDeleted(element, d);
    }
    return new UndoInfo<E>(c, previous, d);
  }
  
  /**
   * Undo the previous delete(E) operation. Note that the behavior is
   * undefined if the previous operation is not delete(E).
   */
  //根据撤销信息恢复删除的元素，移回 created 列表或从 deleted 列表中移除
  public void undoDelete(final E element, final UndoInfo<E> undoInfo) {
    final int c = undoInfo.createdInsertionPoint;
    if (c >= 0) {
      created.add(c, undoInfo.trashed);
    } else {
      remove(deleted, undoInfo.deletedInsertionPoint, element);
    }
  }

  /**
   * Modify an element in current state.
   * @return the undo information.
   */
  //如果元素存在于 created 列表中，则替换该元素。如果元素不在 created 列表中，则先删除旧元素，然后添加新元素
  public UndoInfo<E> modify(final E oldElement, final E newElement) {
    Preconditions.checkArgument(oldElement != newElement,
        "They are the same object: oldElement == newElement = %s", newElement);
    Preconditions.checkArgument(oldElement.compareTo(newElement.getKey()) == 0,
        "The names do not match: oldElement=%s, newElement=%s",
        oldElement, newElement);
    final int c = search(created, newElement.getKey());
    E previous = null;
    Integer d = null;
    if (c >= 0) {
      // Case 1.1.3 and 2.3.3: element is already in c-list,
      previous = created.set(c, newElement);
      
      // For previous != oldElement, set it to oldElement
      previous = oldElement;
    } else {
      d = search(deleted, oldElement.getKey());
      if (d < 0) {
        // Case 2.3: neither in c-list nor d-list
        addCreated(newElement, c);
        addDeleted(oldElement, d);
      }
    }
    return new UndoInfo<E>(c, previous, d);
  }

  /**
   * Undo the previous modify(E, E) operation. Note that the behavior
   * is undefined if the previous operation is not modify(E, E).
   */
  //根据撤销信息恢复被修改的元素，替换 created 列表中的元素，或从 deleted 列表中移除元素
  public void undoModify(final E oldElement, final E newElement,
      final UndoInfo<E> undoInfo) {
    final int c = undoInfo.createdInsertionPoint;
    if (c >= 0) {
      created.set(c, undoInfo.trashed);
    } else {
      final int d = undoInfo.deletedInsertionPoint;
      if (d < 0) {
        remove(created, c, newElement);
        remove(deleted, d, oldElement);
      }
    }
  }

  /**
   * Find an element in the previous state.
   * 
   * @return null if the element cannot be determined in the previous state
   *         since no change is recorded and it should be determined in the
   *         current state; otherwise, return a {@link Container} containing the
   *         element in the previous state. Note that the element can possibly
   *         be null which means that the element is not found in the previous
   *         state.
   */
  //返回一个 Container<E> 对象，包含元素在前一个状态中的信息。如果元素未找到，则返回 null
  public Container<E> accessPrevious(final K name) {
    return accessPrevious(name, created, deleted);
  }

  private static <K, E extends Diff.Element<K>> Container<E> accessPrevious(
      final K name, final List<E> clist, final List<E> dlist) {
    final int d = search(dlist, name);
    if (d >= 0) {
      // the element was in previous and was once deleted in current.
      return new Container<E>(dlist.get(d));
    } else {
      final int c = search(clist, name);
      // When c >= 0, the element in current is a newly created element.
      return c < 0? null: new Container<E>(null);
    }
  }

  /**
   * Find an element in the current state.
   * 
   * @return null if the element cannot be determined in the current state since
   *         no change is recorded and it should be determined in the previous
   *         state; otherwise, return a {@link Container} containing the element in
   *         the current state. Note that the element can possibly be null which
   *         means that the element is not found in the current state.
   */
  //返回一个 Container<E> 对象，包含元素在当前状态中的信息。如果元素未找到，则返回 null
  public Container<E> accessCurrent(K name) {
    return accessPrevious(name, deleted, created);
  }

  /**
   * Apply this diff to previous state in order to obtain current state.
   * @return the current state of the list.
   */
  //返回应用差异后的当前状态
  public List<E> apply2Previous(final List<E> previous) {
    return apply2Previous(previous,
        getCreatedUnmodifiable(), getDeletedUnmodifiable());
  }

  private static <K, E extends Diff.Element<K>> List<E> apply2Previous(
      final List<E> previous, final List<E> clist, final List<E> dlist) {
    // Assumptions:
    // (A1) All lists are sorted.
    // (A2) All elements in dlist must be in previous.
    // (A3) All elements in clist must be not in tmp = previous - dlist.
    final List<E> tmp = new ArrayList<E>(previous.size() - dlist.size());
    {
      // tmp = previous - dlist
      final Iterator<E> i = previous.iterator();
      for(E deleted : dlist) {
        E e = i.next(); //since dlist is non-empty, e must exist by (A2).
        int cmp = 0;
        for(; (cmp = e.compareTo(deleted.getKey())) < 0; e = i.next()) {
          tmp.add(e);
        }
        Preconditions.checkState(cmp == 0); // check (A2)
      }
      for(; i.hasNext(); ) {
        tmp.add(i.next());
      }
    }

    final List<E> current = new ArrayList<E>(tmp.size() + clist.size());
    {
      // current = tmp + clist
      final Iterator<E> tmpIterator = tmp.iterator();
      final Iterator<E> cIterator = clist.iterator();

      E t = tmpIterator.hasNext()? tmpIterator.next(): null;
      E c = cIterator.hasNext()? cIterator.next(): null;
      for(; t != null || c != null; ) {
        final int cmp = c == null? 1
            : t == null? -1
            : c.compareTo(t.getKey());

        if (cmp < 0) {
          current.add(c);
          c = cIterator.hasNext()? cIterator.next(): null;
        } else if (cmp > 0) {
          current.add(t);
          t = tmpIterator.hasNext()? tmpIterator.next(): null;
        } else {
          throw new AssertionError("Violated assumption (A3).");
        }
      }
    }
    return current;
  }

  /**
   * Apply the reverse of this diff to current state in order
   * to obtain the previous state.
   * @return the previous state of the list.
   */
  //返回应用反向差异后的前一个状态
  public List<E> apply2Current(final List<E> current) {
    return apply2Previous(current,
        getDeletedUnmodifiable(), getCreatedUnmodifiable());
  }
  
  /**
   * Combine this diff with a posterior diff.  We have the following cases:
   * 
   * <pre>
   * 1. For (c, 0) in the posterior diff, check the element in this diff:
   * 1.1 (c', 0)  in this diff: impossible
   * 1.2 (0, d')  in this diff: put in c-list --&gt; (c, d')
   * 1.3 (c', d') in this diff: impossible
   * 1.4 (0, 0)   in this diff: put in c-list --&gt; (c, 0)
   * This is the same logic as create(E).
   * 
   * 2. For (0, d) in the posterior diff,
   * 2.1 (c', 0)  in this diff: remove from c-list --&gt; (0, 0)
   * 2.2 (0, d')  in this diff: impossible
   * 2.3 (c', d') in this diff: remove from c-list --&gt; (0, d')
   * 2.4 (0, 0)   in this diff: put in d-list --&gt; (0, d)
   * This is the same logic as delete(E).
   * 
   * 3. For (c, d) in the posterior diff,
   * 3.1 (c', 0)  in this diff: replace the element in c-list --&gt; (c, 0)
   * 3.2 (0, d')  in this diff: impossible
   * 3.3 (c', d') in this diff: replace the element in c-list --&gt; (c, d')
   * 3.4 (0, 0)   in this diff: put in c-list and d-list --&gt; (c, d)
   * This is the same logic as modify(E, E).
   * </pre>
   * 
   * @param posterior The posterior diff to combine with.
   * @param deletedProcesser
   *     process the deleted/overwritten elements in case 2.1, 2.3, 3.1 and 3.3.
   */
  public void combinePosterior(final Diff<K, E> posterior,
      final Processor<E> deletedProcesser) {
    final Iterator<E> createdIterator
        = posterior.getCreatedUnmodifiable().iterator();
    final Iterator<E> deletedIterator
        = posterior.getDeletedUnmodifiable().iterator();

    E c = createdIterator.hasNext()? createdIterator.next(): null;
    E d = deletedIterator.hasNext()? deletedIterator.next(): null;

    for(; c != null || d != null; ) {
      final int cmp = c == null? 1
          : d == null? -1
          : c.compareTo(d.getKey());
      if (cmp < 0) {
        // case 1: only in c-list
        create(c);
        c = createdIterator.hasNext()? createdIterator.next(): null;
      } else if (cmp > 0) {
        // case 2: only in d-list
        final UndoInfo<E> ui = delete(d);
        if (deletedProcesser != null) {
          deletedProcesser.process(ui.trashed);
        }
        d = deletedIterator.hasNext()? deletedIterator.next(): null;
      } else {
        // case 3: in both c-list and d-list 
        final UndoInfo<E> ui = modify(d, c);
        if (deletedProcesser != null) {
          deletedProcesser.process(ui.trashed);
        }
        c = createdIterator.hasNext()? createdIterator.next(): null;
        d = deletedIterator.hasNext()? deletedIterator.next(): null;
      }
    }
  }

  @Override
  public String toString() {
    return getClass().getSimpleName()
        +  "{created=" + getCreatedUnmodifiable()
        + ", deleted=" + getDeletedUnmodifiable() + "}";
  }
}
