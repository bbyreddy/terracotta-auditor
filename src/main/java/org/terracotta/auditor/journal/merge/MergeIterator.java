/*
 * Copyright Terracotta, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.terracotta.auditor.journal.merge;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class MergeIterator<T> implements Iterator<T> {
  private final Comparator<T> comparator;
  private final List<MultipleRead<T>> readers;

  public MergeIterator(Comparator<T> comparator, Iterator<T>... iterators) {
    this.comparator = comparator;
    readers = new ArrayList<>(iterators.length);
    for (Iterator<T> iterator : iterators) {
      MultipleRead<T> reader = new MultipleRead<>(iterator);
      readers.add(reader);
    }
  }

  @Override
  public boolean hasNext() {
    for (MultipleRead<T> reader : readers) {
      T value = reader.getValue();
      if (value != null) {
        return true;
      }
    }

    return false;
  }

  @Override
  public T next() {
    T nextValue = null;
    MultipleRead<T> nextReader = null;

    for (MultipleRead<T> reader : readers) {
      T value = reader.getValue();

      if (isBefore(value, nextValue)) {
        nextValue = value;
        nextReader = reader;
      }
    }

    if (nextValue == null) {
      throw new NoSuchElementException();
    }

    nextReader.advanceValue();

    return nextValue;
  }

  private boolean isBefore(T value, T nextValue) {
    if (value == null) {
      return false;
    }

    if (nextValue == null) {
      return true;
    }

    int comparison = comparator.compare(value, nextValue);
    return comparison < 0;
  }
}
