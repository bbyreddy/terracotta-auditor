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

import java.util.Iterator;
import java.util.Spliterators;
import java.util.function.Consumer;

public class IteratorSpliterator<T> extends Spliterators.AbstractSpliterator<T> {
  private final Iterator<T> iterator;

  public IteratorSpliterator(Iterator<T> iterator) {
    super(Long.MAX_VALUE, 0);
    this.iterator = iterator;
  }

  @Override
  public boolean tryAdvance(Consumer<? super T> action) {
    if (iterator.hasNext()) {
      T next = iterator.next();
      action.accept(next);
      return true;
    }

    return false;
  }
}
