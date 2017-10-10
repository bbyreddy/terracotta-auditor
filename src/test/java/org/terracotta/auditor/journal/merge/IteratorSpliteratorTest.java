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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Spliterator;
import java.util.function.Consumer;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(MockitoJUnitRunner.class)
public class IteratorSpliteratorTest {
  @Mock
  private Consumer<String> consumer;

  @Test
  public void iterates() {
    Spliterator<String> spliterator = new IteratorSpliterator<>(Arrays.asList("A", "B", "C").iterator());

    assertTrue(spliterator.tryAdvance(consumer));
    verify(consumer).accept("A");
    verifyNoMoreInteractions(consumer);

    assertTrue(spliterator.tryAdvance(consumer));
    verify(consumer).accept("B");
    verifyNoMoreInteractions(consumer);

    assertTrue(spliterator.tryAdvance(consumer));
    verify(consumer).accept("C");
    verifyNoMoreInteractions(consumer);

    assertFalse(spliterator.tryAdvance(consumer));
    verifyNoMoreInteractions(consumer);
  }
}
