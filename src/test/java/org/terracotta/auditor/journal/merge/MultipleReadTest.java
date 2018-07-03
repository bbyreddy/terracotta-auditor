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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class MultipleReadTest {
  @Test
  public void canMultipleRead() {
    MultipleRead<String> reader = new MultipleRead<>(Arrays.asList("A", "B", "C").iterator());

    assertEquals("A", reader.getValue());
    assertEquals("A", reader.getValue());

    reader.advanceValue();

    assertEquals("B", reader.getValue());
    assertEquals("B", reader.getValue());

    reader.advanceValue();

    assertEquals("C", reader.getValue());
    assertEquals("C", reader.getValue());

    reader.advanceValue();

    assertNull(reader.getValue());
  }

  @Test
  public void emptyIterator() {
    MultipleRead<String> reader = new MultipleRead<>(Collections.<String>emptyList().iterator());
    assertNull(reader.getValue());
  }
}
