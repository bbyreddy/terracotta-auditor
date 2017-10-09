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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;

public class MergeIteratorTest {
  @Test
  public void merges() {
    Iterator<String> iterator1 = Arrays.asList("AA", "BBB", "CCCCC").iterator();
    Iterator<String> iterator2 = Arrays.asList("A", "BBBB").iterator();
    MergeIterator<String> mergeIterator = new MergeIterator<>(Comparator.comparing(String::length), iterator1, iterator2);

    List<String> merged = new ArrayList<>();
    mergeIterator.forEachRemaining(merged::add);

    assertThat(merged, contains("A", "AA", "BBB", "BBBB", "CCCCC"));
  }
}
