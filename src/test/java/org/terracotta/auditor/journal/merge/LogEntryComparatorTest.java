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

import static org.junit.Assert.assertTrue;

public class LogEntryComparatorTest {
  @Test
  public void lessThan() {
    assertTrue(new LogEntryComparator().compare("1;2;OP;KEY;RESULT", "1;4;OP;KEY;RESULT") < 0);
  }

  @Test
  public void greaterThan() {
    assertTrue(new LogEntryComparator().compare("1;4;OP;KEY;RESULT", "1;2;OP;KEY;RESULT") > 0);
  }

  @Test
  public void equalTo() {
    assertTrue(new LogEntryComparator().compare("0;2;OP;KEY;RESULT", "1;2;OP;KEY;RESULT") == 0);
  }
}
