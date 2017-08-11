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
package org.terracotta.auditor;

import org.junit.Test;
import org.terracotta.auditor.operations.Operations;
import org.terracotta.auditor.verifier.RecordValue;
import org.terracotta.auditor.verifier.Verifier;

import java.io.StringReader;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author Ludovic Orban
 */
public class VerifierTest {

  @Test
  public void orderedConcurrentAddNoError() throws Exception {
    String journalContents =
        "1;2;Add;1;true\n" +
        "1;2;Add;1;false\n";

    Verifier verifier = new Verifier(new StringReader(journalContents), 10, Operations.parser());
    List<String> errors = verifier.verify((results) -> {
      Set<RecordValue> recordValues = results.get("1");
      assertThat(recordValues.size(), is(1));
      RecordValue recordValue = recordValues.iterator().next();
      assertThat(recordValue.isUnknown(), is(true));
      assertThat(recordValue.isAbsent(), is(false));
      return Collections.emptyList();
    });
    assertThat(errors.size(), is(0));
  }

  @Test
  public void unorderedConcurrentAddNoError() throws Exception {
    String journalContents =
        "1;2;Add;1;false\n" +
        "1;2;Add;1;true\n";

    Verifier verifier = new Verifier(new StringReader(journalContents), 10, Operations.parser());
    List<String> errors = verifier.verify((results) -> {
      Set<RecordValue> recordValues = results.get("1");
      assertThat(recordValues.size(), is(1));
      RecordValue recordValue = recordValues.iterator().next();
      assertThat(recordValue.isUnknown(), is(true));
      assertThat(recordValue.isAbsent(), is(false));
      return Collections.emptyList();
    });
    assertThat(errors.size(), is(0));
  }

  @Test
  public void concurrentAddError() throws Exception {
    String journalContents =
        "1;2;Add;1;true\n" +
        "1;2;Add;1;true\n";

    Verifier verifier = new Verifier(new StringReader(journalContents), 10, Operations.parser());
    List<String> errors = verifier.verify((results) -> {
      Set<RecordValue> recordValues = results.get("1");
      assertThat(recordValues.size(), is(2));
      Iterator<RecordValue> it = recordValues.iterator();
      RecordValue recordValue = it.next();
      if (recordValue.isAbsent()) {
        assertThat(recordValue.isUnknown(), is(false));
        recordValue = it.next();
        assertThat(recordValue.isAbsent(), is(false));
        assertThat(recordValue.isUnknown(), is(true));
      } else {
        assertThat(recordValue.isUnknown(), is(true));
        recordValue = it.next();
        assertThat(recordValue.isAbsent(), is(true));
        assertThat(recordValue.isUnknown(), is(false));
      }
      return Collections.emptyList();
    });
    assertThat(errors.size(), is(1));
  }

}
