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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.terracotta.auditor.operations.Operations;
import org.terracotta.auditor.verifier.RecordValue;
import org.terracotta.auditor.verifier.Verifier;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

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

  @Test
  public void nonKeyOperationWithoutOverlap_1() throws Exception {
    String journalContents =
            "1;2;Add;1;true\n" +
            "1;2;Delete;1;true\n" +
            "3;4;Count;;0";

    Verifier verifier = new Verifier(new StringReader(journalContents), 10, Operations.parser());

    List<String> errors = verifier.verify();
    assertThat(errors, empty());
  }

  @Test
  public void nonKeyOperationWithoutOverlap_2() throws Exception {
    String journalContents =
            "1;2;Add;1;true\n" +
            "1;2;Delete;1;true\n" +
            "3;4;Count;;1";

    Verifier verifier = new Verifier(new StringReader(journalContents), 10, Operations.parser());

    List<String> errors = verifier.verify();
    assertThat(errors.size(), is(1));
  }

  @Test
  public void nonKeyOperationWithoutOverlap_3() throws Exception {
    String journalContents =
            "1;2;Add;1;true\n" +
            "1;2;Add;1;false\n" +
            "3;4;Count;;1";

    Verifier verifier = new Verifier(new StringReader(journalContents), 10, Operations.parser());

    List<String> errors = verifier.verify();
    assertThat(errors, empty());
  }

  @Test
  public void nonKeyOperationWithoutOverlap_4() throws Exception {
    String journalContents =
            "1;2;Add;1;false\n" +
            "1;2;Add;1;true\n" +
            "3;4;Count;;1";

    Verifier verifier = new Verifier(new StringReader(journalContents), 10, Operations.parser());

    List<String> errors = verifier.verify();
    assertThat(errors, empty());
  }

  @Test
  public void nonKeyOperationWithoutOverlap_99() throws Exception {
    {
      String journalContents =
                  "1;1;Add;1;true\n" +
                  "2;2;Add;1;false\n" +
                  "1;1;Add;2;true\n" +
                  "2;2;Delete;2;true\n" +
                  "3;3;Delete;1;true\n" +
                  "3;3;Count;;1";

      Verifier verifier = new Verifier(new StringReader(journalContents), 10, Operations.parser());

      List<String> errors = verifier.verify();
      assertThat(errors, empty());
    }
    {
      String journalContents =
                  "1;1;Add;1;true\n" +
                  "2;2;Add;1;false\n" +
                  "1;1;Add;2;true\n" +
                  "2;2;Delete;2;true\n" +
                  "3;3;Delete;1;true\n" +
                  "3;3;Count;;0";

      Verifier verifier = new Verifier(new StringReader(journalContents), 10, Operations.parser());

      List<String> errors = verifier.verify();
      assertThat(errors, empty());
    }
    {
      String journalContents =
                  "1;1;Add;1;true\n" +
                  "2;2;Add;1;false\n" +
                  "1;1;Add;2;true\n" +
                  "2;2;Delete;2;true\n" +
                  "3;3;Delete;1;true\n" +
                  "3;3;Count;;2";

      Verifier verifier = new Verifier(new StringReader(journalContents), 10, Operations.parser());

      List<String> errors = verifier.verify();
      assertThat(errors.size(), is(1));
    }
  }

  @Test
  public void nonKeyOperationWithOverlappingCancellingOperations() throws Exception {
    String journalContents =
            "1;2;Add;369;true\n" +
            "3;4;Get;369;Optional[LazyVersionedRecord{records=[LazySingleRecord{key=369, timestamp=SystemTimeReference{systemTimeMillis=1504876210710}, msn=-9223372036854311915, cells=[LazyCell[definition=CellDefinition[name='rnd' type='Type<Integer>'] value='-1769147478']]}]}]\n" +
            "5;7;Update;369;true\n" +
            "6;8;Delete;369;true\n" +
            "5;9;Find20;[930, 995, 965, 741, 998, 326, 614, 10, 587, 525, 78, 623, 464, 113, 977, 369, 498, 755, 723, 823];[LazyVersionedRecord{records=[LazySingleRecord{key=369, timestamp=SystemTimeReference{systemTimeMillis=1504876210816}, msn=-9223372036854311349, cells=[LazyCell[definition=CellDefinition[name='rnd' type='Type<Integer>'] value='-470853418']]}]}]\n";

    Verifier verifier = new Verifier(new StringReader(journalContents), 10, Operations.parser());

    List<String> errors = verifier.verify();
    assertThat(errors, is(empty()));
  }

  @Test
  public void nonKeyOperationWithOverlappingAdditiveOperations() throws Exception {
    String journalContents =
            "385247837;385255579;Add;116;true\n" +
            "408249980;408253429;Get;116;Optional[LazyVersionedRecord{records=[LazySingleRecord{key=116, timestamp=SystemTimeReference{systemTimeMillis=1504876173072}, msn=-9223372036854517198, cells=[LazyCell[definition=CellDefinition[name='rnd' type='Type<Integer>'] value='-1203235139']]}]}]\n" +
            "475470537;475764371;Delete;116;true\n" +
            "475718997;475793485;Add;116;true\n" +
            "471153194;478300726;Find20;[131, 196, 612, 4, 774, 230, 650, 971, 235, 844, 178, 788, 116, 601, 890, 731, 540, 990, 222, 255];[LazyVersionedRecord{records=[LazySingleRecord{key=116, timestamp=SystemTimeReference{systemTimeMillis=1504876173162}, msn=-9223372036854516740, cells=[LazyCell[definition=CellDefinition[name='rnd' type='Type<Integer>'] value='-49089532']]}]}]\n";

    Verifier verifier = new Verifier(new StringReader(journalContents), 10, Operations.parser());

    List<String> errors = verifier.verify();
    assertThat(errors, is(empty()));
  }

  @Test
  @Disabled
  public void integrity() throws Exception {
    try (InputStream is = getClass().getClassLoader().getResourceAsStream("nonPersistent-journal.txt")) {
      Verifier verifier = new Verifier(new InputStreamReader(is), 1_000_000, Operations.parser());

      List<String> errors = verifier.verify();
      errors.forEach(System.out::println);

      assertThat(errors, empty());
    }
  }
}
