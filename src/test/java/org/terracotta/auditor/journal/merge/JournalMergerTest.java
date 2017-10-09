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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.terracotta.auditor.journal.FileJournal;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;

public class JournalMergerTest {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void mergesJournals() throws Exception {
    File file1 = temporaryFolder.newFile();
    File file2 = temporaryFolder.newFile();
    File file3 = temporaryFolder.newFile();

    try (FileJournal journal1 = new FileJournal(file1);
         FileJournal journal2 = new FileJournal(file2);
         FileJournal journal3 = new FileJournal(file3)) {
      journal1.log(0, 1, "OP1", "KEY1", "RESULT1");
      journal1.log(2, 4, "OP4", "KEY4", "RESULT4");
      journal1.log(5, 7, "OP7", "KEY7", "RESULT7");


      journal2.log(0, 2, "OP2", "KEY2", "RESULT2");
      journal2.log(3, 5, "OP5", "KEY5", "RESULT5");
      journal2.log(6, 8, "OP8", "KEY8", "RESULT8");

      journal3.log(0, 3, "OP3", "KEY3", "RESULT3");
      journal3.log(4, 6, "OP6", "KEY6", "RESULT6");
      journal3.log(7, 9, "OP9", "KEY9", "RESULT9");
    }

    Path output = temporaryFolder.newFile().toPath();
    JournalMerger merger = new JournalMerger(file1, file2, file3);
    merger.mergeTo(output);

    List<String> merged = Files.lines(output).collect(Collectors.toList());

    assertThat(merged, contains(
            "0;1;OP1;KEY1;RESULT1",
            "0;2;OP2;KEY2;RESULT2",
            "0;3;OP3;KEY3;RESULT3",
            "2;4;OP4;KEY4;RESULT4",
            "3;5;OP5;KEY5;RESULT5",
            "4;6;OP6;KEY6;RESULT6",
            "5;7;OP7;KEY7;RESULT7",
            "6;8;OP8;KEY8;RESULT8",
            "7;9;OP9;KEY9;RESULT9"
    ));
  }
}
