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
package org.terracotta.auditor.journal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import com.tc.test.DirectoryHelperExtension;
import com.tc.test.TempDirectoryHelper;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

@ExtendWith(DirectoryHelperExtension.class)
public class FileJournalTest {
  private TempDirectoryHelper tempDirectoryHelper;

  @Test
  public void writesToFile() throws Exception {
    File file = tempDirectoryHelper.getFile(this.getClass().getSimpleName() + "_writeToFile");
    try (Journal journal = new FileJournal(file)) {
      journal.log(1, 2, "OP1", "KEY1", "RESULT1");
      journal.log(3, 4, "OP2", "KEY2", "RESULT2");
      journal.log(5, 6, "OP3", "KEY3", "RESULT3");
    }

    List<String> output = Files.lines(file.toPath()).collect(Collectors.toList());
    assertThat(output, contains("1;2;OP1;KEY1;RESULT1", "3;4;OP2;KEY2;RESULT2", "5;6;OP3;KEY3;RESULT3"));
  }
}
