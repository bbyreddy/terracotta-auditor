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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

public class FileJournal implements Journal {
  private final Writer writer;

  public FileJournal(File file) throws Exception {
    writer = new BufferedWriter(new FileWriter(file));
  }

  @Override
  public void log(long start, long end, String operationName, String key, String result) {
    try {
      writer.write(Long.toString(start));
      writer.write(';');
      writer.write(Long.toString(end));
      writer.write(';');
      writer.write(operationName);
      writer.write(';');
      writer.write(key);
      writer.write(';');
      writer.write(result);
      writer.write('\n');
    } catch (IOException e) {
      throw new RuntimeException("Failed to write to journal", e);
    }
  }

  @Override
  public void close() throws Exception {
    writer.close();
  }
}
