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

import java.util.Comparator;

public class LogEntryComparator implements Comparator<String> {
  @Override
  public int compare(String logLine1, String logLine2) {
    long end1 = getEnd(logLine1);
    long end2 = getEnd(logLine2);

    return Long.signum(end1 - end2);
  }

  private long getEnd(String logLine1) {
    String[] logComponents = logLine1.split(";");
    String endString = logComponents[1];
    return Long.valueOf(endString);
  }
}
