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


public class JournalEntry {
  private long start;
  private long end;
  private String operationName;
  private String key;
  private String result;

  public JournalEntry() {
  }

  public void fillWith(long start, long end, String operationName, String key, String result) {
    this.start = start;
    this.end = end;
    this.operationName = operationName;
    this.key = key;
    this.result = result;
  }

  public long getStart() {
    return start;
  }

  public long getEnd() {
    return end;
  }

  public String getOperationName() {
    return operationName;
  }

  public String getKey() {
    return key;
  }

  public String getResult() {
    return result;
  }

}
