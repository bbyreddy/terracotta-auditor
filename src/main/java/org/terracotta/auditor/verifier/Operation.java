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
package org.terracotta.auditor.verifier;

public abstract class Operation {

  private final String name;
  private final long startTS;
  private final long endTS;
  private final String result;

  protected Operation(String name, long startTS, long endTS, String result) {
    this.name = name;
    this.startTS = startTS;
    this.endTS = endTS;
    this.result = result;
  }

  public String getName() {
    return name;
  }

  public long getStartTS() {
    return startTS;
  }

  public long getEndTS() {
    return endTS;
  }

  public String getResult() {
    return result;
  }
}
