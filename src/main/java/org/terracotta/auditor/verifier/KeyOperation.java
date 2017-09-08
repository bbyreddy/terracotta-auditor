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

public abstract class KeyOperation extends Operation {

  private final String key;

  protected KeyOperation(String name, long startTS, long endTS, String key, String result) {
    super(name, startTS, endTS, result);
    this.key = key;
  }

  public String getKey() {
    return key;
  }

  public abstract Evaluation verifyAndReplay(RecordValue fromValue);

  @Override
  public String toString() {
    return getClass().getSimpleName() + "@" + key + " startTs=" + getStartTS() + " endsTS=" + getEndTS() + " result=" + getResult();
  }
}
