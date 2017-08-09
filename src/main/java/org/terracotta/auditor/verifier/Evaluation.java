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

import java.util.ArrayList;
import java.util.List;

public class Evaluation {

  private RecordValue recordValue;
  private final List<String> errors = new ArrayList<>();


  public void addError(String error) {
    errors.add(error);
  }

  public void setRecordValue(RecordValue recordValue) {
    this.recordValue = recordValue;
  }

  public RecordValue getRecordValue() {
    return recordValue;
  }

  public List<String> getErrors() {
    return errors;
  }
}
