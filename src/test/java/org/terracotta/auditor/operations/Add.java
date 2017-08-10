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
package org.terracotta.auditor.operations;

import org.terracotta.auditor.verifier.Evaluation;
import org.terracotta.auditor.verifier.Operation;
import org.terracotta.auditor.verifier.RecordValue;

/**
 * @author Ludovic Orban
 */
public class Add extends Operation {
  public Add(long startTS, long endTS, String key, String result) {
    super("Add", startTS, endTS, key, result);
  }

  @Override
  public Evaluation verifyAndReplay(RecordValue fromValue) {
    Evaluation evaluation = new Evaluation();

    // verify
    if (fromValue.isAbsent()) {
      if (getResult().equals("false")) {
        evaluation.addError("ERROR: Add screwed up on absent - startTS=" + getStartTS());
      }
    } else {
      if (getResult().equals("true")) {
        evaluation.addError("ERROR: Add screwed up on present");
      }
    }


    if (fromValue.isAbsent()) {
      evaluation.setRecordValue(RecordValue.UNKNOWN_PRESENT);
    } else {
      evaluation.setRecordValue(fromValue);
    }

    return evaluation;
  }
}
