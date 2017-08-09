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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class OperationGroup {

  private final List<Operation> sortedOperations = new ArrayList<>();
  private long endTS;

  OperationGroup(Operation operation) {
    this.sortedOperations.add(operation);
    this.endTS = operation.getEndTS();
  }

  public List<Operation> getOperations() {
    return sortedOperations;
  }

  public void add(Operation operation) {
    sortedOperations.add(operation);
    sortedOperations.sort((o1, o2) -> {
      long o1StartTS = o1.getStartTS();
      long o2StartTS = o2.getStartTS();
      if (o1StartTS < o2StartTS) {
        return -1;
      } else if (o1StartTS > o2StartTS) {
        return 1;
      } else {
        return 0;
      }
    });
    if (operation.getEndTS() > endTS) {
      endTS = operation.getEndTS();
    }
  }

  Set<RecordValue> replay(RecordValue fromValue) {
    HashSet<RecordValue> result = new HashSet<>();

    // generate all possible permutations to replay
    List<List<Operation>> allPermutations = allPermutations(sortedOperations);

    outerLoop:
    for (List<Operation> chosenOrder : allPermutations) {
      Evaluation latestEvaluation = new Evaluation();
      latestEvaluation.setRecordValue(fromValue);

      for (Operation operation : chosenOrder) {
        Evaluation evaluation = operation.verifyAndReplay(latestEvaluation.getRecordValue());
        latestEvaluation = evaluation;
        if (!evaluation.getErrors().isEmpty()) {
          // illegal path, compute next one
          continue outerLoop;
        }
      }

      // valid path, remember result
      result.add(latestEvaluation.getRecordValue());
    }

    // result can be empty if this group makes no sense with the given fromValue
    return result;
  }

  private List<List<Operation>> allPermutations(List<Operation> sortedOperations) {
    if (sortedOperations.size() == 1) {
      return Collections.singletonList(sortedOperations);
    }
    //TODO: the permutation list could be reduced as not all timestamps of a group do overlap
    return generatePermutations(new ArrayList<>(sortedOperations));
  }

  private List<List<Operation>> generatePermutations(List<Operation> original) {
    if (original.isEmpty()) {
      List<List<Operation>> result = new ArrayList<>();
      result.add(new ArrayList<>());
      return result;
    }
    Operation firstElement = original.remove(0);
    List<List<Operation>> returnValue = new ArrayList<>();
    List<List<Operation>> permutations = generatePermutations(original);
    for (List<Operation> smallerPermutated : permutations) {
      for (int i = 0; i <= smallerPermutated.size(); i++) {
        List<Operation> temp = new ArrayList<>(smallerPermutated);
        temp.add(i, firstElement);
        returnValue.add(temp);
      }
    }
    return returnValue;
  }

  int size() {
    return sortedOperations.size();
  }

  long endTS() {
    return endTS;
  }

  @Override
  public String toString() {
    return sortedOperations.toString();
  }
}
