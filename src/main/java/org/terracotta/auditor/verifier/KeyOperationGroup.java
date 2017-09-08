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
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class KeyOperationGroup {

  private final List<KeyOperation> sortedOperations = new ArrayList<>();
  private long startTS;
  private long endTS;

  KeyOperationGroup(KeyOperation operation) {
    this.sortedOperations.add(operation);
    this.startTS = operation.getStartTS();
    this.endTS = operation.getEndTS();
  }

  public List<KeyOperation> getOperations() {
    return sortedOperations;
  }

  public void add(KeyOperation operation) {
    sortedOperations.add(operation);
    sortedOperations.sort(Utils.operationComparator());
    if (operation.getEndTS() > endTS) {
      endTS = operation.getEndTS();
    }
    if (operation.getStartTS() < startTS) {
      startTS = operation.getStartTS();
    }
  }

  Set<RecordValue> replay(RecordValue fromValue) {
    HashSet<RecordValue> result = new HashSet<>();

    // generate all possible permutations to replay
    List<List<KeyOperation>> allPermutations = allPermutations(sortedOperations);

    outerLoop:
    for (List<KeyOperation> chosenOrder : allPermutations) {
      Evaluation latestEvaluation = new Evaluation();
      latestEvaluation.setRecordValue(fromValue);

      for (KeyOperation operation : chosenOrder) {
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

  private List<List<KeyOperation>> allPermutations(List<KeyOperation> sortedOperations) {
    if (sortedOperations.size() == 1) {
      return Collections.singletonList(sortedOperations);
    }
    //TODO: the permutation list could be reduced as not all timestamps of a group do overlap
    return generatePermutations(new ArrayList<>(sortedOperations));
  }

  private List<List<KeyOperation>> generatePermutations(List<KeyOperation> original) {
    if (original.isEmpty()) {
      List<List<KeyOperation>> result = new ArrayList<>();
      result.add(new ArrayList<>());
      return result;
    }
    KeyOperation firstElement = original.remove(0);
    List<List<KeyOperation>> returnValue = new ArrayList<>();
    List<List<KeyOperation>> permutations = generatePermutations(original);
    for (List<KeyOperation> smallerPermutated : permutations) {
      for (int i = 0; i <= smallerPermutated.size(); i++) {
        List<KeyOperation> temp = new ArrayList<>(smallerPermutated);
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

  long startTS() {
    return startTS;
  }

  @Override
  public String toString() {
    return sortedOperations.toString();
  }
}
