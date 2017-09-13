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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class KeyTimeline {

  private final List<KeyOperationGroup> sortedOperationGroups = new ArrayList<>();
  private final String key;
  private long notBeforeTs = Long.MIN_VALUE;
  private boolean activateNotBeforeCheck = false;

  public KeyTimeline(KeyOperation operation) {
    this.key = operation.getKey();
    KeyOperationGroup operationGroup = new KeyOperationGroup(operation);
    this.sortedOperationGroups.add(operationGroup);
    this.notBeforeTs = operationGroup.endTS();
  }

  public String getKey() {
    return key;
  }

  public void add(KeyOperation operation) {
    if (!operation.getKey().equals(key)) {
      throw new IllegalArgumentException("Operation key does not mach : " + operation.getKey() + " - should be : " + key);
    }

    if (operation.getStartTS() <= notBeforeTs && activateNotBeforeCheck) {
      // if this happens, the history is too short
      throw new IllegalStateException("History too short for such unordered gap at " + operation.getName() + "#" + operation.getKey() +
          " startTS=" + operation.getStartTS() + " - timeline size : " + sortedOperationGroups.size());
    }

    // look for an operationGroup this operation is overlapping with
    //TODO: the potential of perf optimization of the following loop is immense
    // hopefully, this happens so rarely that this does not matter
    for (int i = 0; i < sortedOperationGroups.size(); i++) {
      KeyOperationGroup operationGroup = sortedOperationGroups.get(i);
      if (operation.getStartTS() <= operationGroup.endTS()) {
        operationGroup.add(operation);
        if (sortedOperationGroups.size() > i + 1) {
          // all operations after the indexed one may overlap with it, so they must be re-added to make sure they end up in the right group
          List<KeyOperationGroup> endOfList = sortedOperationGroups.subList(i + 1, sortedOperationGroups.size());
          List<KeyOperationGroup> toReprocess = new ArrayList<>(endOfList);
          endOfList.clear();
          for (KeyOperationGroup og : toReprocess) {
            for (KeyOperation o : og.getOperations()) {
              add(o);
            }
          }
        }
        return;
      }
    }

    // either this is the 1st operation, or this timeline got emptied and is now being refilled
    KeyOperationGroup operationGroup = new KeyOperationGroup(operation);
    sortedOperationGroups.add(operationGroup);
  }

  public StepResult step(Set<RecordValue> possibleValuesAtHead) throws VerificationException {
    KeyOperationGroup operationGroup = sortedOperationGroups.remove(0);

    Set<RecordValue> allNewPossibleValues = new HashSet<>();
    Set<RecordValue> allIntermediateValues = new HashSet<>();
    for (RecordValue possibleValue : possibleValuesAtHead) {
      Values values = operationGroup.replay(possibleValue);
      allNewPossibleValues.addAll(values.getCommittedValues());
      allIntermediateValues.addAll(values.getIntermediateValues());
    }
    String error = null;
    if (allNewPossibleValues.isEmpty()) {
      error = "Verification error on key " + key + " : " + operationGroup + " makes no sense with " + possibleValuesAtHead;
      allNewPossibleValues.add(RecordValue.ABSENT);
      allNewPossibleValues.add(RecordValue.UNKNOWN_PRESENT);
    }

    activateNotBeforeCheck = true;
    notBeforeTs = operationGroup.endTS();
    StepResult stepResult = new StepResult(operationGroup.startTS(), operationGroup.endTS(), allNewPossibleValues, operationGroup.size(), allIntermediateValues);
    if (error != null) {
      throw new VerificationException(error, stepResult);
    }
    return stepResult;
  }

  boolean isEmpty() {
    return sortedOperationGroups.isEmpty();
  }

  int size() {
    return sortedOperationGroups.size();
  }
}
