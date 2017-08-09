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

public class KeyTimeline {

  private final List<OperationGroup> sortedOperationGroups = new ArrayList<>();
  private final Set<RecordValue> possibleValuesAtHead = new HashSet<>();
  private final String key;
  private long notBeforeTs = Long.MIN_VALUE;
  private boolean activateNotBeforeCheck = false;

  public KeyTimeline(Operation operation) {
    this.possibleValuesAtHead.add(RecordValue.ABSENT);
    this.key = operation.getKey();
    OperationGroup operationGroup = new OperationGroup(operation);
    this.sortedOperationGroups.add(operationGroup);
    this.notBeforeTs = operationGroup.endTS();
  }

  public String getKey() {
    return key;
  }

  public void add(Operation operation) {
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
      OperationGroup operationGroup = sortedOperationGroups.get(i);
      if (operation.getStartTS() <= operationGroup.endTS()) {
        operationGroup.add(operation);
        if (sortedOperationGroups.size() > i + 1) {
          // all operations after the indexed one may overlap with it, so they must be re-added to make sure they end up in the right group
          List<OperationGroup> endOfList = sortedOperationGroups.subList(i + 1, sortedOperationGroups.size());
          List<OperationGroup> toReprocess = new ArrayList<>(endOfList);
          endOfList.clear();
          for (OperationGroup og : toReprocess) {
            for (Operation o : og.getOperations()) {
              add(o);
            }
          }
        }
        return;
      }
    }

    // either this is the 1st operation, or this timeline got emptied and is now being refilled
    OperationGroup operationGroup = new OperationGroup(operation);
    sortedOperationGroups.add(operationGroup);
  }

  public int step() throws VerificationException {
    OperationGroup operationGroup = sortedOperationGroups.remove(0);

    Set<RecordValue> allNewPossibleValues = new HashSet<>();
    for (RecordValue possibleValue : possibleValuesAtHead) {
      Set<RecordValue> newPossibleValues = operationGroup.replay(possibleValue);
      allNewPossibleValues.addAll(newPossibleValues);
    }
    String error = null;
    if (allNewPossibleValues.isEmpty()) {
      error = "Verification error on key " + key + " : " + operationGroup + " makes no sense with " + possibleValuesAtHead;
      allNewPossibleValues.add(RecordValue.ABSENT);
      allNewPossibleValues.add(RecordValue.UNKNOWN_PRESENT);
    }

    possibleValuesAtHead.clear();
    possibleValuesAtHead.addAll(allNewPossibleValues);
    activateNotBeforeCheck = true;
    notBeforeTs = operationGroup.endTS();
    if (error != null) {
      throw new VerificationException(error, operationGroup.size());
    }
    return operationGroup.size();
  }

  boolean isEmpty() {
    return sortedOperationGroups.isEmpty();
  }

  int size() {
    return sortedOperationGroups.size();
  }

  public Set<RecordValue> getPossibleValuesAtHead() {
    return Collections.unmodifiableSet(possibleValuesAtHead);
  }
}
