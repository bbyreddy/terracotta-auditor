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

import java.util.BitSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class OrderDeterminer {
  private final List<KeyOperation> operations;
  private final Set<RecordValue> intermediateValues;

  public OrderDeterminer(Collection<KeyOperation> operations) {
    this.operations = operations.stream().sorted(new EndTimestampComparator()).collect(Collectors.toList());
    this.intermediateValues = new HashSet<>();
  }

  public Values findPossibleOutcomes(RecordValue initialValue) {
    Set<Possibility> possibilities = new HashSet<>();
    possibilities.add(new Possibility(new BitSet(operations.size()), initialValue));

    for (int i = 0; i < operations.size(); i++) {
      possibilities = iterate(possibilities);
    }

    Set<RecordValue> finalValues = possibilities.stream().parallel().map(Possibility::getValue).collect(Collectors.toSet());
    return new Values(finalValues, intermediateValues);
  }

  private Set<Possibility> iterate(Set<Possibility> possibilities) {
    Set<Possibility> newPossibilities = new HashSet<>();

    for (Possibility possibility : possibilities) {
      Set<Integer> nextSteps = possibility.findNextSteps();

      for (Integer nextStep : nextSteps) {
        Optional<Possibility> newPossibility = possibility.step(nextStep);
        newPossibility.ifPresent(newPossibilities::add);
      }
    }

    return newPossibilities;
  }

  private class Possibility {
    private final BitSet usedOperations;
    private final RecordValue value;

    public Possibility(BitSet usedOperations, RecordValue value) {
      this.usedOperations = usedOperations;
      this.value = value;
    }

    public RecordValue getValue() {
      return value;
    }

    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (o instanceof Possibility) {
        Possibility other = (Possibility) o;
        return usedOperations.equals(other.usedOperations) && value.equals(other.value);
      }

      return false;
    }

    public int hashCode() {
      return Objects.hash(usedOperations, value);
    }

    public Optional<Possibility> step(Integer nextStep) {
      KeyOperation operation = operations.get(nextStep);

      Evaluation evaluation = operation.verifyAndReplay(value);

      if (!evaluation.getErrors().isEmpty()) {
        return Optional.empty();
      }

      BitSet newUsedOperations = withAdditionalUsedOperation(nextStep);
      RecordValue newValue = evaluation.getRecordValue();

      intermediateValues.add(newValue);

      return Optional.of(new Possibility(newUsedOperations, newValue));
    }

    private BitSet withAdditionalUsedOperation(Integer operationIndex) {
      BitSet result = (BitSet) usedOperations.clone();
      result.set(operationIndex);
      return result;
    }

    public Set<Integer> findNextSteps() {
      Set<Integer> result = new HashSet<>();

      int firstRemainingOperationIndex = usedOperations.nextClearBit(0);
      long limitingEndTS = 0;

      for (int operationIndex = firstRemainingOperationIndex; operationIndex < operations.size(); operationIndex = usedOperations.nextClearBit(operationIndex + 1)) {
        KeyOperation operation = operations.get(operationIndex);

        if (operationIndex == firstRemainingOperationIndex) {
          limitingEndTS = operation.getEndTS();
        }

        long startTS = operation.getStartTS();
        if (startTS <= limitingEndTS) {
          result.add(operationIndex);
        }
      }

      return result;
    }
  }
}
