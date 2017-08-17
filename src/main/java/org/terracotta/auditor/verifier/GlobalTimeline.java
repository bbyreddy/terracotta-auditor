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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class GlobalTimeline {
  private final int maxSize;
  private final Map<String, KeyTimeline> timelineMap = new HashMap<>();
  private final List<NonKeyOperation> nonKeyOperations = new ArrayList<>();
  private final SorHistory sorHistory = new SorHistory();
  private int size = 0;

  public GlobalTimeline(int maxSize) {
    this.maxSize = maxSize;
  }

  public void add(Operation operation) {
    if (operation instanceof KeyOperation) {
      add(((KeyOperation) operation));
    } else {
      add(((NonKeyOperation) operation));
    }
  }

  private void add(NonKeyOperation operation) {
    nonKeyOperations.add(operation);
    nonKeyOperations.sort(Utils.operationComparator());
    size++;
  }

  private void add(KeyOperation operation) {
    if (remainingCapacity() == 0) {
      throw new IllegalStateException("Timeline full with " + maxSize + " operations");
    }
    String key = operation.getKey();
    KeyTimeline timeline = timelineMap.get(key);
    if (timeline == null) {
      timeline = new KeyTimeline(operation);
      timelineMap.put(operation.getKey(), timeline);
    } else {
      timeline.add(operation);
    }

    size++;
  }

  public int remainingCapacity() {
    return maxSize - size;
  }

  public boolean isEmpty() {
    return size == 0;
  }

  public void step() throws VerificationException {
    if (size == 0) {
      throw new IllegalStateException("Timeline is empty");
    }

    // first check if some n-key operation can execute
    if (!nonKeyOperations.isEmpty()) {
      NonKeyOperation nonKeyOperation = nonKeyOperations.get(0);
      if (sorHistory.width() == timelineMap.size() && sorHistory.containsEverythingUpTo(nonKeyOperation.getEndTS())) {
        String error = nonKeyOperation.verifyAndReplay(sorHistory);
        nonKeyOperations.remove(0);
        size--;
        if (error != null) {
          throw new VerificationException(error, null);
        }
      }
    }

    KeyTimeline bestTimeline = null;

    // look for the timeline with the most ops in it
    for (KeyTimeline timeline : timelineMap.values()) {
      if (!timeline.isEmpty()) {
        if (bestTimeline == null) {
          bestTimeline = timeline;
        } else if (timeline.size() > bestTimeline.size()) {
          bestTimeline = timeline;
        }
      }
    }

    if (bestTimeline == null) {
      return;
    }

    try {
      StepResult step = bestTimeline.step(sorHistory.getHeadOf(bestTimeline.getKey()));
      size -= step.getStepSize();
      sorHistory.add(bestTimeline.getKey(), step.getStartTs(), step.getEndTs(), step.getPossibleValues());
    } catch (VerificationException ve) {
      StepResult step = ve.getStepResult();
      size -= step.getStepSize();
      sorHistory.add(bestTimeline.getKey(), step.getStartTs(), step.getEndTs(), step.getPossibleValues());
      throw ve;
    }
  }

  Map<String, Set<RecordValue>> getResults() {
    return sorHistory.getHeads();
  }
}
