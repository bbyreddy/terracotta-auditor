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
  private final List<KeyTimeline> timelineList = new ArrayList<>();
  private final Map<String, KeyTimeline> timelineMap = new HashMap<>();
  private int size = 0;

  public GlobalTimeline(int maxSize) {
    this.maxSize = maxSize;
  }

  public void add(Operation operation) {
    if (remainingCapacity() == 0) {
      throw new IllegalStateException("Timeline full with " + maxSize + " operations");
    }
    String key = operation.getKey();
    KeyTimeline timeline = timelineMap.get(key);
    if (timeline == null) {
      timeline = new KeyTimeline(operation);
      timelineMap.put(operation.getKey(), timeline);
      timelineList.add(timeline);
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
    KeyTimeline bestTimeline = null;

    // look for the timeline with the most ops in it
    for (KeyTimeline timeline : timelineList) {
      if (!timeline.isEmpty()) {
        if (bestTimeline == null) {
          bestTimeline = timeline;
        } else if (timeline.size() > bestTimeline.size()) {
          bestTimeline = timeline;
        }
      }
    }

    if (bestTimeline == null) {
      throw new IllegalStateException("Timeline is empty");
    }

    try {
      size -= bestTimeline.step();
    } catch (VerificationException ve) {
      size -= ve.getOperationCount();
      throw ve;
    }
  }

  Map<String, Set<RecordValue>> getResults() {
    Map<String, Set<RecordValue>> result = new HashMap<>();
    for (KeyTimeline keyTimeline : timelineList) {
      String key = keyTimeline.getKey();
      Set<RecordValue> possibleValuesAtHead = keyTimeline.getPossibleValuesAtHead();
      result.put(key, possibleValuesAtHead);
    }
    return result;
  }
}
