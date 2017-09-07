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

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * @author Ludovic Orban
 */
public class SorHistory {

  private final Map<String, SortedMap<Interval, Set<RecordValue>>> history = new HashMap<>();

  public void add(String key, long beforeTs, long afterTs, Set<RecordValue> values) {
    SortedMap<Interval, Set<RecordValue>> intervalSetSortedMap = history.get(key);
    if (intervalSetSortedMap == null) {
      Interval interval = new Interval(beforeTs, afterTs);
      SortedMap<Interval, Set<RecordValue>> map = new TreeMap<>();
      map.put(interval, values);
      history.put(key, map);
    } else {
      Interval interval = new Interval(beforeTs, afterTs);
      intervalSetSortedMap.put(interval, values);
    }
  }

  public Set<RecordValue> getHeadOf(String key) {
    SortedMap<Interval, Set<RecordValue>> intervalListSortedMap = history.get(key);
    if (intervalListSortedMap == null) {
      return Collections.singleton(RecordValue.ABSENT);
    }
    Interval lastKey = intervalListSortedMap.lastKey();
    return intervalListSortedMap.get(lastKey);
  }

  public Map<String, Set<RecordValue>> getHeads() {
    Map<String, Set<RecordValue>> result = new HashMap<>();

    for (String key : history.keySet()) {
      SortedMap<Interval, Set<RecordValue>> intervalSetSortedMap = history.get(key);
      Interval interval = intervalSetSortedMap.lastKey();
      Set<RecordValue> recordValues = intervalSetSortedMap.get(interval);
      result.put(key, recordValues);
    }

    return result;
  }

  public void deleteUntil(long ts) {
    for (Map.Entry<String, SortedMap<Interval, Set<RecordValue>>> stringSortedMapEntry : history.entrySet()) {
      SortedMap<Interval, Set<RecordValue>> value = stringSortedMapEntry.getValue();

      Iterator<Map.Entry<Interval, Set<RecordValue>>> iterator = value.entrySet().iterator();
      // delete everything before the arg TS, but the last value before that TS, hence save the last deleted one to re-add it
      Map.Entry<Interval, Set<RecordValue>> lastEntry = null;
      while (iterator.hasNext()) {
        Map.Entry<Interval, Set<RecordValue>> next = iterator.next();
        Interval interval = next.getKey();
        if (interval.endTs < ts && iterator.hasNext()) {
          lastEntry = next;
          iterator.remove();
        } else {
          if (lastEntry != null) {
            value.put(lastEntry.getKey(), lastEntry.getValue());
          }
          break;
        }
      }
    }
  }

  private long atCount = 0;
  private long atSum = 0;

  private long overlappingCount = 0;
  private long overlappingSum = 0;

  public Map[] getAtAndOverlapping(long startTs, long afterTs) {
    Map<String, Set<RecordValue>> at = new HashMap<>();
    Map<String, Set<RecordValue>> overlapping = new HashMap<>();

    for (Map.Entry<String, SortedMap<Interval, Set<RecordValue>>> stringSortedMapEntry : history.entrySet()) {
      String key = stringSortedMapEntry.getKey();
      SortedMap<Interval, Set<RecordValue>> intervalSetSortedMap = stringSortedMapEntry.getValue();

      // at
      int counter = 0;
      for (Map.Entry<Interval, Set<RecordValue>> intervalSetEntry : intervalSetSortedMap.entrySet()) {
        Interval interval = intervalSetEntry.getKey();
        Set<RecordValue> values = intervalSetEntry.getValue();

        if (interval.endTs > startTs) {
          break;
        }

        at.put(key, values);
        counter++;
      }
      atCount++;
      atSum += counter;

      // overlapping
      counter = 0;
      for (Map.Entry<Interval, Set<RecordValue>> intervalSetEntry : intervalSetSortedMap.entrySet()) {
        Interval interval = intervalSetEntry.getKey();
        Set<RecordValue> value = intervalSetEntry.getValue();

        if (interval.endTs < startTs) {
          continue;
        }
        if (interval.startTs > afterTs) {
          break;
        }

        if (overlapping.putIfAbsent(key, value) != null) {
          Set<RecordValue> recordValues = overlapping.get(key);
          recordValues.addAll(value);
        }
        counter++;
      }
      overlappingCount++;
      overlappingSum += counter;
    }

    at.entrySet().removeIf(next -> next.getValue().size() == 1 && next.getValue().iterator().next().isAbsent());

    return new Map[] {at, overlapping};
  }

  public Map<String, Set<RecordValue>> getAt(long ts) {
    Map<String, Set<RecordValue>> at = new HashMap<>();

    for (Map.Entry<String, SortedMap<Interval, Set<RecordValue>>> stringSortedMapEntry : history.entrySet()) {
      String key = stringSortedMapEntry.getKey();
      SortedMap<Interval, Set<RecordValue>> intervalSetSortedMap = stringSortedMapEntry.getValue();

      int counter = 0;
      for (Map.Entry<Interval, Set<RecordValue>> intervalSetEntry : intervalSetSortedMap.entrySet()) {
        counter++;
        Interval interval = intervalSetEntry.getKey();
        Set<RecordValue> values = intervalSetEntry.getValue();

        if (interval.endTs > ts) {
          break;
        }

        at.put(key, values);
      }
      atCount++;
      atSum += counter;
    }

    at.entrySet().removeIf(next -> next.getValue().size() == 1 && next.getValue().iterator().next().isAbsent());

    return at;
  }

  public Map<String, Set<RecordValue>> getEverythingOverlapping(long startTs, long afterTs) {
    Map<String, Set<RecordValue>> overlapping = new HashMap<>();

    for (Map.Entry<String, SortedMap<Interval, Set<RecordValue>>> stringSortedMapEntry : history.entrySet()) {
      String key = stringSortedMapEntry.getKey();
      SortedMap<Interval, Set<RecordValue>> intervalSetSortedMap = stringSortedMapEntry.getValue();

      int counter = 0;
      for (Map.Entry<Interval, Set<RecordValue>> intervalSetEntry : intervalSetSortedMap.entrySet()) {
        counter++;
        Interval interval = intervalSetEntry.getKey();
        Set<RecordValue> value = intervalSetEntry.getValue();

        if (interval.endTs < startTs) {
          continue;
        }
        if (interval.startTs > afterTs) {
          break;
        }

        if (overlapping.putIfAbsent(key, value) != null) {
          Set<RecordValue> recordValues = overlapping.get(key);
          recordValues.addAll(value);
        }
      }
      overlappingCount++;
      overlappingSum += counter;
    }

    return overlapping;
  }


  static class Interval implements Comparable<Interval> {
    private long startTs;
    private long endTs;

    public Interval(long startTs, long endTs) {
      this.startTs = startTs;
      this.endTs = endTs;
    }

    @Override
    public int hashCode() {
      return (int) (startTs + endTs);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof Interval) {
        Interval other = (Interval) obj;
        return other.startTs == startTs && other.endTs == endTs;
      }
      return false;
    }

    @Override
    public int compareTo(Interval other) {
      return Long.compare(endTs, other.endTs);
    }
  }

  public String averages() {
    return "at avg: " + (atCount == 0 ? "N/A" : (float)atSum / atCount) + " overlapping avg: " + (overlappingCount == 0 ? "N/A" : (float)overlappingSum / overlappingCount);
  }

}
