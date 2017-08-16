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
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * @author Ludovic Orban
 */
public class SorHistory {

  private Map<String, SortedMap<Interval, Set<RecordValue>>> history = new HashMap<>();

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

  public boolean containsEverythingBetween(long beforeTs, long afterTs) {
    throw new UnsupportedOperationException();
  }

  public Map<String, Set<RecordValue>> getEverythingBetween(long beforeTs, long afterTs) {
    throw new UnsupportedOperationException();
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
      if (endTs > other.endTs) {
        return 1;
      } else if (endTs < other.endTs) {
        return -1;
      } else {
        return 0;
      }
    }
  }

}
