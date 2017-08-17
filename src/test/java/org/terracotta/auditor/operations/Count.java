/*
 * Copyright (c) 2011-2017 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
 * Use, reproduction, transfer, publication or disclosure is prohibited except as specifically provided for in your License Agreement with Software AG.
 */
package org.terracotta.auditor.operations;

import org.terracotta.auditor.verifier.NonKeyOperation;
import org.terracotta.auditor.verifier.RecordValue;
import org.terracotta.auditor.verifier.SorHistory;

import java.util.Map;
import java.util.Set;

/**
 * @author Ludovic Orban
 */
public class Count extends NonKeyOperation {

  public Count(long startTS, long endTS, String result) {
    super("Count", startTS, endTS, result);
  }

  @Override
  public String verifyAndReplay(SorHistory from) {
    int minPossible = 0;
    int maxPossible = 0;

    Map<String, Set<RecordValue>> firmValues = from.getAt(getEndTS());
    for (Set<RecordValue> recordValues : firmValues.values()) {
      boolean extraMin = !recordValues.isEmpty();
      boolean extraMax = false;
      for (RecordValue recordValue : recordValues) {
        if (!recordValue.isAbsent()) {
          extraMax = true;
        }
        extraMin &= (!recordValue.isAbsent());
      }
      if (extraMin) minPossible++;
      if (extraMax) maxPossible++;
    }


    Map<String, Set<RecordValue>> overlappingValues = from.getEverythingBetween(getStartTS(), getEndTS());
    for (Map.Entry<String, Set<RecordValue>> stringSetEntry : overlappingValues.entrySet()) {
      String key = stringSetEntry.getKey();
      Set<RecordValue> firmVals = firmValues.get(key);
      Set<RecordValue> overlappingVals = stringSetEntry.getValue();

      for (RecordValue firmVal : firmVals) {
        for (RecordValue overlappingVal : overlappingVals) {
          if (firmVal.isAbsent() && !overlappingVal.isAbsent()) {
            maxPossible++;
          }
          if (!firmVal.isAbsent() && overlappingVal.isAbsent()) {
            minPossible--;
          }
        }
      }
    }


    int result = Integer.parseInt(getResult());

    if (result >= minPossible && result <= maxPossible) {
      return null;
    } else {
      return "Count error: min possible is " + minPossible + " and max possible is " + maxPossible + " but journal said : " + result;
    }
  }
}
