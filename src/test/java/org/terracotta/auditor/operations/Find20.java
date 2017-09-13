/*
 * Copyright (c) 2011-2017 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
 * Use, reproduction, transfer, publication or disclosure is prohibited except as specifically provided for in your License Agreement with Software AG.
 */
package org.terracotta.auditor.operations;

import org.terracotta.auditor.verifier.NonKeyOperation;
import org.terracotta.auditor.verifier.RecordValue;
import org.terracotta.auditor.verifier.SorHistory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Ludovic Orban
 */
public class Find20 extends NonKeyOperation {

  private final Set<String> journalKeys = new HashSet<>();
  private final Map<String, String> journalRecords;

  public Find20(long startTS, long endTS, String keys, String result) {
    super("Find20", startTS, endTS, result);
    String[] split = keys.substring(1, keys.length() - 1).split(",");
    for (String s : split) {
      journalKeys.add(s.trim());
    }
    journalRecords = parseToMap(result);
  }

  @Override
  public String verifyAndReplay(SorHistory from) {
    List<String> errors = new ArrayList<>();
    Map<String, Set<RecordValue>> at = from.getAt(getStartTS());
    Map<String, Set<RecordValue>> overlapping = from.getEverythingOverlapping(getStartTS(), getEndTS());

    for (String key : journalKeys) {
      String journalRecordValue = journalRecords.get(key);
      Set<RecordValue> datasetRecordValues = at.get(key);
      Set<RecordValue> overlappingRecordValues = overlapping.get(key);

      if (journalRecordValue == null) {
        if ((datasetRecordValues != null && overlappingRecordValues != null)) {
          if (!datasetRecordValues.contains(RecordValue.ABSENT) && !overlappingRecordValues.contains(RecordValue.ABSENT)) {
            errors.add("Find20 error at startTs=" + getStartTS() + ": record present but should be absent at key " + key);
          }
        }
      } else {
        boolean unknownPossible = false;
        if ((datasetRecordValues != null && datasetRecordValues.contains(RecordValue.UNKNOWN_PRESENT)) || (overlappingRecordValues != null && overlappingRecordValues.contains(RecordValue.UNKNOWN_PRESENT))) {
          unknownPossible = true;
        }

        Set<String> everythingPossibleInJournal = new HashSet<>();
        if (datasetRecordValues != null) {
          for (RecordValue datasetRecordValue : datasetRecordValues) {
            if (!datasetRecordValue.isUnknown() && !datasetRecordValue.isAbsent()) {
              everythingPossibleInJournal.add(datasetRecordValue.getResult());
            }
          }
        }
        if (overlappingRecordValues != null) {
          for (RecordValue overlappingRecordValue : overlappingRecordValues) {
            if (!overlappingRecordValue.isUnknown() && !overlappingRecordValue.isAbsent()) {
              everythingPossibleInJournal.add(overlappingRecordValue.getResult());
            }
          }
        }

        if (!everythingPossibleInJournal.contains("Optional[" + journalRecordValue + "]") && !unknownPossible) {
          String msg = "Find20 error at startTs=" + getStartTS() + ": journal says: " + journalRecordValue + " but possible values are : " + everythingPossibleInJournal;
          errors.add(msg);
        }
      }
    }

    if (errors.isEmpty()) {
      return null;
    } else {
      return errors.toString();
    }
  }


  private static int findMatching(String s, int startIdx, char opening, char closing) {
    int depth = 0;
    for (int i = startIdx; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c == opening) {
        depth++;
      } else if (c == closing) {
        depth--;
        if (depth == 0) {
          return i;
        }
      }
    }
    return -1;
  }

  private static String findKey(String s) {
    int startIdx = s.indexOf("key=");
    int endIdx = s.indexOf(",", startIdx);
    return s.substring(startIdx + "key=".length(), endIdx);
  }

  private static Map<String, String> parseToMap(String s) {
    Map<String, String> result = new HashMap<>();

    Set<String> records = parse(s);
    for (String record : records) {
      String key = findKey(record);
      result.put(key, record);
    }

    return result;
  }

  private static Set<String> parse(String s) {
    Set<String> result = new HashSet<>();

    int from = 0;
    while (true) {
      int startIdx = s.indexOf("LazyVersionedRecord{", from);
      if (startIdx < 0) {
        break;
      }
      int endIdx = findMatching(s, startIdx, '{', '}') + 1;
      String record = s.substring(startIdx, endIdx);
      result.add(record);
      from = endIdx;
    }

    return result;
  }

}
