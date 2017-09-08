/*
 * Copyright (c) 2011-2017 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
 * Use, reproduction, transfer, publication or disclosure is prohibited except as specifically provided for in your License Agreement with Software AG.
 */
package org.terracotta.auditor.operations;

import org.terracotta.auditor.verifier.Evaluation;
import org.terracotta.auditor.verifier.KeyOperation;
import org.terracotta.auditor.verifier.Operation;
import org.terracotta.auditor.verifier.RecordValue;

/**
 * @author Ludovic Orban
 */
public class Get extends KeyOperation {

  public Get(long startTS, long endTS, String key, String result) {
    super("Get", startTS, endTS, key, result);
  }

  @Override
  public Evaluation verifyAndReplay(RecordValue fromValue) {
    Evaluation evaluation = new Evaluation();

    // verify
    if (fromValue.isAbsent()) {
      if (!getResult().equals("Optional.empty")) {
        evaluation.addError("ERROR: screwed up Get on absent");
      }
    } else {
      if (getResult().equals("Optional.empty")) {
        evaluation.addError("ERROR: screwed up Get on present");
      }
      if (!fromValue.isUnknown() && !getResult().equals(fromValue.getResult())) {
        evaluation.addError("ERROR: screwed up Get : unexpected value");
      }
    }

    if (fromValue.isUnknown()) {
      evaluation.setRecordValue(new RecordValue(getResult()));
    } else {
      evaluation.setRecordValue(fromValue);
    }

    return evaluation;
  }
}
