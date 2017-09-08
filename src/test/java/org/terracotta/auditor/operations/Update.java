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
public class Update extends KeyOperation {

  public Update(long startTS, long endTS, String key, String result) {
    super("Update", startTS, endTS, key, result);
  }

  @Override
  public Evaluation verifyAndReplay(RecordValue fromValue) {
    Evaluation evaluation = new Evaluation();

    // verify
    if (fromValue.isAbsent()) {
      if (getResult().equals("true")) {
        evaluation.addError("ERROR: screwed up Update on absent");
      }
    } else {
      if (getResult().equals("false")) {
        evaluation.addError("ERROR: screwed up Update on present");
      }
    }

    if (!fromValue.isAbsent()) {
      evaluation.setRecordValue(RecordValue.UNKNOWN_PRESENT);
    } else {
      evaluation.setRecordValue(fromValue);
    }

    return evaluation;
  }
}
