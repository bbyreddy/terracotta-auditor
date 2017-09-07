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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class Verifier {

  private static final Logger LOGGER = LoggerFactory.getLogger(Verifier.class);

  private final Reader journalReader;
  private final int windowSize;
  private final Function<String, Operation> operationParser;

  public Verifier(File journalFile, int windowSize, Function<String, Operation> operationParser) throws IOException {
    this(new FileReader(journalFile), windowSize, operationParser);
  }

  public Verifier(Reader journalReader, int windowSize, Function<String, Operation> operationParser) throws IOException {
    this.journalReader = journalReader;
    this.windowSize = windowSize;
    this.operationParser = operationParser;
  }

  public List<String> verify() {
    return verify((x) -> Collections.emptyList());
  }

  public List<String> verify(Function<Map<String, Set<RecordValue>>, List<String>> extraCheck) {
    try {
      List<String> errors = new ArrayList<>();
      long before = System.nanoTime();
      long lineCount = 0L;
      GlobalTimeline timeline = new GlobalTimeline(windowSize);

      try (BufferedReader br = new BufferedReader(journalReader)) {
        while (true) {
          String line = br.readLine();
          if (line == null) {
            break;
          }
          lineCount++;

          Operation operation = operationParser.apply(line);
          timeline.add(operation);

          if (timeline.remainingCapacity() == 0) {
            // timeline is full, process the next op(s) to make some room
            try {
              timeline.step();
            } catch (VerificationException e) {
              errors.add(e.getMessage());
            }
          }

          if ((lineCount % 25000) == 0) {
            LOGGER.debug("Processed {}", lineCount);
          }
        }
      }

      // finished file parsing, process what remains in the timeline
      LOGGER.debug("finished file parsing, process what remains in the timeline");
      while (!timeline.isEmpty()) {
        try {
          timeline.step();
        } catch (VerificationException e) {
          errors.add(e.getMessage());
        }
      }

      Map<String, Set<RecordValue>> results = timeline.getResults();
      List<String> extraErrors = extraCheck.apply(results);
      errors.addAll(extraErrors);

      long after = System.nanoTime();
      LOGGER.info("Verification of {} entries done in {} s - {} error(s)", lineCount, TimeUnit.NANOSECONDS.toSeconds(after - before), errors.size());
      return errors;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
