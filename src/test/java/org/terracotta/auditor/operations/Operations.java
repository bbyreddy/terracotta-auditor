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
package org.terracotta.auditor.operations;

import org.terracotta.auditor.verifier.Operation;

import java.util.function.Function;

/**
 * @author Ludovic Orban
 */
public class Operations {

  public static Function<String, Operation> parser() {
    return (line) -> {
      String[] cols = line.split(";");
      long startTS = Long.parseLong(cols[0]);
      long endTS = Long.parseLong(cols[1]);
      String name = cols[2];
      String key = cols[3];
      String result = cols[4];

      switch (name) {
        case "Add":
          return new Add(startTS, endTS, key, result);
        case "Get":
          return new Get(startTS, endTS, key, result);
        case "Delete":
          return new Delete(startTS, endTS, key, result);
        case "Update":
          return new Update(startTS, endTS, key, result);
        case "Count":
          return new Count(startTS, endTS, result);
        case "Find20":
          return new Find20(startTS, endTS, key, result);
        default:
          throw new UnsupportedOperationException("Unsupported operation : " + name);
      }
    };
  }

}
