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

import java.util.Objects;

public class RecordValue {
  public static final RecordValue ABSENT = new RecordValue(null, true, false);
  public static final RecordValue UNKNOWN_PRESENT = new RecordValue(null, false, true);

  private final String result;
  private final boolean absent;
  private final boolean unknown;

  public RecordValue(String result) {
    this(result, false, false);
  }

  private RecordValue(String result, boolean absent, boolean unknown) {
    this.result = result;
    this.absent = absent;
    this.unknown = unknown;
  }

  public boolean isAbsent() {
    return absent;
  }

  public boolean isUnknown() {
    return unknown;
  }

  public String getResult() {
    return result;
  }

  @Override
  public String toString() {
    if (absent) {
      return "absent";
    }
    if (unknown) {
      return "unknown";
    }
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof RecordValue)) {
      return false;
    }

    RecordValue other = (RecordValue) o;

    if (this == ABSENT) {
      return other == ABSENT;
    }

    if (this == UNKNOWN_PRESENT) {
      return other == UNKNOWN_PRESENT;
    }

    return Objects.equals(result, other.result);
  }

  @Override
  public int hashCode() {
    return Objects.hash(absent, unknown, result);
  }
}
