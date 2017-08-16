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
package org.terracotta.auditor.journal;

/**
 * A journal must provide a thread-safe storage that can later be re-read, akin to a write-ahead log except
 * that persistence and crash-safety are both optional.
 *
 * Ideally, implementations should focus on write performance and should also make the best possible effort to
 * keep the logs ordered by the operations' timestamps.
 */
public interface Journal extends AutoCloseable {

  void log(long start, long end, String operationName, String key, String result);

}
