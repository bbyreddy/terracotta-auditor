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
package org.terracotta.auditor.journal.merge;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.stream.BaseStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class JournalMerger {
  private final Path[] inputPaths;

  public JournalMerger(File... inputFiles) {
    this((Path[]) Arrays.stream(inputFiles).map(File::toPath).toArray(Path[]::new));
  }

  public JournalMerger(Path... inputPaths) {
    this.inputPaths = inputPaths;
  }

  public void mergeTo(File outputFile) throws Exception {
    mergeTo(outputFile.toPath());
  }

  public void mergeTo(Path outputPath) throws Exception {
    Stream<String>[] streams = (Stream<String>[]) Arrays.stream(inputPaths).map(JournalMerger::pathToStream).toArray(Stream[]::new);
    Stream<String> mergedStreams = mergeStreams(new LogEntryComparator(), streams);
    Files.write(outputPath, (Iterable<String>) mergedStreams::iterator);
  }

  private static <T> Stream<T> mergeStreams(Comparator<T> comparator, Stream<T>... streams) {
    Iterator<T>[] iterators = (Iterator<T>[]) Arrays.stream(streams).map(BaseStream::iterator).toArray(Iterator[]::new);
    Iterator<T> mergeIterator = new MergeIterator<T>(comparator, iterators);
    Spliterator<T> spliterator = new IteratorSpliterator<>(mergeIterator);
    return StreamSupport.stream(spliterator, false);
  }

  private static Stream<String> pathToStream(Path path) {
    try {
      return Files.lines(path);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
