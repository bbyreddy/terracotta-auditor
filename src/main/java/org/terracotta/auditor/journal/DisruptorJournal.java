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

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.util.concurrent.TimeUnit;

/**
 * {@link Journal} implementation based on the LMAX disruptor.
 */
public class DisruptorJournal implements Journal {

  private static final Logger LOGGER = LoggerFactory.getLogger(DisruptorJournal.class);

  public static final int RING_BUFFER_SIZE = 2 << 16;
  private volatile Writer writer;
  private volatile Disruptor<JournalEntry> disruptor;

  // only the event handler thread is going to touch those variables
  private boolean atCapacity = false;
  private long atCapacityTimestamp;
  private long cumulativeLostNs = 0L;

  public DisruptorJournal(File file) throws Exception {
    init(file);
  }

  private void init(File file) throws Exception {
    file.getParentFile().mkdirs();
    writer = new BufferedWriter(new FileWriter(file));
    disruptor = new Disruptor<>(JournalEntry::new, RING_BUFFER_SIZE, (Runnable r) -> new Thread(r, "logger-disruptor-thread"), ProducerType.MULTI, new BlockingWaitStrategy());
    disruptor.handleEventsWith((EventHandler<JournalEntry>) (event, sequence, endOfBatch) -> {
      writer.write(Long.toString(event.getStart()));
      writer.write(';');
      writer.write(Long.toString(event.getEnd()));
      writer.write(';');
      writer.write(event.getOperationName());
      writer.write(';');
      writer.write(event.getKey());
      writer.write(';');
      writer.write(event.getResult());
      writer.write('\n');

      // estimate the time the ring buffer was at capacity and blocked producer threads
      long capacityLeft = disruptor.getRingBuffer().remainingCapacity();
      if (!atCapacity && capacityLeft == 0) {
        atCapacityTimestamp = System.nanoTime();
        atCapacity = true;
      }
      if (atCapacity && capacityLeft > 0) {
        long now = System.nanoTime();
        cumulativeLostNs += (now - atCapacityTimestamp);
        atCapacity = false;
      }

    });
    disruptor.start();
  }

  @Override
  public void close() throws Exception {
    while (disruptor.getRingBuffer().remainingCapacity() != RING_BUFFER_SIZE) {
      Thread.sleep(100);
    }

    Disruptor<JournalEntry> toStop = disruptor;
    disruptor = null;
    toStop.shutdown();
    writer.close();
    LOGGER.info("Time lost due to journal back pressure : {} ms", TimeUnit.NANOSECONDS.toMillis(cumulativeLostNs));
  }

  @Override
  public void log(long start, long end, String operationName, String key, String result) {
    disruptor.getRingBuffer().publishEvent((event, sequence) -> event.fillWith(start, end, operationName, key, result));
  }
}
