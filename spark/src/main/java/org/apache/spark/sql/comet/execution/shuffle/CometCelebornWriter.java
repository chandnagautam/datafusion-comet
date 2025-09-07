/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.comet.execution.shuffle;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.shaded.com.google.common.annotations.VisibleForTesting;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.shuffle.comet.CometShuffleMemoryAllocatorTrait;
import org.apache.spark.shuffle.sort.RowPartition;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.UnsafeAlignedOffset;

import org.apache.comet.CometConf$;
import org.apache.comet.Native;

/** CometCelebornWriter */
public class CometCelebornWriter {

  private static final Logger logger = LoggerFactory.getLogger(CometCelebornWriter.class);
  private static final ClassTag<Object> OBJECT_CLASS_TAG = ClassTag$.MODULE$.Object();

  private static final LinkedList<CometCelebornWriter> currentWriters = new LinkedList<>();

  private ConcurrentLinkedQueue<Future<Void>> asyncSpillTasks = new ConcurrentLinkedQueue<>();

  private final LinkedList<CelebornArrowIPCWriter> spillingWriters = new LinkedList<>();

  private final TaskContext taskContext;

  @VisibleForTesting static final int DEFAULT_INITIAL_SER_BUFFER_SIZE = 1024 * 1024;

  // Copied from Spark
  // `org.apache.spark.shuffle.sort.PackedRecordPointer.MAXIMUM_PAGE_SIZE_BYTES`
  static final int MAXIMUM_PAGE_SIZE_BYTES = 1 << 27;

  /** The Comet allocator used to allocate pages. */
  private final CometShuffleMemoryAllocatorTrait allocator;

  /** The serializer used to write rows to memory page. */
  private final SerializerInstance serializer;

  /** The native library used to write rows to disk. */
  private final Native nativeLib;

  private final int uaoSize = UnsafeAlignedOffset.getUaoSize();
  private final StructType schema;
  private final ShuffleWriteMetricsReporter writeMetrics;
  private long totalWritten = 0L;
  private boolean initialized = false;
  private final int columnarBatchSize;
  private final String compressionCodec;
  private final int compressionLevel;
  private final boolean isAsync;
  private final boolean tracingEnabled;
  private final int asyncThreadNum;
  private final ExecutorService threadPool;
  private final int numElementsForSpillThreshold;

  private final double preferDictionaryRatio;

  /** The current active writer. All incoming rows will be inserted into it. */
  private CelebornArrowIPCWriter activeWriter;

  /** A flag indicating whether we are in the process of spilling. */
  private boolean spilling = false;

  /** The buffer used to store serialized row. */
  private ExposedByteArrayOutputStream serBuffer;

  private SerializationStream serOutputStream;

  private long outputRecords = 0;

  private long insertRecords = 0;

  private ShuffleClient shuffleClient;
  private int shuffleId;
  private int mapId;
  private int attemptId;
  private int partitionId;
  private int mappersNum;
  private int partitionNum;

  CometCelebornWriter(
      ShuffleClient shuffleClient,
      CometShuffleMemoryAllocatorTrait allocator,
      TaskContext taskContext,
      SerializerInstance serializer,
      StructType schema,
      ShuffleWriteMetricsReporter writeMetrics,
      boolean isAsync,
      int asyncThreadNum,
      ExecutorService threadPool,
      boolean tracingEnabled,
      int shuffleId,
      int mapId,
      int attemptId,
      int partitionId,
      int mappersNum,
      int partitionNum) {
    this.nativeLib = new Native();
    this.allocator = allocator;
    this.taskContext = taskContext;
    this.serializer = serializer;
    this.schema = schema;
    this.writeMetrics = writeMetrics;
    this.shuffleClient = shuffleClient;
    this.isAsync = isAsync;
    this.asyncThreadNum = asyncThreadNum;
    this.threadPool = threadPool;
    this.tracingEnabled = tracingEnabled;

    this.columnarBatchSize = (int) CometConf$.MODULE$.COMET_COLUMNAR_SHUFFLE_BATCH_SIZE().get();
    this.compressionCodec = CometConf$.MODULE$.COMET_EXEC_SHUFFLE_COMPRESSION_CODEC().get();
    this.compressionLevel =
        (int) CometConf$.MODULE$.COMET_EXEC_SHUFFLE_COMPRESSION_ZSTD_LEVEL().get();

    this.numElementsForSpillThreshold =
        (int) CometConf$.MODULE$.COMET_COLUMNAR_SHUFFLE_SPILL_THRESHOLD().get();

    this.preferDictionaryRatio =
        (double) CometConf$.MODULE$.COMET_SHUFFLE_PREFER_DICTIONARY_RATIO().get();

    this.activeWriter = new CelebornArrowIPCWriter();
    this.partitionId = partitionId;
    this.mapId = mapId;
    this.mappersNum = mappersNum;
    this.partitionNum = partitionNum;
    this.shuffleId = shuffleId;
    this.attemptId = attemptId;

    synchronized (currentWriters) {
      currentWriters.add(this);
    }
  }

  public void setChecksumAlgo(String checksumAlgo) {
    this.activeWriter.setChecksumAlgo(checksumAlgo);
  }

  public void setChecksum(long checksum) {
    this.activeWriter.setChecksum(checksum);
  }

  public long getChecksum() {
    return this.activeWriter.getChecksum();
  }

  public long getOutputRecords() {
    return outputRecords;
  }

  public void insertRow(UnsafeRow row, int partitionId) throws IOException {
    insertRecords++;

    if (!initialized) {
      serBuffer = new ExposedByteArrayOutputStream(DEFAULT_INITIAL_SER_BUFFER_SIZE);
      serOutputStream = serializer.serializeStream(serBuffer);

      initialized = true;
    }

    serBuffer.reset();
    serOutputStream.writeKey(partitionId, OBJECT_CLASS_TAG);
    serOutputStream.writeValue(row, OBJECT_CLASS_TAG);
    serOutputStream.flush();

    final int serializedRecordSize = serBuffer.size();

    assert (serializedRecordSize > 0);

    synchronized (CometCelebornWriter.this) {
      if (activeWriter.numRecords() >= numElementsForSpillThreshold
          || activeWriter.numRecords() >= columnarBatchSize) {
        int threshold = Math.min(numElementsForSpillThreshold, columnarBatchSize);

        logger.info(
            "Spilling data because number of spilledRecords crossed the threshold " + threshold);
        // Spill the current writer
        doSpill(false);
        if (activeWriter.numRecords() != 0) {
          throw new RuntimeException(
              "activeWriter.numRecords()(" + activeWriter.numRecords() + ") != 0");
        }
      }

      // Need 4 or 8 bytes to store the record length.
      final int required = serializedRecordSize + uaoSize;
      // Acquire enough memory to store the record.
      // If we cannot acquire enough memory, we will spill current writers.
      if (!activeWriter.acquireNewPageIfNecessary(required)) {
        // Spilling is happened, initiate new memory page for new writer.
        activeWriter.initialCurrentPage(required);
      }
      activeWriter.insertRecord(
          serBuffer.getBuf(), Platform.BYTE_ARRAY_OFFSET, serializedRecordSize);
    }
  }

  void close() throws IOException {
    if (isAsync) {
      for (Future<Void> task : asyncSpillTasks) {
        try {
          task.get();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }

    totalWritten += activeWriter.doSpilling(true);

    if (outputRecords != insertRecords) {
      throw new RuntimeException(
          "outputRecords("
              + outputRecords
              + ") != insertRecords("
              + insertRecords
              + "). Please file a bug report.");
    }

    serBuffer = null;
    serOutputStream = null;

    activeWriter.freeMemory();

    synchronized (currentWriters) {
      currentWriters.remove(this);
    }
  }

  private void doSpill(boolean forceAsync) throws IOException {
    if (spilling || activeWriter.numRecords() == 0) {
      return;
    }

    spilling = true;

    if (isAsync && !forceAsync) {
      // Remove one of the task to free up memory
      while (asyncSpillTasks.size() == asyncThreadNum) {
        for (Future<Void> task : asyncSpillTasks) {
          if (task.isDone()) {
            asyncSpillTasks.remove(task);

            break;
          }
        }
      }

      final CelebornArrowIPCWriter spillingWriter = activeWriter;
      activeWriter = new CelebornArrowIPCWriter();

      spillingWriters.add(activeWriter);

      asyncSpillTasks.add(
          threadPool.submit(
              new Runnable() {

                @Override
                public void run() {
                  try {
                    long written = spillingWriter.doSpilling(false);
                    totalWritten += written;
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  } finally {
                    spillingWriter.freeMemory();
                    spillingWriters.remove(spillingWriter);
                  }
                }
              },
              null));
    } else {
      synchronized (CometCelebornWriter.this) {
        totalWritten += activeWriter.doSpilling(false);
        activeWriter.freeMemory();
      }
    }

    spilling = false;
  }

  private class CelebornArrowIPCWriter extends CelebornSpillWriter {
    private final RowPartition rowPartition;

    CelebornArrowIPCWriter() {
      this.rowPartition = new RowPartition(columnarBatchSize);

      this.allocatedPages = new LinkedList<>();
      this.allocator = CometCelebornWriter.this.allocator;

      this.nativeLib = CometCelebornWriter.this.nativeLib;
      this.dataTypes = serializeSchema(schema);
      this.shuffleClient = CometCelebornWriter.this.shuffleClient;
      this.mapId = CometCelebornWriter.this.mapId;
      this.shuffleId = CometCelebornWriter.this.shuffleId;
      this.mappersNum = CometCelebornWriter.this.mappersNum;
      this.partitionNum = CometCelebornWriter.this.partitionNum;
      this.attemptId = CometCelebornWriter.this.attemptId;
    }

    int numRecords() {
      return rowPartition.getNumRows();
    }

    /** Inserts a record into current allocated page. */
    void insertRecord(Object recordBase, long recordOffset, int length) {
      // This `ArrowIPCWriter` could be spilled by other threads, so we need to
      // synchronize it.
      final Object base = currentPage.getBaseObject();

      // Add row addresses
      final long recordAddress = allocator.encodePageNumberAndOffset(currentPage, pageCursor);
      rowPartition.addRow(allocator.getOffsetInPage(recordAddress) + uaoSize + 4, length - 4);

      // Write the record (row) size
      UnsafeAlignedOffset.putSize(base, pageCursor, length);
      pageCursor += uaoSize;
      // Copy the record (row) data from serialized buffer to page
      Platform.copyMemory(recordBase, recordOffset, base, pageCursor, length);
      pageCursor += length;
    }

    @Override
    protected void spill(int required) throws IOException {
      synchronized (currentWriters) {
        Collections.sort(
            currentWriters,
            new Comparator<CometCelebornWriter>() {
              @Override
              public int compare(CometCelebornWriter lhs, CometCelebornWriter rhs) {
                long lhsMemoryUsage = lhs.getActiveMemoryUsage();
                long rhsMemoryUsage = rhs.getActiveMemoryUsage();
                return Long.compare(rhsMemoryUsage, lhsMemoryUsage);
              }
            });

        long totalFreed = 0;

        for (CometCelebornWriter writer : currentWriters) {
          long used = writer.getActiveMemoryUsage();

          writer.doSpill(true);

          totalFreed += used;

          if (totalFreed >= required) {
            break;
          }
        }
      }
    }

    long doSpilling(boolean isLast) throws IOException {
      final ShuffleWriteMetricsReporter writeMetricsToUse = writeMetrics;

      outputRecords += rowPartition.getNumRows();
      final long written =
          doSpilling(
              dataTypes,
              rowPartition,
              writeMetricsToUse,
              preferDictionaryRatio,
              compressionCodec,
              compressionLevel,
              tracingEnabled);

      synchronized (writeMetrics) {
        if (!isLast) {
          writeMetrics.incRecordsWritten(
              ((ShuffleWriteMetrics) writeMetricsToUse).recordsWritten());
        }
      }

      return written;
    }
  }

  long getActiveMemoryUsage() {
    return activeWriter.getMemoryUsage();
  }

  void freeMemory() {
    for (CelebornArrowIPCWriter writer : spillingWriters) {
      writer.freeMemory();
    }

    activeWriter.freeMemory();
  }
}
