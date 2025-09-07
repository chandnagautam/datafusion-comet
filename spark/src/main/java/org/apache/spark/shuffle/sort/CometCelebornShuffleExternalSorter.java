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

package org.apache.spark.shuffle.sort;

import java.io.IOException;
import java.util.LinkedList;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.ShuffleClient;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.memory.SparkOutOfMemoryError;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.shuffle.comet.CometShuffleMemoryAllocatorTrait;
import org.apache.spark.shuffle.comet.TooLargePageException;
import org.apache.spark.sql.comet.execution.shuffle.CelebornSpillWriter;
import org.apache.spark.sql.comet.execution.shuffle.ShuffleThreadPool;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.UnsafeAlignedOffset;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.util.Utils;

import org.apache.comet.CometConf$;
import org.apache.comet.Native;

/** CometCelebornShuffleExternalSorter */
public class CometCelebornShuffleExternalSorter {
  private static final Logger logger =
      LoggerFactory.getLogger(CometCelebornShuffleExternalSorter.class);

  public static int MAXIMUM_PAGE_SIZE_BYTES = PackedRecordPointer.MAXIMUM_PAGE_SIZE_BYTES;

  private final int numPartitions;
  private final TaskContext taskContext;
  private final ShuffleWriteMetricsReporter writeMetrics;

  private final StructType schema;

  /** Force this sorter to spill when there are this many elements in memory. */
  private final int numElementsForSpillThreshold;

  // When this external sorter allocates memory of `sorterArray`, we need to keep
  // its
  // assigned initial size. After spilling, we will reset the array to its initial
  // size.
  // See `sorterArray` comment for more details.
  private int initialSize;

  private final ConcurrentLinkedQueue<CelebornSpillSorter> spillingSorters =
      new ConcurrentLinkedQueue<>();

  private final CometShuffleMemoryAllocatorTrait allocator;

  private final LinkedList<SpillInfo> spills = new LinkedList<>();

  private long peakMemoryUsedBytes;

  private final String compressionCodec;
  private final int compressionLevel;

  /** Whether to write shuffle spilling file in async mode */
  private final boolean isAsync;

  /** Thread pool shared for async spilling write */
  private final ExecutorService threadPool;

  private final int threadNum;

  private ConcurrentLinkedQueue<Future<Void>> asyncSpillTasks = new ConcurrentLinkedQueue<>();

  private boolean spilling = false;

  private final int uaoSize = UnsafeAlignedOffset.getUaoSize();
  private final double preferDictionaryRatio;
  private final boolean tracingEnabled;
  private CelebornSpillSorter activeSpillSorter;

  private int mapId;
  private int shuffleId;
  private int numMappers;
  private int attemptId;

  private ShuffleClient shuffleClient;

  public CometCelebornShuffleExternalSorter(
      int numPartitions,
      TaskContext taskContext,
      StructType schema,
      CometShuffleMemoryAllocatorTrait allocator,
      ShuffleWriteMetricsReporter writeMetrics,
      int initialSize,
      ShuffleClient shuffleClient,
      int mapId,
      int shuffleId,
      int numMappers,
      int attemptId) {
    this.numPartitions = numPartitions;
    this.taskContext = taskContext;
    this.schema = schema;
    this.allocator = allocator;
    this.writeMetrics = writeMetrics;

    this.numElementsForSpillThreshold =
        (int) CometConf$.MODULE$.COMET_COLUMNAR_SHUFFLE_SPILL_THRESHOLD().get();

    this.initialSize = initialSize;

    this.peakMemoryUsedBytes = getMemoryUsage();

    this.shuffleClient = shuffleClient;

    this.compressionCodec = CometConf$.MODULE$.COMET_EXEC_SHUFFLE_COMPRESSION_CODEC().get();
    this.compressionLevel =
        (int) CometConf$.MODULE$.COMET_EXEC_SHUFFLE_COMPRESSION_ZSTD_LEVEL().get();

    this.isAsync = (boolean) CometConf$.MODULE$.COMET_COLUMNAR_SHUFFLE_ASYNC_ENABLED().get();
    this.tracingEnabled = (boolean) CometConf$.MODULE$.COMET_TRACING_ENABLED().get();

    if (isAsync) {
      this.threadNum = (int) CometConf$.MODULE$.COMET_COLUMNAR_SHUFFLE_ASYNC_THREAD_NUM().get();
      assert (this.threadNum > 0);
      this.threadPool = ShuffleThreadPool.getThreadPool();
    } else {
      this.threadNum = 0;
      this.threadPool = null;
    }

    this.activeSpillSorter = new CelebornSpillSorter();

    this.preferDictionaryRatio =
        (double) CometConf$.MODULE$.COMET_SHUFFLE_PREFER_DICTIONARY_RATIO().get();

    this.mapId = mapId;
    this.shuffleId = shuffleId;
    this.numMappers = numMappers;
    this.attemptId = attemptId;
  }

  private long getMemoryUsage() {
    throw new UnsupportedOperationException("Unimplemented method 'getMemoryUsage'");
  }

  class CelebornSpillSorter extends CelebornSpillWriter {
    private boolean freed = false;

    private SpillInfo spillInfo;

    // These variables are reset after spilling:
    @Nullable private ShuffleInMemorySorter inMemSorter;

    // This external sorter can call native code to sort partition ids and record
    // pointers of rows.
    // In order to do that, we need pass the address of the internal array in the
    // sorter to native.
    // But we cannot access it as it is private member in the Spark sorter. Instead,
    // we allocate
    // the array and assign the pointer array in the sorter.
    private LongArray sorterArray;

    CelebornSpillSorter() {
      this.spillInfo = null;

      this.allocator = CometCelebornShuffleExternalSorter.this.allocator;
      this.shuffleClient = CometCelebornShuffleExternalSorter.this.shuffleClient;
      this.mapId = CometCelebornShuffleExternalSorter.this.mapId;
      this.partitionNum = CometCelebornShuffleExternalSorter.this.numPartitions;
      this.attemptId = CometCelebornShuffleExternalSorter.this.attemptId;
      this.mappersNum = CometCelebornShuffleExternalSorter.this.numMappers;
      this.attemptId = CometCelebornShuffleExternalSorter.this.attemptId;
      this.shuffleId = CometCelebornShuffleExternalSorter.this.shuffleId;

      try {
        this.inMemSorter = new ShuffleInMemorySorter(allocator, 1, true);
      } catch (java.lang.IllegalAccessError e) {
        throw new java.lang.RuntimeException(
            "Error loading in-memory sorter check class path -- see "
                + "https://github.com/apache/arrow-datafusion-comet?tab=readme-ov-file#enable-comet-shuffle",
            e);
      }
      sorterArray = allocator.allocateArray(initialSize);
      this.inMemSorter.expandPointerArray(sorterArray);

      this.allocatedPages = new LinkedList<>();

      this.nativeLib = new Native();
      this.dataTypes = serializeSchema(schema);
    }

    @Override
    protected void spill(int required) throws IOException {
      CometCelebornShuffleExternalSorter.this.spill();
    }

    public void writeSortedNative(boolean tracingEnabled) {
      long arrayAddr = this.sorterArray.getBaseOffset();

      int pos = inMemSorter.numRecords();
      nativeLib.sortRowPartitionsNative(arrayAddr, pos, tracingEnabled);

      ShuffleInMemorySorter.ShuffleSorterIterator sortedRecords =
          new ShuffleInMemorySorter.ShuffleSorterIterator(pos, this.sorterArray, 0);

      if (!sortedRecords.hasNext()) {
        return;
      }

      int currentPartition = -1;
      final RowPartition rowPartition = new RowPartition(initialSize);

      while (sortedRecords.hasNext()) {
        sortedRecords.loadNext();

        final int partition = sortedRecords.packedRecordPointer.getPartitionId();
        assert (partition >= currentPartition);

        if (partition != currentPartition) {
          if (partition != -1) {
            long written =
                doSpilling(
                    dataTypes,
                    rowPartition,
                    writeMetrics,
                    preferDictionaryRatio,
                    compressionCodec,
                    compressionLevel,
                    tracingEnabled);

            spillInfo.partitionLengths[currentPartition] = written;
          }

          currentPartition = partition;
        }

        final long recordPointer = sortedRecords.packedRecordPointer.getRecordPointer();
        final long recordOffsetInPage = allocator.getOffsetInPage(recordPointer);

        // Note that we need to skip over record key (partition id)
        // Note that we already use off-heap memory for serialized rows, so recordPage
        // is always
        // null.
        int recordSizeInBytes = UnsafeAlignedOffset.getSize(null, recordOffsetInPage) - 4;
        long recordReadPosition = recordOffsetInPage + uaoSize + 4; // skip over record length too
        rowPartition.addRow(recordReadPosition, recordSizeInBytes);
      }

      if (currentPartition != -1) {
        long written =
            doSpilling(
                dataTypes,
                rowPartition,
                writeMetrics,
                preferDictionaryRatio,
                compressionCodec,
                compressionLevel,
                tracingEnabled);

        spillInfo.partitionLengths[currentPartition] = written;

        synchronized (spills) {
          spills.add(spillInfo);
        }
      }

      synchronized (writeMetrics) {
        writeMetrics.incRecordsWritten(((ShuffleWriteMetrics) writeMetrics).recordsWritten());
        taskContext
            .taskMetrics()
            .incDiskBytesSpilled(((ShuffleWriteMetrics) writeMetrics).bytesWritten());
      }
    }

    public boolean hasSpaceForAnotherRecord() {
      return inMemSorter.hasSpaceForAnotherRecord();
    }

    public void expandPointerArray(LongArray newArray) {
      inMemSorter.expandPointerArray(newArray);
      this.sorterArray = newArray;
    }

    public void insertRecord(Object recordBase, long recordOffset, int length, int partitionId) {
      final Object base = currentPage.getBaseObject();
      final long recordAddress = allocator.encodePageNumberAndOffset(currentPage, pageCursor);
      UnsafeAlignedOffset.putSize(base, pageCursor, length);
      pageCursor += uaoSize;
      Platform.copyMemory(recordBase, recordOffset, base, pageCursor, length);
      pageCursor += length;
      inMemSorter.insertRecord(recordAddress, partitionId);
    }

    @Override
    public long getMemoryUsage() {
      // We need to synchronize here because we may free the memory pages in another
      // thread,
      // i.e. when spilling, but this method may be called in the task thread.
      synchronized (this) {
        long totalPageSize = super.getMemoryUsage();

        if (freed) {
          return totalPageSize;
        } else {
          return ((inMemSorter == null) ? 0 : inMemSorter.getMemoryUsage()) + totalPageSize;
        }
      }
    }

    /** Free the pointer array held by this sorter. */
    public void freeArray() {
      synchronized (this) {
        inMemSorter.free();
        freed = true;
      }
    }

    /**
     * Reset the in-memory sorter's pointer array only after freeing up the memory pages holding the
     * records.
     */
    public void reset() {
      // We allocate pointer array outside the sorter.
      // So we can get array address which can be used by native code.
      inMemSorter.reset();
      sorterArray = allocator.allocateArray(initialSize);
      inMemSorter.expandPointerArray(sorterArray);
    }

    void setSpillInfo(SpillInfo spillInfo) {
      this.spillInfo = spillInfo;
    }

    public int numRecords() {
      return this.inMemSorter.numRecords();
    }
  }

  public void spill() {
    if (spilling || activeSpillSorter == null || activeSpillSorter.numRecords() == 0) {
      return;
    }

    spilling = true;

    logger.info(
        "Thread {} spilling sort data of {} to disk ({} {} so far)",
        Thread.currentThread().getId(),
        Utils.bytesToString(getMemoryUsage()),
        spills.size(),
        spills.size() > 1 ? " times" : " time");

    if (isAsync) {
      CelebornSpillSorter spillingSorter = activeSpillSorter;
      Callable<Void> task =
          () -> {
            spillingSorter.writeSortedNative(tracingEnabled);

            final long spillSize = spillingSorter.freeMemory();
            spillingSorter.freeArray();
            spillingSorters.remove(spillingSorter);
            synchronized (CometCelebornShuffleExternalSorter.this) {
              taskContext.taskMetrics().incMemoryBytesSpilled(spillSize);
            }
            return null;
          };

      spillingSorters.add(spillingSorter);
      asyncSpillTasks.add(threadPool.submit(task));

      while (asyncSpillTasks.size() == threadNum) {
        for (Future<Void> spillingTask : asyncSpillTasks) {
          if (spillingTask.isDone()) {
            asyncSpillTasks.remove(spillingTask);
            break;
          }
        }
      }

      activeSpillSorter = new CelebornSpillSorter();
    } else {
      activeSpillSorter.writeSortedNative(tracingEnabled);
      final long spillSize = activeSpillSorter.freeMemory();

      activeSpillSorter.reset();

      synchronized (CometCelebornShuffleExternalSorter.this) {
        taskContext.taskMetrics().incMemoryBytesSpilled(spillSize);
      }
    }
    spilling = false;
  }

  private void updatePeakMemoryUsed() {
    long mem = getMemoryUsage();
    if (mem > peakMemoryUsedBytes) {
      peakMemoryUsedBytes = mem;
    }
  }

  /** Return the peak memory used so far, in bytes. */
  public long getPeakMemoryUsedBytes() {
    updatePeakMemoryUsed();
    return peakMemoryUsedBytes;
  }

  private long freeMemory() {
    updatePeakMemoryUsed();
    long memoryFreed = 0;
    if (isAsync) {
      for (CelebornSpillSorter sorter : spillingSorters) {
        memoryFreed += sorter.freeMemory();
        sorter.freeArray();
      }
    }
    memoryFreed += activeSpillSorter.freeMemory();
    activeSpillSorter.freeArray();

    return memoryFreed;
  }

  public void cleanupResources() {
    freeMemory();
  }

  /**
   * Checks whether there is enough space to insert an additional record in to the sort pointer
   * array and grows the array if additional space is required. If the required space cannot be
   * obtained, then the in-memory data will be spilled to disk.
   */
  private void growPointerArrayIfNecessary() throws IOException {
    assert (activeSpillSorter != null);
    if (!activeSpillSorter.hasSpaceForAnotherRecord()) {
      long used = activeSpillSorter.getMemoryUsage();
      LongArray array;
      try {
        // could trigger spilling
        array = allocator.allocateArray(used / 8 * 2);
      } catch (TooLargePageException e) {
        // The pointer array is too big to fix in a single page, spill.
        spill();
        return;
      } catch (SparkOutOfMemoryError e) {
        // Cannot allocate enough memory, spill and reset pointer array.
        try {
          spill();
        } catch (SparkOutOfMemoryError e2) {
          // Cannot allocate memory even after spilling, throw the error.
          if (!activeSpillSorter.hasSpaceForAnotherRecord()) {
            logger.error("Unable to grow the pointer array");
            throw e2;
          }
        }
        return;
      }
      // check if spilling is triggered or not
      if (activeSpillSorter.hasSpaceForAnotherRecord()) {
        allocator.freeArray(array);
      } else {
        activeSpillSorter.expandPointerArray(array);
      }
    }
  }

  public void insertRecord(Object recordBase, long recordOffset, int length, int partitionId)
      throws IOException {
    assert (activeSpillSorter != null);
    int threshold = numElementsForSpillThreshold;

    if (activeSpillSorter.numRecords() >= threshold) {
      logger.info(
          "Spilling data because number of spilledRecords crossed the threshold " + threshold);
      spill();
    }

    growPointerArrayIfNecessary();

    // Need 4 or 8 bytes to store the record length.
    final int required = length + uaoSize;
    // Acquire enough memory to store the record.
    // If we cannot acquire enough memory, we will spill current writers.
    if (!activeSpillSorter.acquireNewPageIfNecessary(required)) {
      // Spilling is happened, initiate new memory page for new writer.
      activeSpillSorter.initialCurrentPage(required);
    }

    activeSpillSorter.insertRecord(recordBase, recordOffset, length, partitionId);
  }

  /**
   * Close the sorter, causing any buffered data to be sorted and written out to disk.
   *
   * @return metadata for the spill files written by this sorter. If no records were ever inserted
   *     into this sorter, then this will return an empty array.
   */
  public SpillInfo[] closeAndGetSpills() throws IOException {
    if (activeSpillSorter != null) {
      // Do not count the final file towards the spill count.
      final SpillInfo spillInfo = new SpillInfo(numPartitions, null, null);

      // Waits for all async tasks to finish.
      if (isAsync) {
        for (Future<Void> task : asyncSpillTasks) {
          try {
            task.get();
          } catch (Exception e) {
            throw new IOException(e);
          }
        }

        asyncSpillTasks.clear();
      }

      activeSpillSorter.setSpillInfo(spillInfo);
      activeSpillSorter.writeSortedNative(tracingEnabled);

      freeMemory();
    }

    return spills.toArray(new SpillInfo[spills.size()]);
  }
}
