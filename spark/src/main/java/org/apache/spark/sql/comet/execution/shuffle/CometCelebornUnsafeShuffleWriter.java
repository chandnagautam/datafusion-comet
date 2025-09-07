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
import javax.annotation.Nullable;

import scala.Option;
import scala.Product2;
import scala.collection.Iterator;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.unsafe.Platform;
import org.apache.spark.Partitioner;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.internal.config.package$;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.BaseShuffleHandle;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.shuffle.celeborn.SparkUtils;
import org.apache.spark.shuffle.comet.CometShuffleMemoryAllocator;
import org.apache.spark.shuffle.comet.CometShuffleMemoryAllocatorTrait;
import org.apache.spark.shuffle.sort.CometCelebornShuffleExternalSorter;
import org.apache.spark.shuffle.sort.CometShuffleExternalSorter;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.BlockManagerId;

import com.google.common.annotations.VisibleForTesting;

import org.apache.comet.CometConf;
import org.apache.comet.Native;

/** CometCelebornUnsafeShuffleWriter */
final class CometCelebornUnsafeShuffleWriter<K, V> extends ShuffleWriter<K, V> {

  @VisibleForTesting static final int DEFAULT_INITIAL_SER_BUFFER_SIZE = 1024 * 1024;
  private static final Logger logger = LoggerFactory.getLogger(CometUnsafeShuffleWriter.class);
  private static final ClassTag<Object> OBJECT_CLASS_TAG = ClassTag$.MODULE$.Object();
  private final TaskMemoryManager memoryManager;
  private final SerializerInstance serializer;
  private final Partitioner partitioner;
  private final ShuffleWriteMetricsReporter writeMetrics;
  private final int shuffleId;
  private final int mapId;
  private final TaskContext taskContext;
  private final SparkConf sparkConf;
  private final boolean transferToEnabled;
  private final int initialSortBufferSize;
  private final int inputBufferSizeInBytes;
  private final StructType schema;
  private final ShuffleClient shuffleClient;
  private final int partitionNum;

  @Nullable private MapStatus mapStatus;
  @Nullable private CometCelebornShuffleExternalSorter sorter;

  @Nullable private long[] partitionLengths;

  private long peakMemoryUsedBytes = 0;
  private ExposedByteArrayOutputStream serBuffer;
  private SerializationStream serOutputStream;
  private Native nativeLib = new Native();
  private CometShuffleMemoryAllocatorTrait allocator;
  private boolean tracingEnabled;
  private int celebornShuffleId;
  private int numMapper;

  /**
   * Are we in the process of stopping? Because map tasks can call stop() with success = true and
   * then call stop() with success = false if they get an exception, we want to make sure we don't
   * try deleting files, etc twice.
   */
  private boolean stopping = false;

  CometCelebornUnsafeShuffleWriter(
      TaskMemoryManager memoryManager,
      BaseShuffleHandle<K, V, V> handle,
      int mapId,
      TaskContext taskContext,
      SparkConf conf,
      CelebornConf celebornConf,
      ShuffleWriteMetricsReporter writeMetrics,
      ShuffleClient shuffleClient,
      int celebornShuffleId) {
    this.memoryManager = memoryManager;
    this.mapId = mapId;
    final ShuffleDependency<K, V, V> dep = handle.dependency();
    this.shuffleId = dep.shuffleId();
    this.serializer = dep.serializer().newInstance();
    this.partitioner = dep.partitioner();
    this.schema = (StructType) ((CometShuffleDependency) dep).schema().get();
    this.writeMetrics = writeMetrics;
    this.taskContext = taskContext;
    this.sparkConf = conf;
    this.transferToEnabled = sparkConf.getBoolean("spark.file.transferTo", true);
    this.initialSortBufferSize =
        (int) (long) sparkConf.get(package$.MODULE$.SHUFFLE_SORT_INIT_BUFFER_SIZE());
    this.inputBufferSizeInBytes =
        (int) (long) sparkConf.get(package$.MODULE$.SHUFFLE_FILE_BUFFER_SIZE()) * 1024;
    this.tracingEnabled = (boolean) CometConf.COMET_TRACING_ENABLED().get();
    this.celebornShuffleId = celebornShuffleId;
    this.partitionNum = this.partitioner.numPartitions();
    this.numMapper = dep.rdd().getNumPartitions();
    this.shuffleClient = shuffleClient;
  }

  private void open() {
    assert (allocator == null);
    assert (sorter == null);
    allocator =
        CometShuffleMemoryAllocator.getInstance(
            sparkConf,
            memoryManager,
            Math.min(
                CometShuffleExternalSorter.MAXIMUM_PAGE_SIZE_BYTES, memoryManager.pageSizeBytes()));
    sorter =
        new CometCelebornShuffleExternalSorter(
            partitioner.numPartitions(),
            taskContext,
            schema,
            allocator,
            writeMetrics,
            initialSortBufferSize,
            shuffleClient,
            mapId,
            shuffleId,
            numMapper,
            (int) taskContext.taskAttemptId());
    serBuffer = new ExposedByteArrayOutputStream(DEFAULT_INITIAL_SER_BUFFER_SIZE);
    serOutputStream = serializer.serializeStream(serBuffer);
  }

  @Override
  public long[] getPartitionLengths() {
    return new long[0];
  }

  @Override
  public Option<MapStatus> stop(boolean success) {
    try {
      taskContext.taskMetrics().incPeakExecutionMemory(getPeakMemoryUsedBytes());

      if (stopping) {
        return Option.apply(null);
      } else {
        if (success) {
          if (mapStatus == null) {
            throw new IllegalStateException("Cannot call stop(true) without having called write()");
          }
          return Option.apply(mapStatus);
        } else {
          return Option.apply(null);
        }
      }
    } finally {
      if (sorter != null) {
        sorter.cleanupResources();
        shuffleClient.cleanup(celebornShuffleId, mapId, taskContext.attemptNumber());
      }
    }
  }

  @Override
  public void write(Iterator<Product2<K, V>> records) throws IOException {
    boolean success = false;

    if (tracingEnabled) {
      nativeLib.traceBegin(this.getClass().getName());
    }

    String offHeapMemKey = "comet_shuffle_" + Thread.currentThread().getId();

    try {
      while (records.hasNext()) {
        insertRecordIntoSorter(records.next());
      }
      if (tracingEnabled) {
        nativeLib.logMemoryUsage(offHeapMemKey, this.allocator.getUsed());
      }
      closeAndWriteOutput();
      success = true;
    } finally {
      if (tracingEnabled) {
        nativeLib.traceEnd(this.getClass().getName());
      }

      if (sorter != null) {
        try {
          sorter.cleanupResources();
        } catch (Exception e) {
          if (success) {
            throw e;
          } else {
            logger.error(
                "In addition to a failure during writing, we failed during " + "cleanup.", e);
          }
        }
      }

      if (tracingEnabled) {
        nativeLib.logMemoryUsage(offHeapMemKey, this.allocator.getUsed());
      }
    }
  }

  private void closeAndWriteOutput() throws IOException {
    assert (sorter != null);

    updatePeakMemoryUsage();
    serBuffer = null;
    serOutputStream = null;
    Long celebornWriteStart = System.nanoTime();
    shuffleClient.prepareForMergeData(celebornShuffleId, mapId, taskContext.attemptNumber());
    shuffleClient.pushMergedData(celebornShuffleId, mapId, taskContext.attemptNumber());
    shuffleClient.mapperEnd(celebornShuffleId, mapId, taskContext.attemptNumber(), partitionNum);
    BlockManagerId bmid = SparkEnv.get().blockManager().blockManagerId();
    mapStatus = SparkUtils.createMapStatus(bmid, partitionLengths, mapId);
    writeMetrics.incWriteTime(System.nanoTime() - celebornWriteStart);
  }

  private void updatePeakMemoryUsage() {
    if (sorter != null) {
      long mem = sorter.getPeakMemoryUsedBytes();
      if (mem > peakMemoryUsedBytes) {
        peakMemoryUsedBytes = mem;
      }
    }
  }

  private void insertRecordIntoSorter(Product2<K, V> record) throws IOException {
    assert (sorter != null);

    final K key = record._1();

    final int partitionId = partitioner.getPartition(key);
    serBuffer.reset();

    serOutputStream.writeKey(key, OBJECT_CLASS_TAG);
    serOutputStream.writeValue((UnsafeRow) record._2(), OBJECT_CLASS_TAG);
    serOutputStream.flush();

    final int serializedRecordSize = serBuffer.size();

    assert (serializedRecordSize > 0);

    sorter.insertRecord(
        serBuffer.getBuf(), Platform.BYTE_ARRAY_OFFSET, serializedRecordSize, partitionId);
  }

  @VisibleForTesting
  void forceSorterToSpill() throws IOException {
    assert (sorter != null);
    sorter.spill();
  }

  /** Return the peak memory used so far, in bytes. */
  public long getPeakMemoryUsedBytes() {
    updatePeakMemoryUsed();
    return peakMemoryUsedBytes;
  }

  private void updatePeakMemoryUsed() {
    // sorter can be null if this writer is closed
    if (sorter != null) {
      long mem = sorter.getPeakMemoryUsedBytes();
      if (mem > peakMemoryUsedBytes) {
        peakMemoryUsedBytes = mem;
      }
    }
  }
}
