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
import java.util.concurrent.ExecutorService;

import scala.None$;
import scala.Option;
import scala.Product2;
import scala.collection.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.common.CelebornConf;
import org.apache.spark.Partitioner;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.shuffle.celeborn.SparkUtils;
import org.apache.spark.shuffle.comet.CometShuffleMemoryAllocator;
import org.apache.spark.shuffle.comet.CometShuffleMemoryAllocatorTrait;
import org.apache.spark.shuffle.sort.CometShuffleExternalSorter;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.BlockManagerId;

import org.apache.comet.CometConf$;
import org.apache.comet.Native;

final class CometCelebornBypassMergeSortWriter<K, V> extends ShuffleWriter<K, V> {
  private static Logger logger = LoggerFactory.getLogger(CometCelebornBypassMergeSortWriter.class);
  private ShuffleClient shuffleClient;
  private int celebornShuffleId;
  private int mapId;
  private TaskMemoryManager memoryManager;
  private TaskContext taskContext;
  private CometCelebornBypassMergeSortHandle<K, V> handle;
  private CelebornConf celebornConf;
  private ShuffleWriteMetricsReporter writeMetrics;
  private SparkConf conf;
  private SerializerInstance serializer;
  private Partitioner partitioner;
  private CometShuffleMemoryAllocatorTrait allocator;

  private long[] partitionLengths;

  private boolean stopping = false;

  private MapStatus mapStatus;

  private CometCelebornWriter[] partitionWriters;

  private boolean isAsync;
  private int asyncThreadNum;

  private final ExecutorService threadPool;
  private int partitionNum;

  private boolean tracingEnabled;
  private StructType schema;

  CometCelebornBypassMergeSortWriter(
      ShuffleClient shuffleClient,
      int celebornShuffleId,
      int mapId,
      TaskMemoryManager memoryManager,
      TaskContext taskContext,
      CometCelebornBypassMergeSortHandle<K, V> handle,
      SparkConf conf,
      CelebornConf celebornConf,
      ShuffleWriteMetricsReporter writerMetrics) {
    this.shuffleClient = shuffleClient;
    this.celebornShuffleId = celebornShuffleId;
    this.mapId = mapId;
    this.memoryManager = memoryManager;
    this.taskContext = taskContext;
    this.handle = handle;
    this.writeMetrics = writerMetrics;
    this.celebornConf = celebornConf;
    this.conf = conf;
    ShuffleDependency<K, V, V> dep = handle.dependency();
    this.serializer = dep.serializer().newInstance();
    this.partitioner = dep.partitioner();
    this.partitionNum = partitioner.numPartitions();
    this.schema = ((CometShuffleDependency<?, ?, ?>) dep).schema().get();
    this.isAsync = (boolean) CometConf$.MODULE$.COMET_COLUMNAR_SHUFFLE_ASYNC_ENABLED().get();
    this.asyncThreadNum = (int) CometConf$.MODULE$.COMET_COLUMNAR_SHUFFLE_ASYNC_THREAD_NUM().get();
    this.tracingEnabled = (boolean) CometConf$.MODULE$.COMET_TRACING_ENABLED().get();

    if (isAsync) {
      logger.info("Async shuffle writer enabled for celeborn");
      this.threadPool = ShuffleThreadPool.getThreadPool();
    } else {
      logger.info("Async shuffle writer *not* enabled for celeborn");
      this.threadPool = null;
    }
  }

  @Override
  public long[] getPartitionLengths() {
    return partitionLengths;
  }

  @Override
  public Option<MapStatus> stop(boolean success) {
    try {
      if (stopping) {
        return None$.empty();
      } else {
        stopping = true;

        if (success) {
          if (mapStatus == null) {
            throw new IllegalStateException("Cannot call stop(true) without having called write()");
          }

          return Option.apply(mapStatus);
        } else {
          if (partitionWriters != null) {
            try {
              for (CometCelebornWriter writer : partitionWriters) {
                writer.freeMemory();
              }
            } finally {
              partitionWriters = null;
            }
          }
        }
      }
      return None$.empty();
    } finally {
      shuffleClient.cleanup(celebornShuffleId, mapId, taskContext.attemptNumber());
    }
  }

  @Override
  public void write(Iterator<Product2<K, V>> records) throws IOException {
    assert (partitionWriters == null);

    if (!records.hasNext()) {
      BlockManagerId bmId = SparkEnv.get().blockManager().shuffleServerId();
      mapStatus = SparkUtils.createMapStatus(bmId, partitionLengths, mapId);

      return;
    }
    final long openStartTime = System.nanoTime();
    partitionWriters = new CometCelebornWriter[partitionNum];

    allocator =
        CometShuffleMemoryAllocator.getInstance(
            conf,
            memoryManager,
            Math.min(
                CometShuffleExternalSorter.MAXIMUM_PAGE_SIZE_BYTES, memoryManager.pageSizeBytes()));
    for (int i = 0; i < partitionNum; i++) {
      CometCelebornWriter writer =
          new CometCelebornWriter(
              shuffleClient,
              allocator,
              taskContext,
              serializer,
              schema,
              writeMetrics,
              isAsync,
              asyncThreadNum,
              threadPool,
              tracingEnabled,
              celebornShuffleId,
              mapId,
              (int) taskContext.taskAttemptId(),
              i,
              handle.dependency().rdd().getNumPartitions(),
              partitionNum);
      partitionWriters[i] = writer;
    }

    writeMetrics.incWriteTime(System.nanoTime() - openStartTime);

    long outputRows = 0;

    while (records.hasNext()) {
      outputRows += 1;

      final Product2<K, V> record = records.next();

      final K key = record._1();

      int partitionId = partitioner.getPartition(key);

      partitionWriters[partitionId].insertRow((UnsafeRow) record._2(), partitionId);
    }

    Native _native = new Native();
    if (tracingEnabled) {
      _native.logMemoryUsage("comet_shuffle_", allocator.getUsed());
    }

    long spillRecords = 0;

    for (int i = 0; i < partitionNum; i++) {
      CometCelebornWriter writer = partitionWriters[i];

      writer.close();

      spillRecords += writer.getOutputRecords();
    }

    if (tracingEnabled) {
      _native.logMemoryUsage("comet_shuffle_", allocator.getUsed());
    }

    if (outputRows != spillRecords) {
      throw new RuntimeException(
          "outputRows("
              + outputRows
              + ") != spillRecords("
              + spillRecords
              + "). Please file a bug report.");
    }
    Long celebornWriteStart = System.nanoTime();
    shuffleClient.prepareForMergeData(celebornShuffleId, mapId, taskContext.attemptNumber());
    shuffleClient.pushMergedData(celebornShuffleId, mapId, taskContext.attemptNumber());
    shuffleClient.mapperEnd(celebornShuffleId, mapId, taskContext.attemptNumber(), partitionNum);
    BlockManagerId bmid = SparkEnv.get().blockManager().blockManagerId();
    mapStatus = SparkUtils.createMapStatus(bmid, partitionLengths, mapId);
    writeMetrics.incWriteTime(System.nanoTime() - celebornWriteStart);
  }
}
