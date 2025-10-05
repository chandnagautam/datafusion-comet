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

package org.apache.spark.sql.comet.execution.shuffle

import java.io.InputStream
import java.io.OutputStream
import java.nio.ByteBuffer

import scala.reflect.ClassTag

import org.apache.celeborn.common.CelebornConf
import org.apache.spark.ShuffleDependency
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.DeserializationStream
import org.apache.spark.serializer.SerializationStream
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.shuffle.BaseShuffleHandle
import org.apache.spark.shuffle.ShuffleReader
import org.apache.spark.shuffle.ShuffleReadMetricsReporter
import org.apache.spark.shuffle.celeborn.CelebornShuffleHandle
import org.apache.spark.shuffle.celeborn.CelebornShuffleReader
import org.apache.spark.shuffle.celeborn.ExecutorShuffleIdTracker
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

class CometCelebornShuffleReader[K, C](
    handle: CelebornShuffleHandle[K, _, C],
    startPartition: Int,
    endPartition: Int,
    startMapIndex: Int,
    endMapIndex: Int,
    context: TaskContext,
    celebornConf: CelebornConf,
    metrics: ShuffleReadMetricsReporter,
    shuffleTrackerId: ExecutorShuffleIdTracker)
    extends CelebornShuffleReader[K, C](
      handle,
      startPartition,
      endPartition,
      startMapIndex,
      endMapIndex,
      context,
      celebornConf,
      metrics,
      shuffleTrackerId,
      false)
    with Logging {

  // Celeborn uses this as last step in the record processing Iterator before dealing with
  // ordering, aggregate and mapside combine which wouldn't be true for the shuffle handle being
  // used for Comet, so ideally this should be correct and will be treated as the
  // CometShuffleBlockStoreReader
  override def newSerializerInstance(dep: ShuffleDependency[K, _, C]): SerializerInstance =
    new CometNativeDecoder(
      null,
      false,
      context,
      handle.dependency.asInstanceOf[CometShuffleDependency[_, _, _]].decodeTime,
      metrics)
}

private class CometNativeDecoder(
    schema: StructType,
    offHeapColumnVectorEnabled: Boolean,
    context: TaskContext,
    sqlMetric: SQLMetric,
    metrics: ShuffleReadMetricsReporter)
    extends SerializerInstance {

  override def serialize[T: ClassTag](t: T): ByteBuffer = throw new UnsupportedOperationException

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T =
    throw new UnsupportedOperationException

  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T =
    throw new UnsupportedOperationException

  override def serializeStream(s: OutputStream): SerializationStream =
    throw new UnsupportedOperationException

  override def deserializeStream(s: InputStream): DeserializationStream = {
    new DeserializationStream {
      val nativeDecodeIterator = NativeBatchDecoderIterator(s, context, sqlMetric)

      override def readObject[T: ClassTag](): T = throw new UnsupportedOperationException
      override def close(): Unit = nativeDecodeIterator.close

      override def asKeyValueIterator: Iterator[(Int, ColumnarBatch)] = nativeDecodeIterator.map {
        b =>
          // Celeborn Shuffle read will treat each read as 1 so subtracting 1 to get the correct
          // result
          metrics.incRecordsRead(b.numRows() - 1)
          (0, b)
      }
    }
  }
}
