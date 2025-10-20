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

import java.util.UUID
import java.util.concurrent.atomic.LongAdder

import scala.collection.JavaConverters.asJavaIterableConverter
import scala.collection.mutable

import org.apache.celeborn.CelebornShuffleClientWrapper
import org.apache.celeborn.client.ShuffleClient
import org.apache.spark.SparkEnv
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter
import org.apache.spark.shuffle.ShuffleWriter
import org.apache.spark.shuffle.celeborn.SparkCommonUtils
import org.apache.spark.shuffle.celeborn.SparkUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, RangePartitioning, SinglePartition}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.comet.CometExec
import org.apache.spark.sql.comet.CometMetricNode
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.CometConf
import org.apache.comet.JniStore
import org.apache.comet.serde.OperatorOuterClass
import org.apache.comet.serde.OperatorOuterClass.CompressionCodec
import org.apache.comet.serde.PartitioningOuterClass
import org.apache.comet.serde.QueryPlanSerde
import org.apache.comet.serde.QueryPlanSerde.serializeDataType

class CometCelebornNativeShuffleWriter[K, V](
    outputPartitioning: Partitioning,
    outputAttributes: Seq[Attribute],
    metrics: Map[String, SQLMetric],
    numParts: Int,
    shuffleId: Int,
    mapId: Int,
    context: TaskContext,
    metricsReporter: ShuffleWriteMetricsReporter,
    shuffleClient: ShuffleClient,
    handle: CometCelebornNativeShuffleHandle[K, V],
    rangePartitionBounds: Option[Seq[InternalRow]] = None)
    extends ShuffleWriter[K, V]
    with Logging {

  private val OFFSET_LENGTH = 8

  private val shuffleWrapper = new CelebornShuffleClientWrapper(shuffleClient)

  private val outputPartitions = outputPartitioning.numPartitions

  var mapStatusLengths: Array[LongAdder] = {
    val ret = new Array[LongAdder](outputPartitions)
    for (i <- 0 until outputPartitions) {
      ret(i) = new LongAdder()
    }

    ret
  }
  var mapStatus: MapStatus = _

  private var stopping = false

  private val attemptNumber = SparkCommonUtils.getEncodedAttemptNumber(context)

  override def write(records: Iterator[Product2[K, V]]): Unit = {
    val nativePlan = getNativePlan()

    val detailedMetrics = Seq(
      "elapsed_compute",
      "encode_time",
      "repart_time",
      "mempool_time",
      "input_batches",
      "spill_count",
      "spilled_bytes")

    // Maps native metrics to SQL metrics
    val metricsOutputRows = new SQLMetric("outputRows")
    val metricsWriteTime = new SQLMetric("writeTime")
    val nativeSQLMetrics = Map(
      "output_rows" -> metricsOutputRows,
      "data_size" -> metrics("dataSize"),
      "write_time" -> metricsWriteTime) ++
      metrics.filterKeys(detailedMetrics.contains)
    val nativeMetrics = CometMetricNode(nativeSQLMetrics)

    // Getting rid of the fake partitionId
    val newInputs = records.asInstanceOf[Iterator[_ <: Product2[Any, Any]]].map(_._2)

    val cometIter = CometExec.getCometIterator(
      Seq(newInputs.asInstanceOf[Iterator[ColumnarBatch]]),
      outputAttributes.length,
      nativePlan,
      nativeMetrics,
      numParts,
      context.partitionId(),
      broadcastedHadoopConfForEncryption = None,
      encryptedFilePaths = Seq.empty)

    while (cometIter.hasNext) {
      cometIter.next()
    }
    cometIter.close()

    metricsReporter.incBytesWritten(
      shuffleWrapper.getShuffleDataMetrics().values().stream().mapToInt(x => x.intValue()).sum())
    metricsReporter.incRecordsWritten(metricsOutputRows.value)
    metricsReporter.incWriteTime(metricsWriteTime.value)

    shuffleWrapper
      .getShuffleDataMetrics()
      .forEach((partition, bytesWritten) =>
        mapStatusLengths(partition).add(bytesWritten.longValue()))

    shuffleClient.prepareForMergeData(shuffleId, mapId, attemptNumber)
    shuffleClient.pushMergedData(shuffleId, mapId, attemptNumber)
    shuffleClient.mapperEnd(shuffleId, mapId, attemptNumber, numParts)

    val bmId = SparkEnv.get.blockManager.shuffleServerId
    mapStatus =
      SparkUtils.createMapStatus(bmId, SparkUtils.unwrap(mapStatusLengths), context.taskAttemptId)
  }

  override def stop(success: Boolean): Option[MapStatus] = {
    try {
      if (stopping) {
        Option.empty
      } else {
        stopping = true
        if (success) {
          Some(mapStatus)
        } else {
          Option.empty
        }
      }
    } finally {
      shuffleClient.cleanup(shuffleId, mapId, attemptNumber)
    }
  }

  private def getNativePlan() = {
    val scanBuilder = OperatorOuterClass.Scan.newBuilder().setSource("ShuffleWriterInput")
    val opBuilder = OperatorOuterClass.Operator.newBuilder()

    val scanTypes = outputAttributes.flatten { attr =>
      serializeDataType(attr.dataType)
    }

    if (scanTypes.length == outputAttributes.length) {
      scanBuilder.addAllFields(scanTypes.asJava)

      val shuffleWriterBuilder = OperatorOuterClass.CelebornShuffleWriter.newBuilder()
      val storeId = s"CelebornShuffleClient:${UUID.randomUUID()}"
      JniStore.add(storeId, shuffleWrapper)
      shuffleWriterBuilder.setObjectRetrievalId(storeId)
      shuffleWriterBuilder.setMapId(mapId)
      shuffleWriterBuilder.setAttemptId(attemptNumber)
      shuffleWriterBuilder.setNumPartitions(outputPartitions)
      shuffleWriterBuilder.setNumMappers(numParts)
      shuffleWriterBuilder.setShuffleId(shuffleId)

      if (SparkEnv.get.conf.getBoolean("spark.shuffle.compress", true)) {
        val codec = CometConf.COMET_EXEC_SHUFFLE_COMPRESSION_CODEC.get() match {
          case "zstd" => CompressionCodec.Zstd
          case "lz4" => CompressionCodec.Lz4
          case "snappy" => CompressionCodec.Snappy
          case other => throw new UnsupportedOperationException(s"invalid codec: $other")
        }
        shuffleWriterBuilder.setCodec(codec)
      } else {
        shuffleWriterBuilder.setCodec(CompressionCodec.None)
      }

      outputPartitioning match {
        case p if isSinglePartitioning(p) =>
          val partitioning = PartitioningOuterClass.SinglePartition.newBuilder()

          val partitioningBuilder = PartitioningOuterClass.Partitioning.newBuilder()
          shuffleWriterBuilder.setPartitioning(
            partitioningBuilder.setSinglePartition(partitioning).build())
        case _: HashPartitioning =>
          val hashPartitioning = outputPartitioning.asInstanceOf[HashPartitioning]

          val partitioning = PartitioningOuterClass.HashPartition.newBuilder()
          partitioning.setNumPartitions(outputPartitions)

          val partitionExprs = hashPartitioning.expressions
            .flatMap(e => QueryPlanSerde.exprToProto(e, outputAttributes))

          if (partitionExprs.length != hashPartitioning.expressions.length) {
            throw new UnsupportedOperationException(
              s"Partitioning $hashPartitioning is not supported.")
          }

          partitioning.addAllHashExpression(partitionExprs.asJava)

          val partitioningBuilder = PartitioningOuterClass.Partitioning.newBuilder()
          shuffleWriterBuilder.setPartitioning(
            partitioningBuilder.setHashPartition(partitioning).build())
        case _: RangePartitioning =>
          val rangePartitioning = outputPartitioning.asInstanceOf[RangePartitioning]

          val partitioning = PartitioningOuterClass.RangePartition.newBuilder()
          partitioning.setNumPartitions(outputPartitions)
          // Detect duplicates by tracking expressions directly, similar to DataFusion's LexOrdering
          // DataFusion will deduplicate identical sort expressions in LexOrdering,
          // so we need to transform boundary rows to match the deduplicated structure
          val seenExprs = mutable.HashSet[Expression]()
          val deduplicationMap = mutable.ArrayBuffer[(Int, Boolean)]() // (originalIndex, isKept)

          rangePartitioning.ordering.zipWithIndex.foreach { case (sortOrder, idx) =>
            if (seenExprs.contains(sortOrder.child)) {
              deduplicationMap += (idx -> false) // Will be deduplicated by DataFusion
            } else {
              seenExprs += sortOrder.child
              deduplicationMap += (idx -> true) // Will be kept by DataFusion
            }
          }

          {
            // Serialize the ordering expressions for comparisons
            val orderingExprs = rangePartitioning.ordering
              .flatMap(e => QueryPlanSerde.exprToProto(e, outputAttributes))
            if (orderingExprs.length != rangePartitioning.ordering.length) {
              throw new UnsupportedOperationException(
                s"Partitioning $rangePartitioning is not supported.")
            }
            partitioning.addAllSortOrders(orderingExprs.asJava)
          }

          // Convert Spark's sequence of InternalRows that represent partitioning boundaries to
          // sequences of Literals, where each outer entry represents a boundary row, and each
          // internal entry is a value in that row. In other words, these are stored in row major
          // order, not column major
          val boundarySchema = rangePartitioning.ordering.flatMap(e => Some(e.dataType))

          // Transform boundary rows to match DataFusion's deduplicated structure
          val transformedBoundaryExprs: Seq[Seq[Literal]] =
            rangePartitionBounds.get.map((row: InternalRow) => {
              // For every InternalRow, map its values to Literals
              val allLiterals =
                row.toSeq(boundarySchema).zip(boundarySchema).map { case (value, valueType) =>
                  Literal(value, valueType)
                }

              // Keep only the literals that correspond to non-deduplicated expressions
              allLiterals
                .zip(deduplicationMap)
                .filter(_._2._2) // Keep only where isKept = true
                .map(_._1) // Extract the literal
            })

          {
            // Convert the sequences of Literals to a collection of serialized BoundaryRows
            val boundaryRows: Seq[PartitioningOuterClass.BoundaryRow] = transformedBoundaryExprs
              .map((rowLiterals: Seq[Literal]) => {
                // Serialize each sequence of Literals as a BoundaryRow
                val rowBuilder = PartitioningOuterClass.BoundaryRow.newBuilder();
                val serializedExprs =
                  rowLiterals.map(lit_value =>
                    QueryPlanSerde.exprToProto(lit_value, outputAttributes).get)
                rowBuilder.addAllPartitionBounds(serializedExprs.asJava)
                rowBuilder.build()
              })
            partitioning.addAllBoundaryRows(boundaryRows.asJava)
          }

          val partitioningBuilder = PartitioningOuterClass.Partitioning.newBuilder()
          shuffleWriterBuilder.setPartitioning(
            partitioningBuilder.setRangePartition(partitioning).build())
        case _ =>
          throw new UnsupportedOperationException(
            s"Partitioning $outputPartitioning is not supported.")
      }

      val shuffleWriterOpBuilder = OperatorOuterClass.Operator.newBuilder()
      shuffleWriterOpBuilder
        .setCelebornShuffleWriter(shuffleWriterBuilder)
        .addChildren(opBuilder.setScan(scanBuilder).build())
        .build()
    } else {
      // There are unsupported scan type
      throw new UnsupportedOperationException(
        s"$outputAttributes contains unsupported data types for CometShuffleExchangeExec.")
    }
  }

  override def getPartitionLengths(): Array[Long] =
    throw new UnsupportedOperationException(
      "Celeborn is not compatible with Spark push mode," +
        "please set spark.shuffle.push.enabled to false")

  private def isSinglePartitioning(p: Partitioning): Boolean = p match {
    case SinglePartition => true
    case rp: RangePartitioning =>
      // Spark sometimes generates RangePartitioning schemes with numPartitions == 1,
      // or the computed bounds results in a single target partition.
      // In this case Comet just serializes a SinglePartition scheme to native.
      rp.numPartitions == 1 || rangePartitionBounds.forall(_.isEmpty)
    case hp: HashPartitioning => hp.numPartitions == 1
    case _ => false
  }
}
