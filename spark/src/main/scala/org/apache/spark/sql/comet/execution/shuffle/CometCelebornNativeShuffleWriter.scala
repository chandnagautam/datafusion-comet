package org.apache.spark.sql.comet.execution.shuffle

import scala.collection.JavaConverters.asJavaIterableConverter

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.TaskContext
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter
import org.apache.spark.shuffle.ShuffleWriter
import org.apache.spark.scheduler.MapStatus
import org.apache.celeborn.client.ShuffleClient
import org.apache.comet.serde.OperatorOuterClass
import org.apache.comet.serde.OperatorOuterClass.CompressionCodec
import org.apache.comet.serde.PartitioningOuterClass
import org.apache.comet.serde.QueryPlanSerde.serializeDataType
import org.apache.spark.SparkEnv
import org.apache.comet.JniStore
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, RangePartitioning, SinglePartition}
import org.apache.spark.sql.internal.SQLConf
import java.util.UUID
import org.apache.comet.CometConf
import org.apache.comet.serde.QueryPlanSerde
import org.apache.spark.shuffle.celeborn.SparkUtils
import org.apache.spark.sql.comet.CometMetricNode

class CometCelebornNativeShuffleWriter[K, V](
  outputPartitioning: Partitioning,
  outputAttributes: Seq[Attribute],
  metrics: Map[String, SQLMetric],
  numParts: Int,
  shuffleId: Int,
  mapId: Long,
  context: TaskContext,
  metricsReporter: ShuffleWriteMetricsReporter,
  shuffleClient: ShuffleClient) extends ShuffleWriter[K, V] with Logging {

  private val OFFSET_LENGTH  = 8

  var partitionLengths: Array[Long] = _
  var mapStatus: MapStatus = _

  override def write(records: Iterator[Product2[K,V]]): Unit = {
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
    val newInputs = inputs.asInstanceOf[Iterator[_ <: Product2[Any, Any]]].map(_._2)

    val cometIter = CometExec.getCometIterator(
      Seq(newInputs.asInstanceOf[Iterator[ColumnarBatch]]),
      outputAttributes.length,
      nativePlan,
      nativeMetrics,
      numParts,
      context.partitionId())

    while (cometIter.hasNext) {
      cometIter.next()
    }
    cometIter.close()

    // TODO: Figure out a way to find the total written bytes
    // metricsReporter.incBytesWritten(Files.size(tempDataFilePath))
    metricsReporter.incRecordsWritten(metricsOutputRows.value)
    metricsReporter.incWriteTime(metricsWriteTime.value)

    shuffleClient.prepareForMergeData(shuffleId, mapId.intValue(), context.attemptNumber)
    shuffleClient.pushMergedData(shuffleId, mapId.intValue(), context.attemptNumber)
    shuffleClient.mapperEnd(shuffleId, mapId.intValue(), context.attemptNumber(), numParts)

    val bmId = SparkEnv.get.blockManager.blockManagerId
    mapStatus = SparkUtils.createMapStatus(bmId, partitionLengths, mapId)
  }

  override def stop(success: Boolean): Option[MapStatus] = ???

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
      JniStore.add(storeId, shuffleClient)
      shuffleWriterBuilder.setObjectRetrievalId(storeId)

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
        case _: HashPartitioning =>
          val hashPartitioning = outputPartitioning.asInstanceOf[HashPartitioning]

          val partitioning = PartitioningOuterClass.HashPartition.newBuilder()
          partitioning.setNumPartitions(outputPartitioning.numPartitions)

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
          partitioning.setNumPartitions(outputPartitioning.numPartitions)
          val sampleSize = {
            // taken from org.apache.spark.RangePartitioner#rangeBounds
            // This is the sample size we need to have roughly balanced output partitions,
            // capped at 1M.
            // Cast to double to avoid overflowing ints or longs
            val sampleSize = math.min(
              SQLConf.get
                .getConf(SQLConf.RANGE_EXCHANGE_SAMPLE_SIZE_PER_PARTITION)
                .toDouble * outputPartitioning.numPartitions,
              1e6)
            // Assume the input partitions are roughly balanced and over-sample a little bit.
            // Comet: we don't divide by numPartitions since each DF plan handles one partition.
            math.ceil(3.0 * sampleSize).toInt
          }
          if (sampleSize > 8192) {
            logWarning(
              s"RangePartitioning sampleSize of s$sampleSize exceeds Comet RecordBatch size.")
          }
          partitioning.setSampleSize(sampleSize)

          val orderingExprs = rangePartitioning.ordering
            .flatMap(e => QueryPlanSerde.exprToProto(e, outputAttributes))

          if (orderingExprs.length != rangePartitioning.ordering.length) {
            throw new UnsupportedOperationException(
              s"Partitioning $rangePartitioning is not supported.")
          }

          partitioning.addAllSortOrders(orderingExprs.asJava)

          val partitioningBuilder = PartitioningOuterClass.Partitioning.newBuilder()
          shuffleWriterBuilder.setPartitioning(
            partitioningBuilder.setRangePartition(partitioning).build())
        case SinglePartition =>
          val partitioning = PartitioningOuterClass.SinglePartition.newBuilder()

          val partitioningBuilder = PartitioningOuterClass.Partitioning.newBuilder()
          shuffleWriterBuilder.setPartitioning(
            partitioningBuilder.setSinglePartition(partitioning).build())

        case _ =>
          throw new UnsupportedOperationException(
            s"Partitioning $outputPartitioning is not supported.")
      }

      val shuffleWriterOpBuilder = OperatorOuterClass.Operator.newBuilder()
      shuffleWriterOpBuilder.setCelebornShuffleWriter(shuffleWriterBuilder)
      .addChildren(opBuilder.setScan(scanBuilder).build())
      .build()
    } else {
      // There are unsupported scan type
      throw new UnsupportedOperationException(
        s"$outputAttributes contains unsupported data types for CometShuffleExchangeExec.")
    }
  }

  override def getPartitionLengths(): Array[Long] = partitionLengths
}
