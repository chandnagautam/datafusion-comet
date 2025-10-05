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

import java.util.Collection
import java.util.Collections

import org.apache.celeborn.client.LifecycleManager
import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.protocol.ShuffleMode
import org.apache.spark.MapOutputTrackerMaster
import org.apache.spark.ShuffleDependency
import org.apache.spark.SparkConf
import org.apache.spark.SparkEnv
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config
import org.apache.spark.rdd.DeterministicLevel
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.{ShuffleHandle, ShuffleReader, ShuffleReadMetricsReporter}
import org.apache.spark.shuffle.{ShuffleHandle, ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.shuffle.BaseShuffleHandle
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.shuffle.ShuffleBlockResolver
import org.apache.spark.shuffle.ShuffleHandle
import org.apache.spark.shuffle.ShuffleManager
import org.apache.spark.shuffle.celeborn.CelebornShuffleHandle
import org.apache.spark.shuffle.celeborn.ExecutorShuffleIdTracker
import org.apache.spark.shuffle.celeborn.ShuffleFetchFailureReportTaskCleanListener
import org.apache.spark.shuffle.celeborn.SparkShuffleManager
import org.apache.spark.shuffle.celeborn.SparkUtils
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec.prepareShuffleDependency
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf
import org.apache.comet.CometConf.COMET_CONVERT_FROM_CSV_ENABLED
import org.apache.comet.serde.ExprOuterClass.BloomFilterMightContain

class CometCelebornShuffleManager(conf: SparkConf, isDriver: Boolean)
    extends ShuffleManager
    with Logging {
  private val vanillaCelebornShuffleManager = new SparkShuffleManager(conf, isDriver)

  private var lifecycleManager: LifecycleManager = _

  private val celebornConf: CelebornConf = SparkUtils.fromSparkConf(conf)

  private var appUniqueId: String = _

  private val stageRerunEnabled = celebornConf.clientStageRerunEnabled

  private var shuffleClient: ShuffleClient = _

  private val shuffleIdTracker = new ExecutorShuffleIdTracker

  private def isRddDeterminate(dependency: ShuffleDependency[_, _, _]): Boolean = {
    val rdd: RDD[_] = dependency.rdd
    val rddClass = rdd.getClass
    val getDeterminateMethod = rddClass.getMethod("getOutputDeterministicLevel")
    getDeterminateMethod.setAccessible(true)

    getDeterminateMethod.invoke(rdd) != DeterministicLevel.DETERMINATE
  }

  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    val appId = SparkUtils.appUniqueId(dependency.rdd.context)
    initializeLifecycleManager(appId)

    dependency match {
      case cometShuffleDependency: CometShuffleDependency[_, _, _] =>
        cometShuffleDependency.shuffleType match {
          case CometColumnarShuffle =>
            lifecycleManager.registerAppShuffleDeterminate(
              shuffleId,
              isRddDeterminate(dependency))
            if (shouldBypassMergeSort(conf, dependency) || !SortShuffleManager
                .canUseSerializedShuffle(dependency)) {
              new CometCelebornBypassMergeSortHandle(
                shuffleId,
                appId,
                lifecycleManager.getHost,
                lifecycleManager.getPort,
                lifecycleManager.getUserIdentifier,
                celebornConf.clientStageRerunEnabled,
                dependency.rdd.getNumPartitions,
                dependency.asInstanceOf[ShuffleDependency[K, V, V]])
            } else {
              new CometCelebornSerializedShuffleHandle(
                shuffleId,
                appId,
                lifecycleManager.getHost,
                lifecycleManager.getPort,
                lifecycleManager.getUserIdentifier,
                celebornConf.clientStageRerunEnabled,
                dependency.rdd.getNumPartitions,
                dependency.asInstanceOf[ShuffleDependency[K, V, V]])
            }
          case CometNativeShuffle =>
            lifecycleManager.registerAppShuffleDeterminate(
              shuffleId,
              isRddDeterminate(dependency))
            new CometCelebornNativeShuffleHandle(
              shuffleId,
              appId,
              lifecycleManager.getHost,
              lifecycleManager.getPort,
              lifecycleManager.getUserIdentifier,
              celebornConf.clientStageRerunEnabled,
              dependency.rdd.getNumPartitions,
              dependency.asInstanceOf[ShuffleDependency[K, V, V]])
          case _ =>
            throw new UnsupportedOperationException(
              s"Unsupported shuffle type: ${cometShuffleDependency.shuffleType}")
        }
      case _ =>
        // TODO: Handle vanillaCelebornShuffleManager shuffle registration
        new CelebornShuffleHandle(
          appUniqueId,
          lifecycleManager.getHost,
          lifecycleManager.getPort,
          lifecycleManager.getUserIdentifier,
          shuffleId,
          stageRerunEnabled,
          dependency.rdd.getNumPartitions,
          dependency)
    }
  }

  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    val env = SparkEnv.get
    handle match {
      case native: CometCelebornNativeShuffleHandle[K @unchecked, V @unchecked] =>
        val dep = native.dependency.asInstanceOf[CometShuffleDependency[_, _, _]]
        val celebornShuffleId = initShuffleClientForWriter(native, context)
        logInfo(s"mappers: ${dep.numParts}, mapId: ${context.partitionId}")
        logInfo(s"partitions: ${dep.outputPartitioning.get.numPartitions}")
        new CometCelebornNativeShuffleWriter(
          dep.outputPartitioning.get,
          dep.outputAttributes,
          dep.shuffleWriteMetrics,
          dep.numParts,
          celebornShuffleId,
          context.partitionId,
          context,
          metrics,
          shuffleClient,
          native)
      case bypassMergeSortHandle: CometCelebornBypassMergeSortHandle[
            K @unchecked,
            V @unchecked] =>
        val celebornShuffleId = initShuffleClientForWriter(bypassMergeSortHandle, context)
        new CometCelebornBypassMergeSortWriter(
          shuffleClient,
          celebornShuffleId,
          context.partitionId,
          context.taskMemoryManager,
          context,
          bypassMergeSortHandle,
          conf,
          celebornConf,
          metrics)
      case unsafeShuffleHandle: CometCelebornSerializedShuffleHandle[
            K @unchecked,
            V @unchecked] =>
        val celebornShuffleId = initShuffleClientForWriter(unsafeShuffleHandle, context)
        new CometCelebornUnsafeShuffleWriter(
          context.taskMemoryManager,
          unsafeShuffleHandle,
          context.partitionId,
          context,
          conf,
          celebornConf,
          metrics,
          shuffleClient,
          celebornShuffleId)
      case _ =>
        vanillaCelebornShuffleManager.getWriter(handle, mapId, context, metrics)
    }
  }

  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] =
    if (handle.isInstanceOf[CometCelebornBypassMergeSortHandle[_, _]] || handle
        .isInstanceOf[CometCelebornSerializedShuffleHandle[_, _]] || handle
        .isInstanceOf[CometNativeShuffleHandle[_, _]]) {
      new CometCelebornShuffleReader(
        handle.asInstanceOf[CelebornShuffleHandle[K, _, C]],
        startPartition,
        endPartition,
        startMapIndex,
        endMapIndex,
        context,
        celebornConf,
        metrics,
        shuffleIdTracker)
    } else {
      vanillaCelebornShuffleManager.getReader(
        handle,
        startMapIndex,
        endMapIndex,
        startPartition,
        endPartition,
        context,
        metrics)
    }

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    if (lifecycleManager != null) {
      lifecycleManager.unregisterAppShuffle(shuffleId, stageRerunEnabled)
    }

    if (shuffleClient != null) {
      shuffleIdTracker.unregisterAppShuffleId(shuffleClient, shuffleId)
    }

    true
  }

  override def shuffleBlockResolver: ShuffleBlockResolver = {
    classOf[IndexShuffleBlockResolver].getDeclaredConstructors
      .filter(c => List(2, 3).contains(c.getParameterCount()))
      .map { c =>
        c.getParameterCount match {
          case 2 =>
            c.newInstance(conf, null).asInstanceOf[IndexShuffleBlockResolver]
          case 3 =>
            c.newInstance(conf, null, Collections.emptyMap())
              .asInstanceOf[IndexShuffleBlockResolver]
        }
      }
      .head
  }

  override def stop(): Unit = {
    if (shuffleClient != null) {
      shuffleClient.shutdown()
      ShuffleClient.reset()
      shuffleClient = null
    }

    if (lifecycleManager != null) {
      lifecycleManager.stop()
      lifecycleManager = null
    }
  }

  private def initializeLifecycleManager(appId: String): Unit = {
    if (isDriver && lifecycleManager == null) {
      CometCelebornShuffleManager.this.synchronized {
        if (lifecycleManager == null) {
          appUniqueId = celebornConf.appUniqueIdWithUUIDSuffix(appId)

          lifecycleManager = new LifecycleManager(appUniqueId, celebornConf)

          lifecycleManager.applicationCount.increment()

          lifecycleManager.registerCancelShuffleCallback((shuffleId, reason) => {
            try {
              SparkUtils.cancelShuffle(shuffleId, reason)
            } catch {
              case e: Exception =>
                throw new RuntimeException(e)
            }
          })

          if (stageRerunEnabled) {
            val mapOutputTracker =
              SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
            lifecycleManager.registerReportTaskShuffleFetchFailurePreCheck(taskId => {
              SparkUtils.shouldReportShuffleFetchFailure(taskId)
            })

            SparkUtils.addSparkListener(new ShuffleFetchFailureReportTaskCleanListener)

            lifecycleManager.registerShuffleTrackerCallback(shufffleId =>
              SparkUtils.unregisterAllMapOutput(mapOutputTracker, shufffleId))
          }
        }
      }
    }
  }

  def shouldBypassMergeSort(conf: SparkConf, dep: ShuffleDependency[_, _, _]): Boolean = {
    if (dep.mapSideCombine) {
      false
    } else {
      if (celebornConf.dynamicWriteModeEnabled) {
        val partitionCond =
          dep.partitioner.numPartitions <= celebornConf.dynamicWriteModePartitionNumThreshold

        val executorCores = conf.get(config.EXECUTOR_CORES)
        val maxThreads = CometConf.COMET_COLUMNAR_SHUFFLE_ASYNC_MAX_THREAD_NUM.get(SQLConf.get)
        val threadCond = dep.partitioner.numPartitions * executorCores <= maxThreads

        partitionCond && threadCond
      } else {
        celebornConf.shuffleWriterMode != ShuffleMode.SORT
      }
    }
  }

  private def initShuffleClientForWriter(
      handle: CelebornShuffleHandle[_, _, _],
      context: TaskContext): Int = {
    shuffleClient = ShuffleClient.get(
      handle.appUniqueId,
      handle.lifecycleManagerHost,
      handle.lifecycleManagerPort,
      celebornConf,
      handle.userIdentifier,
      handle.extension)
    if (handle.stageRerunEnabled) {
      SparkUtils.addFailureListenerIfBarrierTask(shuffleClient, context, handle)
    }

    SparkUtils.celebornShuffleId(shuffleClient, handle, context, true)
  }
}

private[spark] class CometCelebornBypassMergeSortHandle[K, V](
    shuffleId: Int,
    appId: String,
    host: String,
    port: Int,
    userIdentifier: UserIdentifier,
    stageRerunEnabled: Boolean,
    numMappers: Int,
    dependency: ShuffleDependency[K, V, V])
    extends CelebornShuffleHandle(
      appId,
      host,
      port,
      userIdentifier,
      shuffleId,
      stageRerunEnabled,
      numMappers,
      dependency) {}

private[spark] class CometCelebornSerializedShuffleHandle[K, V](
    shuffleId: Int,
    appId: String,
    host: String,
    port: Int,
    userIdentifier: UserIdentifier,
    stageRerunEnabled: Boolean,
    numMappers: Int,
    dependency: ShuffleDependency[K, V, V])
    extends CelebornShuffleHandle(
      appId,
      host,
      port,
      userIdentifier,
      shuffleId,
      stageRerunEnabled,
      numMappers,
      dependency) {}

private[spark] class CometCelebornNativeShuffleHandle[K, V](
    shuffleId: Int,
    appId: String,
    host: String,
    port: Int,
    userIdentifier: UserIdentifier,
    stageRerunEnabled: Boolean,
    numMappers: Int,
    dependency: ShuffleDependency[K, V, V])
    extends CelebornShuffleHandle(
      appId,
      host,
      port,
      userIdentifier,
      shuffleId,
      stageRerunEnabled,
      numMappers,
      dependency) {}
