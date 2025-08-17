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

package org.apache.spark.sql.comet.sql.comet.execution.shuffle;

import java.util.Collection
import java.util.Collections

import org.apache.celeborn.client.LifecycleManager
import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.common.CelebornConf
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
import org.apache.spark.sql.comet.execution.shuffle.CometColumnarShuffle
import org.apache.spark.sql.comet.execution.shuffle.CometNativeShuffle
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleDependency
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
          case CometColumnarShuffle => {
            lifecycleManager.registerAppShuffleDeterminate(
              shuffleId,
              isRddDeterminate(dependency))
            if (shouldBypassMergeSort(conf, dependency) || !SortShuffleManager
                .canUseSerializedShuffle(dependency)) {
              new CometCelebornByMergeSortHandle(
                shuffleId,
                dependency.asInstanceOf[ShuffleDependency[K, V, V]])
            } else {
              new CometCelebornSerializedShuffleHandle(
                shuffleId,
                dependency.asInstanceOf[ShuffleDependency[K, V, V]])
            }
          }
          case CometNativeShuffle => {
            lifecycleManager.registerAppShuffleDeterminate(
              shuffleId,
              isRddDeterminate(dependency))
            new CometCelebornNativeShuffleHandle(
              shuffleId,
              dependency.asInstanceOf[ShuffleDependency[K, V, V]])
          }
          case _ =>
            throw new UnsupportedOperationException(
              s"Unsupported shuffle type: ${cometShuffleDependency.shuffleType}")
        }
      case _ => {
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
  }

  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = ???

  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = ???

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
}

private[spark] class CometCelebornByMergeSortHandle[K, V](
    shuffleId: Int,
    dependency: ShuffleDependency[K, V, V])
    extends BaseShuffleHandle(shuffleId, dependency) {}

private[spark] class CometCelebornSerializedShuffleHandle[K, V](
    shuffleId: Int,
    dependency: ShuffleDependency[K, V, V])
    extends BaseShuffleHandle(shuffleId, dependency) {}

private[spark] class CometCelebornNativeShuffleHandle[K, V](
    shuffleId: Int,
    dependency: ShuffleDependency[K, V, V])
    extends BaseShuffleHandle(shuffleId, dependency) {}
