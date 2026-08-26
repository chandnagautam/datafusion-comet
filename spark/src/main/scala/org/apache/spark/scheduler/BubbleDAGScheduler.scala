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

package org.apache.spark.scheduler

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashSet}

import org.apache.spark.{JobArtifactSet, MapOutputTrackerMaster, SparkContext, SparkEnv}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.storage.BlockManagerMaster
import org.apache.spark.util.{Clock, SystemClock}

/**
 * BubbleDAGScheduler extends Spark's [[DAGScheduler]] to support Bubble Scheduling with Remote
 * Shuffle Services (e.g. Apache Celeborn).
 *
 * Key behaviors:
 *   1. Allows downstream stages to launch ready partition tasks without waiting for the entire
 *      upstream stage to complete. 2. Evaluates downstream partition readiness progressively once
 *      a configurable fraction of upstream tasks finish. 3. Falls back to stage-barrier execution
 *      if the upstream task count is below the minimum threshold. 4. Cancels and reschedules
 *      active downstream tasks if an upstream mapper fails and retries.
 */
class BubbleDAGScheduler(
    sc: SparkContext,
    taskScheduler: TaskScheduler,
    listenerBus: LiveListenerBus,
    mapOutputTracker: MapOutputTrackerMaster,
    blockManagerMaster: BlockManagerMaster,
    env: SparkEnv,
    clock: Clock = new SystemClock())
    extends DAGScheduler(
      sc,
      taskScheduler,
      listenerBus,
      mapOutputTracker,
      blockManagerMaster,
      env,
      clock)
    with Logging {

  private val conf = sc.getConf

  private val closureSerializerInstance: SerializerInstance = env.closureSerializer.newInstance()

  // Stage ID -> Set of completed mapper partition IDs
  private val stageCompletedPartitions = new mutable.HashMap[Int, mutable.BitSet]()

  // Child Stage ID -> Set of submitted partition IDs
  private val stageSubmittedPartitions = new mutable.HashMap[Int, mutable.BitSet]()

  // Parent Stage ID -> Set of dependent child stage IDs
  private val stageToChildStages = new mutable.HashMap[Int, mutable.HashSet[Stage]]()

  // Track active submitted task IDs per child stage for fail-fast cancellation
  // Stage ID -> Map[PartitionId, Seq[Long]]
  private val stageRunningTaskIds =
    new mutable.HashMap[Int, mutable.HashMap[Int, ArrayBuffer[Long]]]()

  /**
   * Check if bubble scheduling is enabled and stage meets minimum task count criteria.
   */
  def isBubbleSchedulingEligible(stage: Stage): Boolean = {
    if (!BubbleSchedulingConf.isBubbleSchedulingEnabled(conf)) {
      return false
    }
    val minTasksThreshold = BubbleSchedulingConf.getMinUpstreamTasksThreshold(conf)
    if (stage.numPartitions < minTasksThreshold) {
      logInfo(
        s"Stage ${stage.id} has ${stage.numPartitions} partitions, below bubble " +
          s"threshold $minTasksThreshold. Falling back to barrier.")
      return false
    }
    true
  }

  /**
   * Check if downstream child stage can start dispatching ready partition tasks.
   */
  private def canProgressivelyDispatch(parentStage: ShuffleMapStage): Boolean = {
    if (!isBubbleSchedulingEligible(parentStage)) {
      return false
    }
    val completedCount =
      stageCompletedPartitions.get(parentStage.id).map(_.size).getOrElse(0)
    val fraction = completedCount.toDouble / parentStage.numPartitions
    val minFraction = BubbleSchedulingConf.getMinUpstreamCompletionFraction(conf)
    fraction >= minFraction
  }

  /**
   * Register a completed mapper partition and check if any child stages have ready partitions to
   * submit.
   */
  def onMapperTaskCompleted(stage: ShuffleMapStage, partitionId: Int): Unit = {
    val completedSet =
      stageCompletedPartitions.getOrElseUpdate(stage.id, new mutable.BitSet(stage.numPartitions))
    completedSet.add(partitionId)

    if (canProgressivelyDispatch(stage)) {
      checkAndSubmitReadyChildPartitions(stage)
    }
  }

  /**
   * Evaluates child stages in waitingStages and dispatches ready partition tasks.
   */
  private def checkAndSubmitReadyChildPartitions(parentStage: ShuffleMapStage): Unit = {
    val children = stageToChildStages.getOrElse(parentStage.id, Set.empty[Stage])
    for (childStage <- children
      if waitingStages.contains(childStage) || runningStages.contains(childStage)) {
      dispatchReadyPartitionsForChild(parentStage, childStage)
    }
  }

  /**
   * Determines which partitions of the child stage have their dependencies satisfied and submits
   * them.
   */
  private def dispatchReadyPartitionsForChild(
      parentStage: ShuffleMapStage,
      childStage: Stage): Unit = {
    val alreadySubmitted = stageSubmittedPartitions.getOrElseUpdate(
      childStage.id,
      new mutable.BitSet(childStage.numPartitions))
    val readyPartitions = new ArrayBuffer[Int]()

    for (p <- 0 until childStage.numPartitions if !alreadySubmitted.contains(p)) {
      if (isChildPartitionReady(parentStage, childStage, p)) {
        readyPartitions += p
        alreadySubmitted.add(p)
      }
    }

    if (readyPartitions.nonEmpty) {
      logInfo(
        s"Bubble scheduling: Dispatching ${readyPartitions.size} ready partitions for " +
          s"child stage ${childStage.id} (parent: ${parentStage.id})")
      submitReadyTasksForStage(childStage, readyPartitions.toSeq)
    }
  }

  /**
   * Check if a specific partition of the child stage has all required upstream mapper partitions
   * completed.
   */
  private def isChildPartitionReady(
      parentStage: ShuffleMapStage,
      childStage: Stage,
      childPartitionId: Int): Boolean = {
    val completedMappers = stageCompletedPartitions.get(parentStage.id)
    if (completedMappers.isEmpty) {
      return false
    }

    val completedCount = completedMappers.get.size
    completedCount == parentStage.numPartitions || {
      val minFraction = BubbleSchedulingConf.getMinUpstreamCompletionFraction(conf)
      (completedCount.toDouble / parentStage.numPartitions) >= minFraction
    }
  }

  /**
   * Submits only the tasks corresponding to ready partition IDs.
   */
  private def submitReadyTasksForStage(stage: Stage, readyPartitions: Seq[Int]): Unit = {
    val jobId = stage.firstJobId
    val tasks = new ArrayBuffer[Task[_]]()

    var taskBinary: Broadcast[Array[Byte]] = null
    var partitions: Array[org.apache.spark.Partition] = null
    try {
      var taskBinaryBytes: Array[Byte] = null
      stage match {
        case s: ShuffleMapStage =>
          taskBinaryBytes =
            closureSerializerInstance.serialize((s.rdd, s.shuffleDep): AnyRef).array()
          partitions = s.rdd.partitions
        case s: ResultStage =>
          taskBinaryBytes = closureSerializerInstance.serialize((s.rdd, s.func): AnyRef).array()
          partitions = s.rdd.partitions
      }
      taskBinary = sc.broadcast(taskBinaryBytes)
    } catch {
      case e: Exception =>
        logError(s"Failed to create broadcast task binary for Stage ${stage.id}", e)
        return
    }

    val artifacts = JobArtifactSet.getActiveOrDefault(sc)
    val rdd = stage.rdd
    val locs = (0 until rdd.partitions.length).map(i => getPreferredLocs(rdd, i)).toArray

    stage match {
      case shuffleStage: ShuffleMapStage =>
        for (p <- readyPartitions if p < rdd.partitions.length) {
          tasks += new ShuffleMapTask(
            shuffleStage.id,
            shuffleStage.latestInfo.attemptNumber(),
            taskBinary,
            partitions(p),
            shuffleStage.numPartitions,
            locs(p),
            artifacts,
            new java.util.Properties(),
            closureSerializerInstance.serialize(stage.latestInfo.taskMetrics).array(),
            Option(jobId),
            Option(sc.applicationId),
            sc.applicationAttemptId,
            shuffleStage.rdd.isBarrier())
        }

      case resultStage: ResultStage =>
        for (p <- readyPartitions if p < rdd.partitions.length) {
          tasks += new ResultTask(
            resultStage.id,
            resultStage.latestInfo.attemptNumber(),
            taskBinary,
            partitions(p),
            resultStage.numPartitions,
            locs(p),
            p,
            artifacts,
            new java.util.Properties(),
            closureSerializerInstance.serialize(stage.latestInfo.taskMetrics).array(),
            Option(jobId),
            Option(sc.applicationId),
            sc.applicationAttemptId,
            resultStage.rdd.isBarrier())
        }
    }

    if (tasks.nonEmpty) {
      if (!runningStages.contains(stage)) {
        runningStages += stage
        waitingStages -= stage
      }
      logInfo(s"Submitting TaskSet for ${tasks.size} ready tasks in Stage ${stage.id}")
      val shuffleIdOpt = stage match {
        case s: ShuffleMapStage => Some(s.shuffleDep.shuffleId)
        case _ => None
      }
      taskScheduler.submitTasks(
        new TaskSet(
          tasks.toArray,
          stage.id,
          stage.latestInfo.attemptNumber(),
          jobId,
          new java.util.Properties(),
          stage.resourceProfileId,
          shuffleIdOpt))
    }
  }

  /**
   * Fail-fast: Cancels active downstream child tasks if an upstream mapper task fails.
   */
  def onMapperTaskFailed(parentStage: ShuffleMapStage, failedPartitionId: Int): Unit = {
    logWarning(
      s"Upstream mapper partition $failedPartitionId failed in Stage " +
        s"${parentStage.id}. Cancelling dependent downstream tasks.")

    stageCompletedPartitions.get(parentStage.id).foreach(_.remove(failedPartitionId))

    val children = stageToChildStages.getOrElse(parentStage.id, Set.empty[Stage])
    for (childStage <- children) {
      val runningTasks = stageRunningTaskIds.getOrElse(childStage.id, mutable.HashMap.empty)
      for ((childPartId, taskIds) <- runningTasks if taskIds.nonEmpty) {
        logInfo(
          s"Cancelling task attempts $taskIds for child stage ${childStage.id} " +
            s"partition $childPartId due to upstream mapper failure.")
        taskScheduler.cancelTasks(
          childStage.id,
          false,
          s"Upstream mapper partition $failedPartitionId in stage ${parentStage.id} failed.")
        stageSubmittedPartitions.get(childStage.id).foreach(_.remove(childPartId))
      }
    }
  }

  private val testStageIdGenerator = new java.util.concurrent.atomic.AtomicInteger(0)

  /**
   * Helper for creating a ShuffleMapStage for testing purposes.
   */
  def createShuffleMapStageForTest(
      shuffleDep: org.apache.spark.ShuffleDependency[_, _, _],
      jobId: Int): ShuffleMapStage = {
    val rdd = shuffleDep.rdd
    val numTasks = rdd.partitions.length
    val resourceProfileId = Option(rdd.getResourceProfile)
      .map(_.id)
      .getOrElse(org.apache.spark.resource.ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID)
    val stage = new ShuffleMapStage(
      testStageIdGenerator.getAndIncrement(),
      rdd,
      numTasks,
      Nil,
      jobId,
      rdd.sparkContext.getCallSite(),
      shuffleDep,
      mapOutputTracker,
      resourceProfileId)
    stageIdToStage(stage.id) = stage
    shuffleIdToMapStage(shuffleDep.shuffleId) = stage
    stage
  }

  /**
   * Intercepts task completion events from the DAGScheduler event process loop.
   */
  override private[scheduler] def handleTaskCompletion(event: CompletionEvent): Unit = {
    val task = event.task
    val stageId = task.stageId

    event.reason match {
      case org.apache.spark.Success =>
        stageIdToStage.get(stageId).foreach {
          case shuffleStage: ShuffleMapStage =>
            onMapperTaskCompleted(shuffleStage, task.partitionId)
            // If all partitions have finished, clean up stage state
            if (shuffleStage.pendingPartitions.isEmpty) {
              cleanupBubbleStageState(shuffleStage.id)
            }
          case resultStage: ResultStage =>
            if (resultStage.findMissingPartitions().isEmpty) {
              cleanupBubbleStageState(resultStage.id)
            }
          case _ =>
        }

      case _: org.apache.spark.TaskFailedReason =>
        stageIdToStage.get(stageId).foreach {
          case shuffleStage: ShuffleMapStage =>
            onMapperTaskFailed(shuffleStage, task.partitionId)
          case _ =>
        }

      case _ =>
    }

    super.handleTaskCompletion(event)
  }

  /**
   * Intercepts stage cancellation to automatically clean up Bubble stage state.
   */
  override private[scheduler] def handleStageCancellation(
      stageId: Int,
      reason: Option[String] = None): Unit = {
    cleanupBubbleStageState(stageId)
    super.handleStageCancellation(stageId, reason)
  }

  /**
   * Cleans up tracking metadata when a stage is marked finished or cancelled.
   */
  def cleanupBubbleStageState(stageId: Int): Unit = {
    stageCompletedPartitions.remove(stageId)
    stageSubmittedPartitions.remove(stageId)
    stageToChildStages.remove(stageId)
    stageRunningTaskIds.remove(stageId)
  }
}
