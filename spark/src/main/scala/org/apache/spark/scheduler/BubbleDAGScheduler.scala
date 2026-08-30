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
import scala.collection.mutable.{ArrayBuffer, HashSet, Queue}

import org.apache.spark.{JobArtifactSet, MapOutputTrackerMaster, SparkContext, SparkEnv}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.storage.BlockManagerMaster
import org.apache.spark.util.{Clock, SystemClock}

/**
 * BubbleDAGScheduler extends Spark's [[DAGScheduler]] to support Bubble Scheduling with Remote
 * Shuffle Services (e.g. Apache Celeborn) and Active Streaming Shuffle Reads.
 *
 * Key behaviors:
 *   1. Bounded Sliding Window Dispatch: Dispatches only a budgeted window W of downstream tasks
 *      to prevent overwhelming the scheduler and exhausting cluster CPU cores. 2. Progressive
 *      Downstream Activation: Evaluates and launches the initial window of downstream tasks once
 *      a configurable fraction of upstream mappers finish. 3. Continuous Sliding Dispatch: As
 *      downstream tasks finish their partition processing, the next pending partitions in the
 *      queue are submitted immediately. 4. Deadlock Prevention: Guarantees a minimum core
 *      reservation for upstream mappers. 5. Fail-Fast Reschedule: Cancels active downstream tasks
 *      if an upstream mapper fails and retries.
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

  // Stage ID -> BitSet of completed mapper partition IDs
  private val stageCompletedPartitions = new mutable.HashMap[Int, mutable.BitSet]()

  // Child Stage ID -> BitSet of submitted partition IDs
  private val stageSubmittedPartitions = new mutable.HashMap[Int, mutable.BitSet]()

  // Child Stage ID -> Queue of pending partition IDs waiting for a slot in the sliding window
  private val stagePendingPartitionsQueue = new mutable.HashMap[Int, mutable.Queue[Int]]()

  // Child Stage ID -> Number of currently active (in-flight) tasks
  private val stageActiveTasksCount = new mutable.HashMap[Int, Int]()

  // Parent Stage ID -> Set of dependent child stages
  private val stageToChildStages = new mutable.HashMap[Int, mutable.HashSet[Stage]]()

  // Stage ID -> Map[PartitionId, Seq[Long]] (track active task IDs for cancellation)
  private val stageRunningTaskIds =
    new mutable.HashMap[Int, mutable.HashMap[Int, ArrayBuffer[Long]]]()

  private val testStageIdGenerator = new java.util.concurrent.atomic.AtomicInteger(0)

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
   * Calculates the maximum number of downstream tasks allowed to run concurrently in the sliding
   * window W to prevent deadlock and core saturation.
   */
  def computeMaxActiveDownstreamTasks(childStage: Stage): Int = {
    val explicitCap = BubbleSchedulingConf.getMaxActiveDownstreamTasks(conf)
    if (explicitCap > 0) {
      return math.min(childStage.numPartitions, explicitCap)
    }

    val totalCores = math.max(1, sc.defaultParallelism)
    val upstreamFraction = BubbleSchedulingConf.getUpstreamCoreFraction(conf)
    val downstreamCores = math.max(1, ((1.0 - upstreamFraction) * totalCores).toInt)
    math.min(childStage.numPartitions, downstreamCores)
  }

  /**
   * Check if downstream child stage can start dispatching its sliding window of tasks.
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
   * Register a completed mapper partition and trigger downstream sliding window dispatch.
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
   * Evaluates child stages in waitingStages or runningStages and dispatches the next window of
   * ready partitions.
   */
  private def checkAndSubmitReadyChildPartitions(parentStage: ShuffleMapStage): Unit = {
    val children = stageToChildStages.getOrElse(parentStage.id, Set.empty[Stage])
    for (childStage <- children
      if waitingStages.contains(childStage) || runningStages.contains(childStage)) {
      dispatchReadyPartitionsForChild(parentStage, childStage)
    }
  }

  /**
   * Dispatches tasks for the child stage within the bounded concurrency window W.
   */
  def dispatchReadyPartitionsForChild(parentStage: ShuffleMapStage, childStage: Stage): Unit = {
    val maxActive = computeMaxActiveDownstreamTasks(childStage)
    val currentActive = stageActiveTasksCount.getOrElse(childStage.id, 0)
    val availableSlots = maxActive - currentActive

    if (availableSlots <= 0) {
      return
    }

    val pendingQueue = stagePendingPartitionsQueue.getOrElseUpdate(
      childStage.id, {
        val q = new mutable.Queue[Int]()
        val submitted = stageSubmittedPartitions.getOrElseUpdate(
          childStage.id,
          new mutable.BitSet(childStage.numPartitions))
        for (p <- 0 until childStage.numPartitions if !submitted.contains(p)) {
          q.enqueue(p)
        }
        q
      })

    val partitionsToDispatch = new ArrayBuffer[Int]()
    val submittedSet = stageSubmittedPartitions.getOrElseUpdate(
      childStage.id,
      new mutable.BitSet(childStage.numPartitions))

    while (partitionsToDispatch.size < availableSlots && pendingQueue.nonEmpty) {
      val p = pendingQueue.dequeue()
      if (!submittedSet.contains(p)) {
        partitionsToDispatch += p
        submittedSet.add(p)
      }
    }

    if (partitionsToDispatch.nonEmpty) {
      stageActiveTasksCount(childStage.id) = currentActive + partitionsToDispatch.size
      logInfo(
        s"Bubble scheduling: Dispatching sliding window of ${partitionsToDispatch.size} " +
          s"partitions for child stage ${childStage.id} (active: ${stageActiveTasksCount(
              childStage.id)} / $maxActive)")
      submitReadyTasksForStage(childStage, partitionsToDispatch.toSeq)
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
            if (shuffleStage.pendingPartitions.isEmpty) {
              cleanupBubbleStageState(shuffleStage.id)
            }
          case resultStage: ResultStage =>
            // Decrement active task count and advance sliding window
            val currentActive = stageActiveTasksCount.getOrElse(resultStage.id, 1)
            stageActiveTasksCount(resultStage.id) = math.max(0, currentActive - 1)
            advanceSlidingWindowOnTaskComplete(resultStage)

            if (resultStage.findMissingPartitions().isEmpty) {
              cleanupBubbleStageState(resultStage.id)
            }
          case _ =>
        }

      case _: org.apache.spark.TaskFailedReason =>
        stageIdToStage.get(stageId).foreach {
          case shuffleStage: ShuffleMapStage =>
            onMapperTaskFailed(shuffleStage, task.partitionId)
          case childStage =>
            val currentActive = stageActiveTasksCount.getOrElse(childStage.id, 1)
            stageActiveTasksCount(childStage.id) = math.max(0, currentActive - 1)
        }

      case _ =>
    }

    super.handleTaskCompletion(event)
  }

  /**
   * Advances the sliding window by submitting the next unsubmitted partition when a task
   * completes.
   */
  private def advanceSlidingWindowOnTaskComplete(childStage: Stage): Unit = {
    val pendingQueue = stagePendingPartitionsQueue.get(childStage.id)
    if (pendingQueue.isDefined && pendingQueue.get.nonEmpty) {
      val nextPartition = pendingQueue.get.dequeue()
      val submittedSet = stageSubmittedPartitions.getOrElseUpdate(
        childStage.id,
        new mutable.BitSet(childStage.numPartitions))
      if (!submittedSet.contains(nextPartition)) {
        submittedSet.add(nextPartition)
        val currentActive = stageActiveTasksCount.getOrElse(childStage.id, 0)
        stageActiveTasksCount(childStage.id) = currentActive + 1
        logInfo(
          s"Bubble scheduling: Advancing sliding window for stage ${childStage.id} " +
            s"with next partition $nextPartition")
        submitReadyTasksForStage(childStage, Seq(nextPartition))
      }
    }
  }

  /**
   * Fail-fast: Cancels active downstream child tasks if an upstream mapper task fails.
   */
  def onMapperTaskFailed(parentStage: ShuffleMapStage, failedPartitionId: Int): Unit = {
    logWarning(
      s"Upstream mapper partition $failedPartitionId failed in Stage " +
        s"${parentStage.id}. Cancelling dependent downstream tasks in active window.")

    stageCompletedPartitions.get(parentStage.id).foreach(_.remove(failedPartitionId))

    val children = stageToChildStages.getOrElse(parentStage.id, Set.empty[Stage])
    for (childStage <- children) {
      taskScheduler.cancelTasks(
        childStage.id,
        false,
        s"Upstream mapper partition $failedPartitionId in stage ${parentStage.id} failed.")
      // Reset submitted state for pending reschedule
      stageSubmittedPartitions.remove(childStage.id)
      stageActiveTasksCount.remove(childStage.id)
      stagePendingPartitionsQueue.remove(childStage.id)
    }
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
   * Cleans up tracking metadata when a stage is marked finished or cancelled.
   */
  def cleanupBubbleStageState(stageId: Int): Unit = {
    stageCompletedPartitions.remove(stageId)
    stageSubmittedPartitions.remove(stageId)
    stagePendingPartitionsQueue.remove(stageId)
    stageActiveTasksCount.remove(stageId)
    stageToChildStages.remove(stageId)
    stageRunningTaskIds.remove(stageId)
  }
}
