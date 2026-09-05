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

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.{MapOutputTrackerMaster, SparkConf, SparkContext, SparkEnv, SparkFunSuite}
import org.apache.spark.storage.BlockManagerMaster
import org.apache.spark.util.SystemClock

class BubbleDAGSchedulerSuite extends SparkFunSuite with BeforeAndAfterEach {

  private var sc: SparkContext = _

  override def afterEach(): Unit = {
    try {
      if (sc != null) {
        sc.stop()
        sc = null
      }
    } finally {
      super.afterEach()
    }
  }

  test("BubbleSchedulingConf: default configuration values") {
    val conf = new SparkConf()
    assert(!BubbleSchedulingConf.isBubbleSchedulingEnabled(conf))
    assert(BubbleSchedulingConf.getMinUpstreamTasksThreshold(conf) === 10)
    assert(BubbleSchedulingConf.getMinUpstreamCompletionFraction(conf) === 0.2)
    assert(BubbleSchedulingConf.getMaxConcurrentStages(conf) === 2)
    assert(BubbleSchedulingConf.getUpstreamCoreFraction(conf) === 0.5)
    assert(BubbleSchedulingConf.getMaxActiveDownstreamTasks(conf) === 0)
    assert(BubbleSchedulingConf.getReaderPollTimeoutMs(conf) === 100L)
  }

  test("BubbleSchedulingConf: custom configuration values") {
    val conf = new SparkConf()
      .set("spark.shuffle.bubble.enabled", "true")
      .set("spark.shuffle.bubble.minUpstreamTasksThreshold", "5")
      .set("spark.shuffle.bubble.minUpstreamCompletionFraction", "0.35")
      .set("spark.shuffle.bubble.maxConcurrentStages", "4")
      .set("spark.shuffle.bubble.upstreamCoreFraction", "0.6")
      .set("spark.shuffle.bubble.maxActiveDownstreamTasks", "8")
      .set("spark.shuffle.bubble.readerPollTimeoutMs", "250ms")

    assert(BubbleSchedulingConf.isBubbleSchedulingEnabled(conf))
    assert(BubbleSchedulingConf.getMinUpstreamTasksThreshold(conf) === 5)
    assert(BubbleSchedulingConf.getMinUpstreamCompletionFraction(conf) === 0.35)
    assert(BubbleSchedulingConf.getMaxConcurrentStages(conf) === 4)
    assert(BubbleSchedulingConf.getUpstreamCoreFraction(conf) === 0.6)
    assert(BubbleSchedulingConf.getMaxActiveDownstreamTasks(conf) === 8)
    assert(BubbleSchedulingConf.getReaderPollTimeoutMs(conf) === 250L)
  }

  test("BubbleDAGScheduler: eligibility check and small-stage fallback") {
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("BubbleSchedulerTest")
      .set("spark.shuffle.bubble.enabled", "true")
      .set("spark.shuffle.bubble.minUpstreamTasksThreshold", "10")

    sc = new SparkContext(conf)
    val rddSmall = sc.parallelize(1 to 100, 2)
    val rddLarge = sc.parallelize(1 to 100, 20)

    val env = SparkEnv.get
    val scheduler = new BubbleDAGScheduler(
      sc,
      sc.taskScheduler,
      sc.listenerBus,
      env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster],
      env.blockManager.master,
      env,
      new SystemClock())

    // Small stage with 2 partitions should fall back to stage-barrier
    val dummyJobId = 0
    val smallShuffleDep = new org.apache.spark.ShuffleDependency[Int, Int, Int](
      rddSmall.map(x => (x, x)),
      new org.apache.spark.HashPartitioner(2))
    val smallStage = scheduler.createShuffleMapStageForTest(smallShuffleDep, dummyJobId)
    assert(!scheduler.isBubbleSchedulingEligible(smallStage))

    // Large stage with 20 partitions should be eligible
    val largeShuffleDep = new org.apache.spark.ShuffleDependency[Int, Int, Int](
      rddLarge.map(x => (x, x)),
      new org.apache.spark.HashPartitioner(20))
    val largeStage = scheduler.createShuffleMapStageForTest(largeShuffleDep, dummyJobId)
    assert(scheduler.isBubbleSchedulingEligible(largeStage))
  }

  test("BubbleDAGScheduler: compute bounded sliding window size") {
    val conf = new SparkConf()
      .setMaster("local[8]")
      .setAppName("BubbleSchedulerWindowTest")
      .set("spark.shuffle.bubble.enabled", "true")
      .set("spark.shuffle.bubble.upstreamCoreFraction", "0.5")

    sc = new SparkContext(conf)
    val rdd200 = sc.parallelize(1 to 1000, 200)
    val env = SparkEnv.get
    val scheduler = new BubbleDAGScheduler(
      sc,
      sc.taskScheduler,
      sc.listenerBus,
      env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster],
      env.blockManager.master,
      env,
      new SystemClock())

    val shuffleDep = new org.apache.spark.ShuffleDependency[Int, Int, Int](
      rdd200.map(x => (x, x)),
      new org.apache.spark.HashPartitioner(200))
    val stage = scheduler.createShuffleMapStageForTest(shuffleDep, 0)

    // With 8 total cores and 0.5 upstream fraction, max active downstream tasks = 4
    val maxActive = scheduler.computeMaxActiveDownstreamTasks(stage)
    assert(maxActive === 4)
  }

  test("BubbleDAGScheduler: progressive completion tracking and state cleanup") {
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("BubbleSchedulerProgressTest")
      .set("spark.shuffle.bubble.enabled", "true")
      .set("spark.shuffle.bubble.minUpstreamTasksThreshold", "10")
      .set("spark.shuffle.bubble.minUpstreamCompletionFraction", "0.2")

    sc = new SparkContext(conf)
    val rdd = sc.parallelize(1 to 100, 10)
    val env = SparkEnv.get
    val scheduler = new BubbleDAGScheduler(
      sc,
      sc.taskScheduler,
      sc.listenerBus,
      env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster],
      env.blockManager.master,
      env,
      new SystemClock())

    val shuffleDep = new org.apache.spark.ShuffleDependency[Int, Int, Int](
      rdd.map(x => (x, x)),
      new org.apache.spark.HashPartitioner(10))
    val stage = scheduler.createShuffleMapStageForTest(shuffleDep, 0)

    // Task completions
    scheduler.onMapperTaskCompleted(stage, 0)
    scheduler.onMapperTaskCompleted(stage, 1)

    // Failure triggers fail-fast cancellation
    scheduler.onMapperTaskFailed(stage, 1)

    // State cleanup
    scheduler.cleanupBubbleStageState(stage.id)
  }

  test(
    "BubbleDAGScheduler: validates that bubble scheduling is happening (child starts before parent finishes)") {
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("BubbleValidationTest")
      .set("spark.shuffle.bubble.enabled", "true")
      .set("spark.shuffle.bubble.minUpstreamTasksThreshold", "10")
      .set("spark.shuffle.bubble.minUpstreamCompletionFraction", "0.2")

    sc = new SparkContext(conf)
    val parentRDD = sc.parallelize(1 to 100, 10)
    val childRDD = parentRDD.map(x => (x % 5, x))
    val env = SparkEnv.get
    val scheduler = new BubbleDAGScheduler(
      sc,
      sc.taskScheduler,
      sc.listenerBus,
      env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster],
      env.blockManager.master,
      env,
      new SystemClock())

    val parentDep = new org.apache.spark.ShuffleDependency[Int, Int, Int](
      parentRDD.map(x => (x, x)),
      new org.apache.spark.HashPartitioner(10))
    val parentStage = scheduler.createShuffleMapStageForTest(parentDep, 0)

    val childStage = scheduler.createResultStageForTest(childRDD, 0, parents = List(parentStage))

    // Initially, child stage is blocked in waitingStages
    scheduler.waitingStages += childStage
    assert(scheduler.waitingStages.contains(childStage))
    assert(!scheduler.runningStages.contains(childStage))

    // 1 task finishes (10% completion < 20% threshold): child stage should NOT start yet
    scheduler.onMapperTaskCompleted(parentStage, 0)
    assert(
      scheduler.waitingStages.contains(childStage),
      "Child stage should remain in waitingStages when below completion fraction threshold")
    assert(!scheduler.runningStages.contains(childStage))

    // 2nd task finishes (20% completion >= 20% threshold):
    // BUBBLE SCHEDULING TRIGGERED!
    // Downstream child stage is eagerly admitted to runningStages and dispatched
    // while parent stage still has 8 out of 10 tasks pending!
    scheduler.onMapperTaskCompleted(parentStage, 1)

    assert(
      scheduler.runningStages.contains(childStage),
      "Bubble scheduling validation failed: Child stage was not admitted to runningStages while parent was still running")
    assert(
      !scheduler.waitingStages.contains(childStage),
      "Child stage should have transitioned out of waitingStages into runningStages")
    assert(parentStage.numPartitions === 10, "Parent stage must have 10 total partitions")
  }
}
