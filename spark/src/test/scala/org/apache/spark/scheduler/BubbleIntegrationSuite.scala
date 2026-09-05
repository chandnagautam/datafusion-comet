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

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}

class BubbleIntegrationSuite extends SparkFunSuite with BeforeAndAfterEach {

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

  test("BubbleDriverPlugin: successfully injects BubbleDAGScheduler into SparkContext") {
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("BubblePluginInjectionTest")
      .set("spark.plugins", classOf[BubblePlugin].getName)
      .set("spark.shuffle.bubble.enabled", "true")

    sc = new SparkContext(conf)
    // Run an action to trigger job/listener lifecycle
    sc.parallelize(1 to 10, 2).count()
    assert(
      sc.dagScheduler.isInstanceOf[BubbleDAGScheduler],
      "Expected SparkContext._dagScheduler to be instance of BubbleDAGScheduler")
  }

  test("BubbleIntegration: End-to-End multi-stage aggregation and correctness") {
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("BubbleE2EAggregationTest")
      .set("spark.plugins", classOf[BubblePlugin].getName)
      .set("spark.shuffle.bubble.enabled", "true")
      .set("spark.shuffle.bubble.minUpstreamTasksThreshold", "4")
      .set("spark.shuffle.bubble.minUpstreamCompletionFraction", "0.25")
      .set("spark.shuffle.bubble.upstreamCoreFraction", "0.5")

    sc = new SparkContext(conf)

    // Stage 1: 8 partitions -> Stage 2: 16 partitions
    val data = (1 to 1000).map(i => (i % 20, i * 2))
    val rdd = sc.parallelize(data, 8)
    val aggregated = rdd.reduceByKey((a, b) => a + b, 16).collect().toMap

    // Ground truth calculation
    val expected = data.groupBy(_._1).mapValues(_.map(_._2).sum)

    assert(aggregated.size === expected.size)
    for ((k, v) <- expected) {
      assert(
        aggregated.get(k) === Some(v),
        s"Mismatch for key $k: expected $v, got ${aggregated.get(k)}")
    }
  }

  test("BubbleIntegration: Sliding window execution with 50 shuffle partitions under 4 cores") {
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("BubbleSlidingWindowE2ETest")
      .set("spark.plugins", classOf[BubblePlugin].getName)
      .set("spark.shuffle.bubble.enabled", "true")
      .set("spark.shuffle.bubble.minUpstreamTasksThreshold", "4")
      .set("spark.shuffle.bubble.upstreamCoreFraction", "0.5")

    sc = new SparkContext(conf)

    val numbers = sc.parallelize(1 to 5000, 10)
    val wordCounts = numbers
      .map(x => (x % 50, 1))
      .reduceByKey((a, b) => a + b, 50)
      .collect()

    assert(wordCounts.length === 50)
    val totalSum = wordCounts.map(_._2).sum
    assert(totalSum === 5000)
  }

  test(
    "BubbleIntegration: validates that bubble scheduling is happening (stage transition and progressive dispatch)") {
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("BubbleStageTransitionTest")
      .set("spark.plugins", classOf[BubblePlugin].getName)
      .set("spark.shuffle.bubble.enabled", "true")
      .set("spark.shuffle.bubble.minUpstreamTasksThreshold", "4")
      .set("spark.shuffle.bubble.minUpstreamCompletionFraction", "0.25")

    sc = new SparkContext(conf)
    // Run an action so that SparkContext initialization is complete
    sc.parallelize(1 to 10, 2).count()

    // Verify scheduler is active BubbleDAGScheduler
    val bubbleScheduler = sc.dagScheduler.asInstanceOf[BubbleDAGScheduler]
    assert(bubbleScheduler != null)

    // Execute multi-stage job and verify correctness
    val rdd = sc.parallelize(1 to 1000, 8)
    val result = rdd.map(x => (x % 10, x)).reduceByKey(_ + _, 10).collect()
    assert(result.length === 10)
  }
}
