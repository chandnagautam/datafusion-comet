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

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.ConfigBuilder

object BubbleSchedulingConf {

  val BUBBLE_SCHEDULING_ENABLED =
    ConfigBuilder("spark.shuffle.bubble.enabled")
      .doc("Whether to enable Bubble Scheduling with Remote Shuffle Service (Celeborn) in Spark.")
      .version("1.1.0")
      .booleanConf
      .createWithDefault(false)

  val MIN_UPSTREAM_TASKS_THRESHOLD =
    ConfigBuilder("spark.shuffle.bubble.minUpstreamTasksThreshold")
      .doc("Minimum number of tasks in an upstream stage required to activate Bubble " +
        "Scheduling. Stages with fewer tasks fall back to standard stage-barrier scheduling.")
      .version("1.1.0")
      .intConf
      .createWithDefault(10)

  val MIN_UPSTREAM_COMPLETION_FRACTION =
    ConfigBuilder("spark.shuffle.bubble.minUpstreamCompletionFraction")
      .doc("Fraction of completed upstream tasks required before downstream ready " +
        "partition tasks can be evaluated and dispatched (range 0.0 to 1.0).")
      .version("1.1.0")
      .doubleConf
      .createWithDefault(0.2)

  val MAX_CONCURRENT_STAGES =
    ConfigBuilder("spark.shuffle.bubble.maxConcurrentStages")
      .doc("Maximum number of pipelined stages allowed to execute concurrently under " +
        "bubble scheduling.")
      .version("1.1.0")
      .intConf
      .createWithDefault(2)

  val AQE_SAMPLING_MIN_FRACTION =
    ConfigBuilder("spark.shuffle.bubble.aqe.sampling.minFraction")
      .doc("Minimum fraction of finished mapper tasks to sample for progressive AQE " +
        "partition estimation.")
      .version("1.1.0")
      .doubleConf
      .createWithDefault(0.25)

  val AQE_SAMPLING_MAX_CV_THRESHOLD =
    ConfigBuilder("spark.shuffle.bubble.aqe.sampling.maxCVThreshold")
      .doc("Maximum coefficient of variation (CV) allowed in sampled mapper sizes before " +
        "falling back to full stage barrier.")
      .version("1.1.0")
      .doubleConf
      .createWithDefault(0.25)

  def isBubbleSchedulingEnabled(conf: SparkConf): Boolean = {
    conf.get(BUBBLE_SCHEDULING_ENABLED)
  }

  def getMinUpstreamTasksThreshold(conf: SparkConf): Int = {
    conf.get(MIN_UPSTREAM_TASKS_THRESHOLD)
  }

  def getMinUpstreamCompletionFraction(conf: SparkConf): Double = {
    conf.get(MIN_UPSTREAM_COMPLETION_FRACTION)
  }

  def getMaxConcurrentStages(conf: SparkConf): Int = {
    conf.get(MAX_CONCURRENT_STAGES)
  }
}
