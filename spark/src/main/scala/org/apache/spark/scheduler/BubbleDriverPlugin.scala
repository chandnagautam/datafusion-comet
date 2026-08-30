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

import java.lang.reflect.Field
import java.util.{Map => JMap}

import org.apache.spark.{MapOutputTrackerMaster, SparkContext}
import org.apache.spark.api.plugin.{DriverPlugin, PluginContext, SparkPlugin}
import org.apache.spark.internal.Logging

/**
 * BubbleDriverPlugin is a Spark DriverPlugin that enables Bubble Scheduling by dynamically
 * attaching [[BubbleDAGScheduler]] to the active [[SparkContext]].
 *
 * Usage:
 * {{{
 *   spark.plugins=org.apache.spark.scheduler.BubblePlugin
 *   spark.shuffle.bubble.enabled=true
 * }}}
 */
class BubbleDriverPlugin extends DriverPlugin with Logging {

  override def init(sc: SparkContext, pluginContext: PluginContext): JMap[String, String] = {
    val conf = sc.getConf
    if (BubbleSchedulingConf.isBubbleSchedulingEnabled(conf)) {
      logInfo("BubbleDriverPlugin: Initializing BubbleDAGScheduler hook...")

      // Attempt immediate attachment
      attachBubbleScheduler(sc)

      // Also register a SparkListener to ensure BubbleDAGScheduler is attached
      // after SparkContext finishes its internal constructor setup
      sc.addSparkListener(new SparkListener {
        override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
          attachBubbleScheduler(sc)
        }

        override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
          attachBubbleScheduler(sc)
        }
      })
    }
    java.util.Collections.emptyMap()
  }

  def attachBubbleScheduler(sc: SparkContext): Unit = {
    try {
      if (sc != null && sc.env != null && !sc.dagScheduler.isInstanceOf[BubbleDAGScheduler]) {
        val env = sc.env
        val bubbleScheduler = new BubbleDAGScheduler(
          sc,
          sc.taskScheduler,
          sc.listenerBus,
          env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster],
          env.blockManager.master,
          env)

        val setterMethod = classOf[SparkContext].getMethods.find(m =>
          m.getName == "_dagScheduler_$eq" ||
            m.getName == "dagScheduler_$eq" ||
            m.getName == "_dagScheduler_=")

        setterMethod match {
          case Some(m) =>
            m.setAccessible(true)
            m.invoke(sc, bubbleScheduler)
          case None =>
            val field: Field = classOf[SparkContext].getDeclaredField("_dagScheduler")
            field.setAccessible(true)
            field.set(sc, bubbleScheduler)
        }
        logInfo("BubbleDriverPlugin: Successfully attached BubbleDAGScheduler to SparkContext.")
      }
    } catch {
      case e: Exception =>
        logError("BubbleDriverPlugin: Failed to attach BubbleDAGScheduler", e)
    }
  }
}

/**
 * Entry point for --conf spark.plugins=org.apache.spark.scheduler.BubblePlugin
 */
class BubblePlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = new BubbleDriverPlugin()
  override def executorPlugin(): org.apache.spark.api.plugin.ExecutorPlugin = null
}
