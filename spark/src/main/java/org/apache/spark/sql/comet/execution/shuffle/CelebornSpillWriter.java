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

import org.apache.celeborn.client.ShuffleClient;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.shuffle.sort.RowPartition;

import org.apache.comet.CometConf;

/** CelebornRssSpillWriter */
public abstract class CelebornSpillWriter extends SpillWriter {
  protected ShuffleClient shuffleClient;

  protected int shuffleId;
  protected int mapId;
  protected int attemptId;
  protected int partitionId;
  protected int mappersNum;
  protected int partitionNum;

  protected long doSpilling(
      byte[][] dataTypes,
      RowPartition rowPartition,
      ShuffleWriteMetricsReporter writeMetricsToUse,
      double preferDictionaryRatio,
      String compressionCodec,
      int compressionLevel,
      boolean tracingEnabled) {
    long[] addresses = rowPartition.getRowAddresses();
    int[] sizes = rowPartition.getRowSizes();

    long start = System.nanoTime();

    int batchSize = (int) CometConf.COMET_COLUMNAR_SHUFFLE_BATCH_SIZE().get();

    long result =
        nativeLib.writeToCeleborn(
            addresses,
            sizes,
            dataTypes,
            shuffleClient,
            preferDictionaryRatio,
            batchSize,
            compressionCodec,
            compressionLevel,
            tracingEnabled,
            shuffleId,
            mapId,
            attemptId,
            partitionId,
            mappersNum,
            partitionNum);

    rowPartition.reset();

    synchronized (writeMetricsToUse) {
      writeMetricsToUse.incWriteTime(System.nanoTime() - start);
      writeMetricsToUse.incRecordsWritten(addresses.length);
      writeMetricsToUse.incBytesWritten(result);
    }

    return result;
  }
}
