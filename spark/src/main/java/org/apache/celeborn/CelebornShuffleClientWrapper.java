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

package org.apache.celeborn;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.celeborn.client.ShuffleClient;

public class CelebornShuffleClientWrapper {
  private ShuffleClient shuffleClient;

  // Maintains map of partition id vs bytes written map
  private Map<Integer, Integer> shuffleMetadataMap = new ConcurrentHashMap<>();

  public CelebornShuffleClientWrapper(ShuffleClient shuffleClient) {
    this.shuffleClient = shuffleClient;
  }

  public int pushData(
      int shuffleId,
      int mapId,
      int attemptId,
      int partitionId,
      byte[] data,
      int offset,
      int length,
      int numMappers,
      int numPartitions)
      throws IOException {
    int writtenData =
        shuffleClient.pushData(
            shuffleId,
            mapId,
            attemptId,
            partitionId,
            data,
            offset,
            length,
            numMappers,
            numPartitions);
    shuffleMetadataMap.put(partitionId, writtenData);

    return writtenData;
  }

  public int mergeData(
      int shuffleId,
      int mapId,
      int attemptId,
      int partitionId,
      byte[] data,
      int offset,
      int length,
      int numMappers,
      int numPartitions)
      throws IOException {
    int writtenData =
        shuffleClient.mergeData(
            shuffleId,
            mapId,
            attemptId,
            partitionId,
            data,
            offset,
            length,
            numMappers,
            numPartitions);
    shuffleMetadataMap.put(partitionId, writtenData);
    return writtenData;
  }

  public Map<Integer, Integer> getShuffleDataMetrics() {
    return shuffleMetadataMap;
  }
}
