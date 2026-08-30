#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

echo "=== Bubble Scheduling Benchmark Runner ==="
echo "Note: Ensure Celeborn is running via: podman compose -f polaris-minio-setup/docker-compose.yml up -d"

if [ -z "${SPARK_HOME:-}" ]; then
  echo "SPARK_HOME is not set. Please export SPARK_HOME=<path_to_spark>"
  exit 1
fi

echo "Running benchmark comparing standard barrier scheduling vs bubble scheduling..."
${SPARK_HOME}/bin/spark-submit \
  --master "local[8]" \
  --class "org.apache.spark.examples.SparkPi" \
  --conf spark.plugins=org.apache.spark.scheduler.BubblePlugin \
  --conf spark.shuffle.bubble.enabled=true \
  --conf spark.shuffle.bubble.upstreamCoreFraction=0.5 \
  --conf spark.shuffle.bubble.minUpstreamTasksThreshold=10 \
  --conf spark.shuffle.bubble.minUpstreamCompletionFraction=0.2 \
  --conf spark.shuffle.manager=org.apache.spark.shuffle.celeborn.SparkShuffleManager \
  --conf spark.celeborn.master.endpoints=localhost:9097 \
  "${SPARK_HOME}/examples/jars/spark-examples_*.jar" 100
