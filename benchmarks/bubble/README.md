<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Bubble Scheduling Benchmark & Testing Scripts

This directory provides scripts to test and benchmark **Bubble Scheduling with Apache Celeborn RSS**.

## Prerequisites

1. **Start Celeborn 0.7.0 Service**:
   ```bash
   podman compose -f polaris-minio-setup/docker-compose.yml up -d
   ```
   Verify Master UI at [http://localhost:9098](http://localhost:9098).

2. **Set `SPARK_HOME`**:
   ```bash
   export SPARK_HOME=/path/to/spark
   ```

---

## Available Scripts

### 1. Interactive Spark Shell (`run-bubble-spark-shell.sh`)
Launches `spark-shell` pre-configured with `BubblePlugin` and Celeborn Remote Shuffle Service:
```bash
./benchmarks/bubble/run-bubble-spark-shell.sh
```

Inside the shell, test multi-stage shuffle jobs:
```scala
val df = spark.range(0, 1000000).repartition(200).groupBy($"id" % 50).count()
df.show()
```

### 2. Benchmark Runner (`run-bubble-benchmarks.sh`)
Submits a Spark job with bubble scheduling enabled:
```bash
./benchmarks/bubble/run-bubble-benchmarks.sh
```

---

## Further Documentation
For detailed architecture and configuration guides, refer to the [Bubble Scheduling Contributor Guide](../../docs/source/contributor-guide/bubble_scheduling.md).
