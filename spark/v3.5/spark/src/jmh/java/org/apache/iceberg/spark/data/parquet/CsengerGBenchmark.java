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
package org.apache.iceberg.spark.data.parquet;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * The way to develop this:
 * 1. Make a change
 * 2. Apply formatting
 *    ./gradlew spotlessApply
 * 3. Compile
 *    ./gradlew build -x test -x integrationTest
 * 4. Execute
 *    ./gradlew \
 *      -DsparkVersions=3.5 \
 *      :iceberg-spark:iceberg-spark-3.5_2.12:jmh \
 *      -PjmhIncludeRegex=CsengerGBenchmark \
 *      -PjmhOutputPath=benchmark/csengerg-benchmark-result.txt
 */

@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.SingleShotTime)
public class CsengerGBenchmark {

  @Benchmark
  @Threads(1)
  public int hello() throws InterruptedException {
    Thread.sleep(1000);
    return 1 + 1;
  }
}
