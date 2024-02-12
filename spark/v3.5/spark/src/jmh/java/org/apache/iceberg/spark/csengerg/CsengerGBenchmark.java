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
package org.apache.iceberg.spark.csengerg;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.IOUtils;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.SingleShotTime)
public class CsengerGBenchmark {

  private static final String S3_BUCKET = "<snip>";

  @Benchmark
  @Threads(1)
  public int hello() throws InterruptedException {
    Thread.sleep(1000);
    return 1 + 1;
  }

  @SuppressWarnings("checkstyle:RegexpSinglelineJava")
  private String readObjectFully(String s3Uri) {
    S3Client s3 = S3Client.builder().region(Region.EU_WEST_1).build();
    S3FileIO s3FileIO = new S3FileIO(() -> s3);
    InputFile inputFile = s3FileIO.newInputFile(s3Uri);

    try (SeekableInputStream seekableInputStream = inputFile.newStream()) {
      return IOUtils.toString(seekableInputStream, StandardCharsets.UTF_8);
    } catch (IOException e) {
        throw new RuntimeException(e);
    }
  }

  @Benchmark
  @Threads(1)
  public void readSmallCsv() {
    readObjectFully("s3://" + S3_BUCKET + "/iceberg/call_center.dat");
  }

  @Benchmark
  @Threads(1)
  public void readBigCsv() {
    readObjectFully("s3://" + S3_BUCKET + "/iceberg/catalog_sales.dat");
  }
}
