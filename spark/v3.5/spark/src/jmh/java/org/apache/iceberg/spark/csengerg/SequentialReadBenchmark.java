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
import org.apache.iceberg.aws.s3.S3AsyncFileIO;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import software.amazon.awssdk.http.crt.AwsCrtHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;

@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 1)
@Measurement(iterations = 1)
@BenchmarkMode(Mode.SingleShotTime)
public class SequentialReadBenchmark extends BenchmarkBase {

  // Objects from TPC-DS dataset
  @Param({
          "ship_mode.dat",
          "web_site.dat",
          "promotion.dat",
          "household_demographics.dat",
          "catalog_page.dat",
          "customer_address.dat",
          "customer.dat",
          "store_returns.dat",
          "customer_demographics.dat",
          "web_sales.dat",
          "catalog_sales.dat"
  })
  private String key;

  private String sync__readObjectFully(String s3Uri) {
    S3Client s3 = S3Client.builder().region(Region.EU_WEST_1).build();

    try (S3FileIO s3FileIO = new S3FileIO(() -> s3)) {
      InputFile inputFile = s3FileIO.newInputFile(s3Uri);
      return readFile(inputFile);
    }
  }

  private String async__readObjectFully(String s3Uri) {
    S3AsyncClient s3 = S3AsyncClient.builder().region(Region.EU_WEST_1).build();

    try (S3AsyncFileIO s3FileIO = new S3AsyncFileIO(() -> s3)) {
      InputFile inputFile = s3FileIO.newInputFile(s3Uri);
      return readFile(inputFile);
    }
  }

  private String s3_crt__readObjectFully(String s3Uri) {
    S3AsyncClient s3 = S3AsyncClient.crtBuilder().region(Region.EU_WEST_1).build();

    try (S3AsyncFileIO s3FileIO = new S3AsyncFileIO(() -> s3)) {
      InputFile inputFile = s3FileIO.newInputFile(s3Uri);
      return readFile(inputFile);
    }
  }

  private String http_crt__readObjectFully(String s3Uri) {
    S3Client s3 = S3Client.builder()
      .region(Region.EU_WEST_1)
      .httpClient(AwsCrtHttpClient.builder().build())
      .build();

    try (S3FileIO s3FileIO = new S3FileIO(() -> s3)) {
      InputFile inputFile = s3FileIO.newInputFile(s3Uri);
      return readFile(inputFile);
    }
  }

  @SuppressWarnings("checkstyle:RegexpSinglelineJava")
  private String readFile(InputFile inputFile) {
    try (SeekableInputStream seekableInputStream = inputFile.newStream()) {
      return IOUtils.toString(seekableInputStream, StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Threads(1)
  @Benchmark
  public void sync__readCsv() {
    sync__readObjectFully(getS3Path());
  }

  @Threads(1)
  @Benchmark
  public void async__readCsv() {
    async__readObjectFully(getS3Path());
  }

  @Threads(1)
  @Benchmark
  public void async__http_crt_readCsv() {
    http_crt__readObjectFully(getS3Path());
  }

  @Threads(1)
  @Benchmark
  public void async__s3_crt_readCsv() {
    s3_crt__readObjectFully(getS3Path());
  }

  private String getS3Path() {
    return "s3://" + S3_BUCKET + "/iceberg/" + key;
  }
}
