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
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.aws.s3.S3AsyncFileIO;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.spark.data.SparkParquetReaders;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
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
import org.openjdk.jmh.infra.Blackhole;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;

@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 2)
@Measurement(iterations = 100)
@BenchmarkMode(Mode.SingleShotTime)
public class S3ParquetReadBenchmark extends BenchmarkBase {

  private Schema getSchema() {
    List<Types.NestedField> schema = new ArrayList<>();

    schema.add(Types.NestedField.required(1, "c0", Types.IntegerType.get()));
    schema.add(Types.NestedField.required(2, "c1", Types.IntegerType.get()));

    return new Schema(schema);
  }

  // Full run took 38m 46s
  @Param({
    "single_col_random__128kb_row_group_100k_rows.parquet",
    "single_col_random__128kb_row_group_100m_rows.parquet",
    "single_col_random__128kb_row_group_10k_rows.parquet",
    "single_col_random__128kb_row_group_10m_rows.parquet",
    "single_col_random__128kb_row_group_1m_rows.parquet",
    "single_col_random__128mb_row_group_100k_rows.parquet",
    "single_col_random__128mb_row_group_100m_rows.parquet",
    "single_col_random__128mb_row_group_10k_rows.parquet",
    "single_col_random__128mb_row_group_10m_rows.parquet",
    "single_col_random__128mb_row_group_1m_rows.parquet",
    "single_col_random__16kb_row_group_100k_rows.parquet",
    "single_col_random__16kb_row_group_100m_rows.parquet",
    "single_col_random__16kb_row_group_10k_rows.parquet",
    "single_col_random__16kb_row_group_10m_rows.parquet",
    "single_col_random__16kb_row_group_1m_rows.parquet",
    "single_col_random__16mb_row_group_100k_rows.parquet",
    "single_col_random__16mb_row_group_100m_rows.parquet",
    "single_col_random__16mb_row_group_10k_rows.parquet",
    "single_col_random__16mb_row_group_10m_rows.parquet",
    "single_col_random__16mb_row_group_1m_rows.parquet",
    "single_col_random__1gb_row_group_100k_rows.parquet",
    "single_col_random__1gb_row_group_100m_rows.parquet",
    "single_col_random__1gb_row_group_10k_rows.parquet",
    "single_col_random__1gb_row_group_10m_rows.parquet",
    "single_col_random__1gb_row_group_1m_rows.parquet",
    "single_col_random__1kb_row_group_100k_rows.parquet",
    "single_col_random__1kb_row_group_100m_rows.parquet",
    "single_col_random__1kb_row_group_10k_rows.parquet",
    "single_col_random__1kb_row_group_10m_rows.parquet",
    "single_col_random__1kb_row_group_1m_rows.parquet",
    "single_col_random__1mb_row_group_100k_rows.parquet",
    "single_col_random__1mb_row_group_100m_rows.parquet",
    "single_col_random__1mb_row_group_10k_rows.parquet",
    "single_col_random__1mb_row_group_10m_rows.parquet",
    "single_col_random__1mb_row_group_1m_rows.parquet",
    "single_col_random__512mb_row_group_100k_rows.parquet",
    "single_col_random__512mb_row_group_100m_rows.parquet",
    "single_col_random__512mb_row_group_10k_rows.parquet",
    "single_col_random__512mb_row_group_10m_rows.parquet",
    "single_col_random__512mb_row_group_1m_rows.parquet",
  })
  private String key;

  private void consumeParquet(InputFile inputFile, Blackhole blackHole) throws IOException {
    Schema schema = getSchema();

    try (CloseableIterable<InternalRow> rows =
        Parquet.read(inputFile)
            .project(schema)
            .createReaderFunc(type -> SparkParquetReaders.buildReader(schema, type))
            .build()) {

      for (InternalRow row : rows) {
        blackHole.consume(row);
      }
    }
  }

  @Threads(1)
  @Benchmark
  public void sync_ParquetSeqRead(Blackhole blackHole) throws IOException {
    S3Client s3 = S3Client.builder().region(Region.EU_WEST_1).build();

    try (S3FileIO s3FileIO = new S3FileIO(() -> s3)) {
      InputFile inputFile = s3FileIO.newInputFile(getS3Uri());
      consumeParquet(inputFile, blackHole);
    }
  }

  @Threads(1)
  @Benchmark
  public void async_ParquetSeqRead(Blackhole blackHole) throws IOException {
    S3AsyncClient s3 = S3AsyncClient.builder().region(Region.EU_WEST_1).build();

    try (S3AsyncFileIO s3FileIO = new S3AsyncFileIO(() -> s3)) {
      InputFile inputFile = s3FileIO.newInputFile(getS3Uri());
      consumeParquet(inputFile, blackHole);
    }
  }

  @Threads(1)
  @Benchmark
  public void s3crt_ParquetSeqRead(Blackhole blackHole) throws IOException {
    S3AsyncClient s3 = S3AsyncClient.crtBuilder().region(Region.EU_WEST_1).build();

    try (S3AsyncFileIO s3FileIO = new S3AsyncFileIO(() -> s3)) {
      InputFile inputFile = s3FileIO.newInputFile(getS3Uri());
      consumeParquet(inputFile, blackHole);
    }
  }

  private static final String PREFIX = "/parquet-entitlement/";

  private String getS3Uri() {
    return "s3://" + S3_BUCKET + PREFIX + key;
  }
}
