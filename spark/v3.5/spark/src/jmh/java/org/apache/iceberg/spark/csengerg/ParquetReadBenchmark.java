/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg.spark.csengerg;

import java.io.IOException;
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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 1)
@Measurement(iterations = 1)
@BenchmarkMode(Mode.SingleShotTime)
public class ParquetReadBenchmark extends BenchmarkBase {

    private static final Schema SCHEMA =
            new Schema(
                    required(1, "A", Types.StringType.get()),
                    optional(2, "B", Types.StringType.get()),
                    optional(3, "C", Types.StringType.get()),
                    optional(4, "D", Types.StringType.get()),
                    optional(5, "E", Types.StringType.get()),
                    optional(6, "F", Types.StringType.get()),
                    optional(7, "G", Types.StringType.get()),
                    optional(8, "H", Types.StringType.get()),
                    optional(9, "I", Types.StringType.get()),
                    optional(10, "J", Types.StringType.get()),
                    optional(11, "K", Types.StringType.get()),
                    optional(12, "L", Types.StringType.get()),
                    optional(13, "M", Types.StringType.get()),
                    optional(14, "N", Types.StringType.get()),
                    optional(15, "O", Types.StringType.get()),
                    optional(16, "P", Types.StringType.get()),
                    optional(17, "Q", Types.StringType.get()),
                    optional(18, "R", Types.StringType.get()),
                    optional(19, "S", Types.StringType.get()),
                    optional(20, "T", Types.StringType.get()),
                    optional(21, "U", Types.StringType.get()),
                    optional(22, "V", Types.StringType.get()),
                    optional(23, "W", Types.StringType.get()),
                    optional(24, "X", Types.StringType.get()));

    @Param({
            "/iceberg-parquet/store_sales.parquet"
    })
    private String key;

    @Threads(1)
    @Benchmark
    public void s3__readUsingIcebergReader(Blackhole blackHole) throws IOException {
        S3Client s3 = S3Client.builder().region(Region.EU_WEST_1).build();

        try (S3FileIO s3FileIO = new S3FileIO(() -> s3)) {
            InputFile inputFile = s3FileIO.newInputFile(getS3Uri());

            try (CloseableIterable<InternalRow> rows =
                         Parquet.read(inputFile)
                                 .project(SCHEMA)
                                 .createReaderFunc(type -> SparkParquetReaders.buildReader(SCHEMA, type))
                                 .build()) {

                for (InternalRow row : rows) {
                    blackHole.consume(row);
                }
            }
        }
    }

    @Threads(1)
    @Benchmark
    public void async__readUsingIcebergReader(Blackhole blackHole) throws IOException {
        S3AsyncClient s3 = S3AsyncClient.builder()
                .region(Region.EU_WEST_1)
                .build();

        try (S3AsyncFileIO s3FileIO = new S3AsyncFileIO(() -> s3)) {
            InputFile inputFile = s3FileIO.newInputFile(getS3Uri());

            try (CloseableIterable<InternalRow> rows =
                         Parquet.read(inputFile)
                                 .project(SCHEMA)
                                 .createReaderFunc(type -> SparkParquetReaders.buildReader(SCHEMA, type))
                                 .build()) {

                for (InternalRow row : rows) {
                    blackHole.consume(row);
                }
            }
        }
    }

    @Threads(1)
    @Benchmark
    public void s3_crt__readUsingIcebergReader(Blackhole blackHole) throws IOException {
        S3AsyncClient s3 = S3AsyncClient.crtBuilder()
                .region(Region.EU_WEST_1)
                .build();

        try (S3AsyncFileIO s3FileIO = new S3AsyncFileIO(() -> s3)) {
            InputFile inputFile = s3FileIO.newInputFile(getS3Uri());

            try (CloseableIterable<InternalRow> rows =
                         Parquet.read(inputFile)
                                 .project(SCHEMA)
                                 .createReaderFunc(type -> SparkParquetReaders.buildReader(SCHEMA, type))
                                 .build()) {

                for (InternalRow row : rows) {
                    blackHole.consume(row);
                }
            }
        }
    }

    private String getS3Uri() {
        return "s3://" + S3_BUCKET + key;
    }
}
