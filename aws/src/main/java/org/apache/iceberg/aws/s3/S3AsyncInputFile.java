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
package org.apache.iceberg.aws.s3;

import org.apache.iceberg.encryption.NativeFileCryptoParameters;
import org.apache.iceberg.encryption.NativelyEncryptedFile;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.metrics.MetricsContext;
import software.amazon.awssdk.services.s3.S3AsyncClient;

public class S3AsyncInputFile extends BaseS3AsyncFile implements InputFile, NativelyEncryptedFile {

  private Long length;

  public S3AsyncInputFile(
      S3AsyncClient client,
      S3URI s3URI,
      Long length,
      S3FileIOProperties s3FileIOProperties,
      MetricsContext metrics) {
    super(client, s3URI, s3FileIOProperties, metrics);
    this.length = length;
  }

  public static S3AsyncInputFile fromLocation(
      String location,
      S3AsyncClient client,
      S3FileIOProperties s3FileIOProperties,
      MetricsContext metrics) {
    return new S3AsyncInputFile(
        client,
        new S3URI(location, s3FileIOProperties.bucketToAccessPointMapping()),
        null,
        s3FileIOProperties,
        metrics);
  }

  @Override
  public long getLength() {
    return 0;
  }

  @Override
  public SeekableInputStream newStream() {
    return new S3AsyncInputStream(client(), uri(), s3FileIOProperties(), metrics());
  }

  @Override
  public NativeFileCryptoParameters nativeCryptoParameters() {
    return null;
  }

  @Override
  public void setNativeCryptoParameters(NativeFileCryptoParameters nativeCryptoParameters) {}
}
