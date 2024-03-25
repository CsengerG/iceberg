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

import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.iceberg.encryption.NativeFileCryptoParameters;
import org.apache.iceberg.encryption.NativelyEncryptedFile;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.metrics.MetricsContext;
import software.amazon.awssdk.services.s3.S3AsyncClient;

public class S3AsyncOutputFile extends BaseS3AsyncFile
    implements OutputFile, NativelyEncryptedFile {

  private NativeFileCryptoParameters nativeEncryptionParameters;

  public S3AsyncOutputFile(
      S3AsyncClient client,
      S3URI s3URI,
      S3FileIOProperties s3FileIOProperties,
      MetricsContext metrics) {
    super(client, s3URI, s3FileIOProperties, metrics);
  }

  public static S3AsyncOutputFile fromLocation(
      String location,
      S3AsyncClient client,
      S3FileIOProperties s3FileIOProperties,
      MetricsContext metrics) {
    return new S3AsyncOutputFile(
        client,
        new S3URI(location, s3FileIOProperties.bucketToAccessPointMapping()),
        s3FileIOProperties,
        metrics);
  }

  @Override
  public PositionOutputStream create() {
    if (!exists()) {
      return createOrOverwrite();
    } else {
      throw new AlreadyExistsException("Location already exists: %s", uri());
    }
  }

  @Override
  public PositionOutputStream createOrOverwrite() {
    try {
      return new S3AsyncOutputStream(client(), uri(), s3FileIOProperties(), metrics());
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create output stream for location: " + uri(), e);
    }
  }

  @Override
  public InputFile toInputFile() {
    return new S3AsyncInputFile(client(), uri(), null, s3FileIOProperties(), metrics());
  }

  @Override
  public NativeFileCryptoParameters nativeCryptoParameters() {
    return nativeEncryptionParameters;
  }

  @Override
  public void setNativeCryptoParameters(NativeFileCryptoParameters nativeCryptoParameters) {
    this.nativeEncryptionParameters = nativeCryptoParameters;
  }
}
