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

import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.CredentialSupplier;
import org.apache.iceberg.io.DelegateFileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.util.SerializableSupplier;
import software.amazon.awssdk.services.s3.S3AsyncClient;

public class S3AsyncFileIO implements CredentialSupplier, DelegateFileIO {
  private String credential = null;
  private S3AsyncClient client = null;
  private SerializableSupplier<S3AsyncClient> s3;

  private S3FileIOProperties s3FileIOProperties;

  private MetricsContext metrics = MetricsContext.nullMetrics();

  private transient StackTraceElement[] createStack;

  public S3AsyncFileIO(SerializableSupplier<S3AsyncClient> s3) {
    this(s3, new S3FileIOProperties());
  }

  public S3AsyncFileIO(
      SerializableSupplier<S3AsyncClient> s3, S3FileIOProperties s3FileIOProperties) {
    this.s3 = s3;
    this.s3FileIOProperties = s3FileIOProperties;
    this.createStack = Thread.currentThread().getStackTrace();
  }

  public S3AsyncClient client() {
    if (client == null) {
      synchronized (this) {
        if (client == null) {
          client = s3.get();
        }
      }
    }
    return client;
  }

  @Override
  public String getCredential() {
    return credential;
  }

  @Override
  public InputFile newInputFile(String path) {
    return S3AsyncInputFile.fromLocation(path, client(), s3FileIOProperties, metrics);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return S3AsyncOutputFile.fromLocation(path, client(), s3FileIOProperties, metrics);
  }

  @Override
  public void deleteFile(String path) {}

  @Override
  public void deleteFiles(Iterable<String> pathsToDelete) throws BulkDeletionFailureException {}

  @Override
  public Iterable<FileInfo> listPrefix(String prefix) {
    return null;
  }

  @Override
  public void deletePrefix(String prefix) {}
}
