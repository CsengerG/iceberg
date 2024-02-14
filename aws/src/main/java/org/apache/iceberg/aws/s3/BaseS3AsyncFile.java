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

import org.apache.iceberg.metrics.MetricsContext;
import software.amazon.awssdk.http.HttpStatusCode;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

public abstract class BaseS3AsyncFile {
  private final S3FileIOProperties s3FileIOProperties;
  private final S3AsyncClient client;
  private final S3URI s3URI;
  private final MetricsContext metrics;

  private HeadObjectResponse metadata;

  public BaseS3AsyncFile(
      S3AsyncClient client,
      S3URI s3URI,
      S3FileIOProperties s3FileIOProperties,
      MetricsContext metrics) {
    this.client = client;
    this.s3URI = s3URI;
    this.s3FileIOProperties = s3FileIOProperties;
    this.metrics = metrics;
  }

  protected S3FileIOProperties s3FileIOProperties() {
    return s3FileIOProperties;
  }

  protected MetricsContext metrics() {
    return metrics;
  }

  public String location() {
    return s3URI.location();
  }

  S3AsyncClient client() {
    return client;
  }

  S3URI uri() {
    return s3URI;
  }

  public boolean exists() {
    try {
      return getObjectMetadata() != null;
    } catch (S3Exception e) {
      if (e.statusCode() == HttpStatusCode.NOT_FOUND) {
        return false;
      } else {
        throw e;
      }
    }
  }

  protected HeadObjectResponse getObjectMetadata() throws S3Exception {
    // TODO: configureEncryption?
    if (metadata == null) {
      metadata = client().headObject(x -> x.bucket(uri().bucket()).key(uri().key()).build()).join();
    }

    return metadata;
  }
}
