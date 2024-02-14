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
import java.io.InputStream;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIOMetricsContext;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.metrics.Counter;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.http.Abortable;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

public class S3AsyncInputStream extends SeekableInputStream implements RangeReadable {

  private static final Logger LOG = LoggerFactory.getLogger(S3AsyncInputStream.class);

  private final S3AsyncClient s3;

  private final S3URI location;

  private final S3FileIOProperties s3FileIOProperties;

  private final MetricsContext metrics;

  // Stream internals
  private boolean closed;
  private InputStream stream;
  private long pos = 0;
  private long next = 0;
  private int skipSize = 0;

  // Telemetry
  private final Counter readBytes;
  private final Counter readOperations;

  public S3AsyncInputStream(
      S3AsyncClient s3,
      S3URI location,
      S3FileIOProperties s3FileIOProperties,
      MetricsContext metrics) {
    this.s3 = s3;
    this.location = location;
    this.s3FileIOProperties = s3FileIOProperties;
    this.metrics = metrics;

    this.readBytes = metrics.counter(FileIOMetricsContext.READ_BYTES, MetricsContext.Unit.BYTES);
    this.readOperations = metrics.counter(FileIOMetricsContext.READ_OPERATIONS);
  }

  @Override
  public long getPos() throws IOException {
    return 0;
  }

  @Override
  public void seek(long newPos) throws IOException {}

  @Override
  public int read() throws IOException {
    Preconditions.checkState(!closed, "Cannot read: already closed");
    positionStream();

    pos += 1;
    next += 1;
    readBytes.increment();
    readOperations.increment();

    return stream.read();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    Preconditions.checkState(!closed, "Cannot read: already closed");
    positionStream();

    int bytesRead = stream.read(b, off, len);
    pos += bytesRead;
    next += bytesRead;
    readBytes.increment(bytesRead);
    readOperations.increment();

    return bytesRead;
  }

  private void positionStream() throws IOException {
    if ((stream != null) && (next == pos)) {
      // already at specified position
      return;
    }

    if ((stream != null) && (next > pos)) {
      // seeking forwards
      long skip = next - pos;
      if (skip <= Math.max(stream.available(), skipSize)) {
        // already buffered or seek is small enough
        LOG.debug("Read-through seek for {} to offset {}", location, next);
        try {
          ByteStreams.skipFully(stream, skip);
          pos = next;
          return;
        } catch (IOException ignored) {
          // will retry by re-opening the stream
          LOG.debug("Ignoring IOException", ignored);
        }
      }
    }

    // close the stream and open at desired position
    LOG.debug("Seek with new stream for {} to offset {}", location, next);
    pos = next;
    openStream();
  }

  private void abortStream() {
    try {
      if (stream instanceof Abortable && stream.read() != -1) {
        ((Abortable) stream).abort();
      }
    } catch (Exception e) {
      LOG.warn("An error occurred while aborting the steam", e);
    }
  }

  private void closeStream() throws IOException {
    if (stream != null) {
      abortStream();
      try {
        stream.close();
      } catch (IOException e) {
        if (!e.getClass().getSimpleName().equals("ConnectionClosedException")) {
          throw e;
        }
      }
      stream = null;
    }
  }

  private void openStream() throws IOException {
    closeStream();

    try {
      stream =
          s3.getObject(
                  x ->
                      x.bucket(location.bucket())
                          .key(location.key())
                          .range(String.format("bytes=%s-", pos))
                          .build(),
                  AsyncResponseTransformer.toBlockingInputStream())
              .join();
    } catch (NoSuchKeyException nke) {
      throw new NotFoundException(nke, "Location does not exist: %s", location);
    }
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {}

  @Override
  public int readTail(byte[] buffer, int offset, int length) throws IOException {
    return 0;
  }
}
