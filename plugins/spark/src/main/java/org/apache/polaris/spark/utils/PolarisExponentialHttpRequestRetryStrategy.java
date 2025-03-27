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
package org.apache.polaris.spark.utils;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.UnknownHostException;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import javax.net.ssl.SSLException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.shaded.org.apache.hc.client5.http.HttpRequestRetryStrategy;
import org.apache.iceberg.shaded.org.apache.hc.client5.http.utils.DateUtils;
import org.apache.iceberg.shaded.org.apache.hc.core5.concurrent.CancellableDependency;
import org.apache.iceberg.shaded.org.apache.hc.core5.http.ConnectionClosedException;
import org.apache.iceberg.shaded.org.apache.hc.core5.http.Header;
import org.apache.iceberg.shaded.org.apache.hc.core5.http.HttpRequest;
import org.apache.iceberg.shaded.org.apache.hc.core5.http.HttpResponse;
import org.apache.iceberg.shaded.org.apache.hc.core5.http.Method;
import org.apache.iceberg.shaded.org.apache.hc.core5.http.protocol.HttpContext;
import org.apache.iceberg.shaded.org.apache.hc.core5.util.TimeValue;

public class PolarisExponentialHttpRequestRetryStrategy implements HttpRequestRetryStrategy {
  private final int maxRetries;
  private final Set<Class<? extends IOException>> nonRetriableExceptions;
  private final Set<Integer> retriableCodes;

  PolarisExponentialHttpRequestRetryStrategy(int maximumRetries) {
    Preconditions.checkArgument(
        maximumRetries > 0, "Cannot set retries to %s, the value must be positive", maximumRetries);
    this.maxRetries = maximumRetries;
    this.retriableCodes = ImmutableSet.of(429, 503, 502, 504);
    this.nonRetriableExceptions =
        ImmutableSet.of(
            InterruptedIOException.class,
            UnknownHostException.class,
            ConnectException.class,
            ConnectionClosedException.class,
            NoRouteToHostException.class,
            SSLException.class);
  }

  @Override
  public boolean retryRequest(
      HttpRequest request, IOException exception, int execCount, HttpContext context) {
    if (execCount > this.maxRetries) {
      return false;
    } else if (this.nonRetriableExceptions.contains(exception.getClass())) {
      return false;
    } else {
      for (Class<? extends IOException> rejectException : this.nonRetriableExceptions) {
        if (rejectException.isInstance(exception)) {
          return false;
        }
      }

      if (request instanceof CancellableDependency
          && ((CancellableDependency) request).isCancelled()) {
        return false;
      } else {
        return Method.isIdempotent(request.getMethod());
      }
    }
  }

  @Override
  public boolean retryRequest(HttpResponse response, int execCount, HttpContext context) {
    return execCount <= this.maxRetries && this.retriableCodes.contains(response.getCode());
  }

  @Override
  public TimeValue getRetryInterval(HttpResponse response, int execCount, HttpContext context) {
    Header header = response.getFirstHeader("Retry-After");
    TimeValue retryAfter = null;
    if (header != null) {
      String value = header.getValue();

      try {
        retryAfter = TimeValue.ofSeconds(Long.parseLong(value));
      } catch (NumberFormatException var9) {
        Instant retryAfterDate = DateUtils.parseStandardDate(value);
        if (retryAfterDate != null) {
          retryAfter =
              TimeValue.ofMilliseconds(retryAfterDate.toEpochMilli() - System.currentTimeMillis());
        }
      }

      if (TimeValue.isPositive(retryAfter)) {
        return retryAfter;
      }
    }

    int delayMillis =
        1000
            * (int)
                Math.min(
                    Math.pow((double) 2.0F, (double) ((long) execCount) - (double) 1.0F),
                    (double) 64.0F);
    int jitter =
        ThreadLocalRandom.current().nextInt(Math.max(1, (int) ((double) delayMillis * 0.1)));
    return TimeValue.ofMilliseconds((long) (delayMillis + jitter));
  }
}
