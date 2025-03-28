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
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.iceberg.IcebergBuild;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.*;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.iceberg.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.iceberg.shaded.org.apache.hc.client5.http.auth.CredentialsProvider;
import org.apache.iceberg.shaded.org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.iceberg.shaded.org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.iceberg.shaded.org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.iceberg.shaded.org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.iceberg.shaded.org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.iceberg.shaded.org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.iceberg.shaded.org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.iceberg.shaded.org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.iceberg.shaded.org.apache.hc.core5.http.*;
import org.apache.iceberg.shaded.org.apache.hc.core5.http.impl.EnglishReasonPhraseCatalog;
import org.apache.iceberg.shaded.org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.iceberg.shaded.org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.iceberg.shaded.org.apache.hc.core5.io.CloseMode;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PolarisHTTPClient extends BaseHTTPClient {
  private static final Logger LOG = LoggerFactory.getLogger(PolarisHTTPClient.class);
  private static final String SIGV4_ENABLED = "rest.sigv4-enabled";
  private static final String SIGV4_REQUEST_INTERCEPTOR_IMPL =
      "org.apache.iceberg.aws.RESTSigV4Signer";
  @VisibleForTesting static final String CLIENT_VERSION_HEADER = "X-Client-Version";

  @VisibleForTesting
  static final String CLIENT_GIT_COMMIT_SHORT_HEADER = "X-Client-Git-Commit-Short";

  private static final String REST_MAX_RETRIES = "rest.client.max-retries";
  static final String REST_MAX_CONNECTIONS = "rest.client.max-connections";
  static final int REST_MAX_CONNECTIONS_DEFAULT = 100;
  static final String REST_MAX_CONNECTIONS_PER_ROUTE = "rest.client.connections-per-route";
  static final int REST_MAX_CONNECTIONS_PER_ROUTE_DEFAULT = 100;

  @VisibleForTesting
  static final String REST_CONNECTION_TIMEOUT_MS = "rest.client.connection-timeout-ms";

  @VisibleForTesting static final String REST_SOCKET_TIMEOUT_MS = "rest.client.socket-timeout-ms";
  private final URI baseUri;
  private final CloseableHttpClient httpClient;
  private final Map<String, String> baseHeaders;
  private final ObjectMapper mapper;
  private final AuthSession authSession;

  private PolarisHTTPClient(
      URI baseUri,
      HttpHost proxy,
      CredentialsProvider proxyCredsProvider,
      Map<String, String> baseHeaders,
      ObjectMapper objectMapper,
      HttpRequestInterceptor requestInterceptor,
      Map<String, String> properties,
      HttpClientConnectionManager connectionManager,
      AuthSession session) {
    this.baseUri = baseUri;
    this.baseHeaders = baseHeaders;
    this.mapper = objectMapper;
    this.authSession = session;
    HttpClientBuilder clientBuilder = HttpClients.custom();
    clientBuilder.setConnectionManager(connectionManager);
    if (requestInterceptor != null) {
      clientBuilder.addRequestInterceptorLast(requestInterceptor);
    }

    int maxRetries = PropertyUtil.propertyAsInt(properties, "rest.client.max-retries", 5);
    clientBuilder.setRetryStrategy(new PolarisExponentialHttpRequestRetryStrategy(maxRetries));
    if (proxy != null) {
      if (proxyCredsProvider != null) {
        clientBuilder.setDefaultCredentialsProvider(proxyCredsProvider);
      }

      clientBuilder.setProxy(proxy);
    }

    this.httpClient = clientBuilder.build();
  }

  private PolarisHTTPClient(PolarisHTTPClient parent, AuthSession authSession) {
    this.baseUri = parent.baseUri;
    this.httpClient = parent.httpClient;
    this.mapper = parent.mapper;
    this.baseHeaders = parent.baseHeaders;
    this.authSession = authSession;
  }

  @Override
  public PolarisHTTPClient withAuthSession(AuthSession session) {
    Preconditions.checkNotNull(session, "Invalid auth session: null");
    return new PolarisHTTPClient(this, session);
  }

  private static String extractResponseBodyAsString(CloseableHttpResponse response) {
    try {
      return response.getEntity() == null
          ? null
          : EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
    } catch (ParseException | IOException e) {
      throw new RESTException(e, "Failed to convert HTTP response body to string", new Object[0]);
    }
  }

  private static boolean isSuccessful(CloseableHttpResponse response) {
    int code = response.getCode();
    return code == 200 || code == 202 || code == 204;
  }

  private static ErrorResponse buildDefaultErrorResponse(CloseableHttpResponse response) {
    String responseReason = response.getReasonPhrase();
    String message =
        responseReason != null && !responseReason.isEmpty()
            ? responseReason
            : EnglishReasonPhraseCatalog.INSTANCE.getReason(response.getCode(), (Locale) null);
    String type = "RESTException";
    return ErrorResponse.builder()
        .responseCode(response.getCode())
        .withMessage(message)
        .withType(type)
        .build();
  }

  private static void throwFailure(
      CloseableHttpResponse response, String responseBody, Consumer<ErrorResponse> errorHandler) {
    ErrorResponse errorResponse = null;
    if (responseBody != null) {
      try {
        if (errorHandler instanceof ErrorHandler) {
          errorResponse =
              ((ErrorHandler) errorHandler).parseResponse(response.getCode(), responseBody);
        } else {
          LOG.warn(
              "Unknown error handler {}, response body won't be parsed",
              errorHandler.getClass().getName());
          errorResponse =
              ErrorResponse.builder()
                  .responseCode(response.getCode())
                  .withMessage(responseBody)
                  .build();
        }
      } catch (IllegalArgumentException | UncheckedIOException e) {
        LOG.error("Failed to parse an error response. Will create one instead.", e);
      }
    }

    if (errorResponse == null) {
      errorResponse = buildDefaultErrorResponse(response);
    }

    errorHandler.accept(errorResponse);
    throw new RESTException("Unhandled error: %s", new Object[] {errorResponse});
  }

  @Override
  protected HTTPRequest buildRequest(
      HTTPRequest.HTTPMethod method,
      String path,
      Map<String, String> queryParams,
      Map<String, String> headers,
      Object body) {
    ImmutableHTTPRequest.Builder builder =
        ImmutableHTTPRequest.builder()
            .baseUri(this.baseUri)
            .mapper(this.mapper)
            .method(method)
            .path(path)
            .body(body)
            .queryParameters(queryParams == null ? Map.of() : queryParams);
    Map<String, String> allHeaders = Maps.newLinkedHashMap();
    if (headers != null) {
      allHeaders.putAll(headers);
    }

    allHeaders.putIfAbsent("Accept", ContentType.APPLICATION_JSON.getMimeType());
    ContentType mimeType =
        body instanceof Map
            ? ContentType.APPLICATION_FORM_URLENCODED
            : ContentType.APPLICATION_JSON;
    allHeaders.putIfAbsent("Content-Type", mimeType.getMimeType());
    if (baseHeaders != null) {
      baseHeaders.forEach(allHeaders::putIfAbsent);
    }

    Preconditions.checkState(this.authSession != null, "Invalid auth session: null");
    return this.authSession.authenticate(builder.headers(HTTPHeaders.of(allHeaders)).build());
  }

  @Override
  protected <T extends RESTResponse> T execute(
      HTTPRequest req,
      Class<T> responseType,
      Consumer<ErrorResponse> errorHandler,
      Consumer<Map<String, String>> responseHeaders) {
    HttpUriRequestBase request = new HttpUriRequestBase(req.method().name(), req.requestUri());
    req.headers().entries().forEach((ex) -> request.addHeader(ex.name(), ex.value()));
    String encodedBody = req.encodedBody();
    if (encodedBody != null) {
      request.setEntity(new StringEntity(encodedBody));
    }

    try (CloseableHttpResponse response = this.httpClient.execute(request)) {
      Map<String, String> respHeaders = Maps.newHashMap();

      for (Header header : response.getHeaders()) {
        respHeaders.put(header.getName(), header.getValue());
      }

      responseHeaders.accept(respHeaders);
      if (response.getCode() != 204 && (responseType != null || !isSuccessful(response))) {
        String responseBody = extractResponseBodyAsString(response);
        if (!isSuccessful(response)) {
          throwFailure(response, responseBody, errorHandler);
        }

        if (responseBody == null) {
          throw new RESTException(
              "Invalid (null) response body for request (expected %s): method=%s, path=%s, status=%d",
              new Object[] {
                responseType.getSimpleName(), req.method(), req.path(), response.getCode()
              });
        } else {
          try {
            LOG.warn("The responseBody to parse {}", responseBody);
            if (responseType.getSimpleName().endsWith("TableRESTResponse")) {
              LOG.warn("RAW Response parsing");
              ObjectMapper tempMapper = new ObjectMapper();
              return (T) (tempMapper.readValue(responseBody, responseType));
            }
            return (T) (this.mapper.readValue(responseBody, responseType));
          } catch (JsonProcessingException e) {
            throw new RESTException(
                e,
                "Received a success response code of %d, but failed to parse response body into %s",
                new Object[] {response.getCode(), responseType.getSimpleName()});
          }
        }
      } else {
        return null;
      }
    } catch (IOException e) {
      throw new RESTException(
          e, "Error occurred while processing %s request", new Object[] {req.method()});
    }
  }

  @Override
  public void close() throws IOException {
    try {
      if (this.authSession != null) {
        this.authSession.close();
      }
    } finally {
      this.httpClient.close(CloseMode.GRACEFUL);
    }
  }

  @VisibleForTesting
  static HttpRequestInterceptor loadInterceptorDynamically(
      String impl, Map<String, String> properties) {
    DynConstructors.Ctor<HttpRequestInterceptor> ctor;
    try {
      ctor =
          DynConstructors.builder(HttpRequestInterceptor.class)
              .loader(HTTPClient.class.getClassLoader())
              .impl(impl, new Class[0])
              .buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize RequestInterceptor, missing no-arg constructor: %s", impl),
          e);
    }

    HttpRequestInterceptor instance;
    try {
      instance = (HttpRequestInterceptor) ctor.newInstance(new Object[0]);
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format("Cannot initialize, %s does not implement RequestInterceptor", impl), e);
    }

    DynMethods.builder("initialize")
        .hiddenImpl(impl, new Class[] {Map.class})
        .orNoop()
        .build(instance)
        .invoke(new Object[] {properties});
    return instance;
  }

  static HttpClientConnectionManager configureConnectionManager(Map<String, String> properties) {
    PoolingHttpClientConnectionManagerBuilder connectionManagerBuilder =
        PoolingHttpClientConnectionManagerBuilder.create();
    ConnectionConfig connectionConfig = configureConnectionConfig(properties);
    if (connectionConfig != null) {
      connectionManagerBuilder.setDefaultConnectionConfig(connectionConfig);
    }

    return connectionManagerBuilder
        .useSystemProperties()
        .setMaxConnTotal(
            Integer.getInteger(
                "rest.client.max-connections",
                PropertyUtil.propertyAsInt(properties, "rest.client.max-connections", 100)))
        .setMaxConnPerRoute(
            PropertyUtil.propertyAsInt(properties, "rest.client.connections-per-route", 100))
        .build();
  }

  @VisibleForTesting
  static ConnectionConfig configureConnectionConfig(Map<String, String> properties) {
    Long connectionTimeoutMillis =
        PropertyUtil.propertyAsNullableLong(properties, "rest.client.connection-timeout-ms");
    Integer socketTimeoutMillis =
        PropertyUtil.propertyAsNullableInt(properties, "rest.client.socket-timeout-ms");
    if (connectionTimeoutMillis == null && socketTimeoutMillis == null) {
      return null;
    } else {
      ConnectionConfig.Builder connConfigBuilder = ConnectionConfig.custom();
      if (connectionTimeoutMillis != null) {
        connConfigBuilder.setConnectTimeout(connectionTimeoutMillis, TimeUnit.MILLISECONDS);
      }

      if (socketTimeoutMillis != null) {
        connConfigBuilder.setSocketTimeout(socketTimeoutMillis, TimeUnit.MILLISECONDS);
      }

      return connConfigBuilder.build();
    }
  }

  public static PolarisHTTPClient.Builder builder(Map<String, String> properties) {
    return new PolarisHTTPClient.Builder(properties);
  }

  public static class Builder {
    private final Map<String, String> properties;
    private final Map<String, String> baseHeaders = Maps.newHashMap();
    private URI uri;
    private ObjectMapper mapper = PolarisRESTObjectMapper.mapper();
    private HttpHost proxy;
    private CredentialsProvider proxyCredentialsProvider;
    private AuthSession authSession;

    private Builder(Map<String, String> properties) {
      this.properties = properties;
    }

    public PolarisHTTPClient.Builder uri(String baseUri) {
      Preconditions.checkNotNull(baseUri, "Invalid uri for http client: null");

      try {
        this.uri = URI.create(RESTUtil.stripTrailingSlash(baseUri));
        return this;
      } catch (IllegalArgumentException e) {
        throw new RESTException(
            e, "Failed to create request URI from base %s", new Object[] {baseUri});
      }
    }

    public PolarisHTTPClient.Builder uri(URI baseUri) {
      Preconditions.checkNotNull(baseUri, "Invalid uri for http client: null");
      this.uri = baseUri;
      return this;
    }

    public PolarisHTTPClient.Builder withProxy(String hostname, int port) {
      Preconditions.checkNotNull(hostname, "Invalid hostname for http client proxy: null");
      this.proxy = new HttpHost(hostname, port);
      return this;
    }

    public PolarisHTTPClient.Builder withProxyCredentialsProvider(
        CredentialsProvider credentialsProvider) {
      Preconditions.checkNotNull(
          credentialsProvider, "Invalid credentials provider for http client proxy: null");
      this.proxyCredentialsProvider = credentialsProvider;
      return this;
    }

    public PolarisHTTPClient.Builder withHeader(String key, String value) {
      this.baseHeaders.put(key, value);
      return this;
    }

    public PolarisHTTPClient.Builder withHeaders(Map<String, String> headers) {
      this.baseHeaders.putAll(headers);
      return this;
    }

    public PolarisHTTPClient.Builder withObjectMapper(ObjectMapper objectMapper) {
      LOG.warn("update object mapper to custom object mapper");
      this.mapper = objectMapper;
      return this;
    }

    public PolarisHTTPClient.Builder withAuthSession(AuthSession session) {
      this.authSession = session;
      return this;
    }

    public PolarisHTTPClient build() {
      this.withHeader("X-Client-Version", IcebergBuild.fullVersion());
      this.withHeader("X-Client-Git-Commit-Short", IcebergBuild.gitCommitShortId());
      HttpRequestInterceptor interceptor = null;
      if (PropertyUtil.propertyAsBoolean(this.properties, "rest.sigv4-enabled", false)) {
        interceptor =
            PolarisHTTPClient.loadInterceptorDynamically(
                "org.apache.iceberg.aws.RESTSigV4Signer", this.properties);
      }

      if (this.proxyCredentialsProvider != null) {
        Preconditions.checkNotNull(
            this.proxy, "Invalid http client proxy for proxy credentials provider: null");
      }

      return new PolarisHTTPClient(
          this.uri,
          this.proxy,
          this.proxyCredentialsProvider,
          this.baseHeaders,
          this.mapper,
          interceptor,
          this.properties,
          PolarisHTTPClient.configureConnectionManager(this.properties),
          this.authSession);
    }
  }
}
