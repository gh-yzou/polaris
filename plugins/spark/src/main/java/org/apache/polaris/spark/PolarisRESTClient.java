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
package org.apache.polaris.spark;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.relocated.com.google.common.base.Function;
import org.apache.iceberg.rest.*;
import org.apache.iceberg.rest.auth.AuthConfig;
import org.apache.iceberg.rest.auth.DefaultAuthSession;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.auth.OAuth2Util;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.apache.iceberg.util.EnvironmentUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.ThreadPools;
import org.apache.polaris.core.PolarisEndpoints;
import org.apache.polaris.core.catalog.PolarisGenericTable;
import org.apache.polaris.service.types.CreateGenericTableRequest;
import org.apache.polaris.service.types.LoadGenericTableResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.function.Supplier;

class PolarisRESTClient implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(PolarisRESTClient.class);

  static final String VIEW_ENDPOINTS_SUPPORTED = "view-endpoints-supported";
  public static final String REST_PAGE_SIZE = "rest-page-size";

  private static final List<String> TOKEN_PREFERENCE_ORDER =
      ImmutableList.of(
          OAuth2Properties.ID_TOKEN_TYPE,
          OAuth2Properties.ACCESS_TOKEN_TYPE,
          OAuth2Properties.JWT_TOKEN_TYPE,
          OAuth2Properties.SAML2_TOKEN_TYPE,
          OAuth2Properties.SAML1_TOKEN_TYPE);

  private static final Set<Endpoint> DEFAULT_GENERIC_TABLE_ENDPOINTS =
      ImmutableSet.<Endpoint>builder()
          .add(PolarisEndpoints.V1_CREATE_GENERIC_TABLE)
          .add(PolarisEndpoints.V1_LOAD_GENERIC_TABLE)
          .add(Endpoint.V1_LIST_TABLES)
          .add(Endpoint.V1_DELETE_TABLE)
          .build();

  private static final Set<Endpoint> DEFAULT_ICEBERG_ENDPOINTS =
      org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet.<Endpoint>builder()
          .add(Endpoint.V1_LIST_NAMESPACES)
          .add(Endpoint.V1_LOAD_NAMESPACE)
          .add(Endpoint.V1_CREATE_NAMESPACE)
          .add(Endpoint.V1_UPDATE_NAMESPACE)
          .add(Endpoint.V1_DELETE_NAMESPACE)
          .add(Endpoint.V1_LIST_TABLES)
          .add(Endpoint.V1_LOAD_TABLE)
          .add(Endpoint.V1_CREATE_TABLE)
          .add(Endpoint.V1_UPDATE_TABLE)
          .add(Endpoint.V1_DELETE_TABLE)
          .add(Endpoint.V1_RENAME_TABLE)
          .add(Endpoint.V1_REGISTER_TABLE)
          .add(Endpoint.V1_REPORT_METRICS)
          .add(Endpoint.V1_COMMIT_TRANSACTION)
          .build();

  // these view endpoints must not be updated in order to maintain backwards compatibility with
  // legacy servers
  private static final Set<Endpoint> VIEW_ENDPOINTS =
      org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet.<Endpoint>builder()
          .add(Endpoint.V1_LIST_VIEWS)
          .add(Endpoint.V1_LOAD_VIEW)
          .add(Endpoint.V1_CREATE_VIEW)
          .add(Endpoint.V1_UPDATE_VIEW)
          .add(Endpoint.V1_DELETE_VIEW)
          .add(Endpoint.V1_RENAME_VIEW)
          .build();

  private RESTClient restClient = null;
  private final Function<Map<String, String>, RESTClient> clientBuilder;
  private CloseableGroup closeables = null;
  private Set<Endpoint> endpoints = null;
  private final SessionCatalog.SessionContext context;
  private OAuth2Util.AuthSession catalogAuth = null;
  private boolean keepTokenRefreshed = true;
  private Map<String, String> configs = null;

  private volatile ScheduledExecutorService refreshExecutor = null;

  public PolarisRESTClient() {
    this(
        SessionCatalog.SessionContext.createEmpty(),
        config -> HTTPClient.builder(config).uri(config.get(CatalogProperties.URI)).build());
  }

  public PolarisRESTClient(
      SessionCatalog.SessionContext context,
      Function<Map<String, String>, RESTClient> clientBuilder) {
    this.clientBuilder = clientBuilder;
    this.context = context;
  }

  public void initialize(String name, Map<String, String> unresolved) {
    // resolve any configuration that is supplied by environment variables
    // note that this is only done for local config properties and not for properties from the
    // catalog service
    Map<String, String> props = EnvironmentUtil.resolveAll(unresolved);

    long startTimeMillis =
        System.currentTimeMillis(); // keep track of the init start time for token refresh
    String initToken = props.get(OAuth2Properties.TOKEN);
    boolean hasInitToken = initToken != null;

    ConfigResponse config;
    OAuthTokenResponse authResponse;
    String credential = props.get(OAuth2Properties.CREDENTIAL);
    boolean hasCredential = credential != null && !credential.isEmpty();
    String scope = props.getOrDefault(OAuth2Properties.SCOPE, OAuth2Properties.CATALOG_SCOPE);
    Map<String, String> optionalOAuthParams = OAuth2Util.buildOptionalParam(props);
    if (!props.containsKey(OAuth2Properties.OAUTH2_SERVER_URI)
        && (hasInitToken || hasCredential)
        && !PropertyUtil.propertyAsBoolean(props, "rest.sigv4-enabled", false)) {
      LOG.warn(
          "Polaris REST client is missing the OAuth2 server URI configuration and defaults to {}/{}. "
              + "This automatic fallback will be removed in a future Iceberg release."
              + "It is recommended to configure the OAuth2 endpoint using the '{}' property to be prepared. "
              + "This warning will disappear if the OAuth2 endpoint is explicitly configured. "
              + "See https://github.com/apache/iceberg/issues/10537",
          RESTUtil.stripTrailingSlash(props.get(CatalogProperties.URI)),
          ResourcePaths.tokens(),
          OAuth2Properties.OAUTH2_SERVER_URI);
    }

    String oauth2ServerUri =
        props.getOrDefault(OAuth2Properties.OAUTH2_SERVER_URI, ResourcePaths.tokens());
    try (DefaultAuthSession initSession =
             DefaultAuthSession.of(HTTPHeaders.of(OAuth2Util.authHeaders(initToken)));
         RESTClient initClient = clientBuilder.apply(props).withAuthSession(initSession)) {
      Map<String, String> initHeaders = configHeaders(props);
      if (hasCredential) {
        authResponse =
            OAuth2Util.fetchToken(
                initClient, initHeaders, credential, scope, oauth2ServerUri, optionalOAuthParams);
        Map<String, String> authHeaders =
            RESTUtil.merge(initHeaders, OAuth2Util.authHeaders(authResponse.token()));
        config = fetchConfig(initClient, authHeaders, props);
      } else {
        authResponse = null;
        config = fetchConfig(initClient, initHeaders, props);
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close HTTP client", e);
    }

    // build the final configuration and set up the catalog's auth
    Map<String, String> mergedProps = config.merge(props);
    Map<String, String> baseHeaders = configHeaders(mergedProps);

    if (config.endpoints().isEmpty()) {
      this.endpoints =
          PropertyUtil.propertyAsBoolean(mergedProps, VIEW_ENDPOINTS_SUPPORTED, false)
              ? org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet.<Endpoint>builder()
              .addAll(DEFAULT_GENERIC_TABLE_ENDPOINTS)
              .addAll(DEFAULT_ICEBERG_ENDPOINTS)
              .addAll(VIEW_ENDPOINTS)
              .build()
              : org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet.<Endpoint>builder()
              .addAll(DEFAULT_GENERIC_TABLE_ENDPOINTS)
              .addAll(DEFAULT_ICEBERG_ENDPOINTS).build();
    } else {
      this.endpoints = ImmutableSet.copyOf(config.endpoints());
    }

    this.keepTokenRefreshed =
        PropertyUtil.propertyAsBoolean(
            mergedProps,
            OAuth2Properties.TOKEN_REFRESH_ENABLED,
            OAuth2Properties.TOKEN_REFRESH_ENABLED_DEFAULT);

    String token = mergedProps.get(OAuth2Properties.TOKEN);
    this.catalogAuth =
        new OAuth2Util.AuthSession(
            baseHeaders,
            AuthConfig.builder()
                .credential(credential)
                .scope(scope)
                .oauth2ServerUri(oauth2ServerUri)
                .optionalOAuthParams(optionalOAuthParams)
                .build());
    this.restClient = clientBuilder.apply(mergedProps).withAuthSession(catalogAuth);
    if (authResponse != null) {
      this.catalogAuth =
          OAuth2Util.AuthSession.fromTokenResponse(
              restClient, tokenRefreshExecutor(name), authResponse, startTimeMillis, catalogAuth);
      this.restClient = restClient.withAuthSession(catalogAuth);
    } else if (token != null) {
      this.catalogAuth =
          OAuth2Util.AuthSession.fromAccessToken(
              restClient,
              tokenRefreshExecutor(name),
              token,
              expiresAtMillis(mergedProps),
              catalogAuth);
      this.restClient = restClient.withAuthSession(catalogAuth);
    }

    this.configs = mergedProps;

    this.closeables = new CloseableGroup();
    this.closeables.addCloseable(this.restClient);
    this.closeables.setSuppressCloseFailure(true);
  }

  public Map<String, String> getConfigs() {
    return configs;
  }

  public Set<Endpoint> getEndpoints() {
    return endpoints;
  }

  public <T extends RESTResponse> T get(
      String path,
      Map<String, String> queryParams,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler){
    return this.restClient.get(path, queryParams, responseType, headers, errorHandler);
  }

  public <T extends RESTResponse> T post(
      String path,
      RESTRequest body,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    return this.restClient.post(path, body, responseType, headers, errorHandler);
  }

  public <T extends RESTResponse> T delete(
      String path,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    return this.restClient.delete(path, responseType, headers, errorHandler);
  }

  private static Map<String, String> configHeaders(Map<String, String> properties) {
    return RESTUtil.extractPrefixMap(properties, "header.");
  }

  private static ConfigResponse fetchConfig(
      RESTClient client, Map<String, String> headers, Map<String, String> properties) {
    // send the client's warehouse location to the service to keep in sync
    // this is needed for cases where the warehouse is configured client side, but may be used on
    // the server side,
    // like the Hive Metastore, where both client and service hive-site.xml may have a warehouse
    // location.
    ImmutableMap.Builder<String, String> queryParams = ImmutableMap.builder();
    if (properties.containsKey(CatalogProperties.WAREHOUSE_LOCATION)) {
      queryParams.put(
          CatalogProperties.WAREHOUSE_LOCATION,
          properties.get(CatalogProperties.WAREHOUSE_LOCATION));
    }

    ConfigResponse configResponse =
        client.get(
            ResourcePaths.config(),
            queryParams.build(),
            ConfigResponse.class,
            headers,
            ErrorHandlers.defaultErrorHandler());
    configResponse.validate();
    return configResponse;
  }

  private ScheduledExecutorService tokenRefreshExecutor(String catalogName) {
    if (!keepTokenRefreshed) {
      return null;
    }

    if (refreshExecutor == null) {
      synchronized (this) {
        if (refreshExecutor == null) {
          this.refreshExecutor = ThreadPools.newScheduledPool(catalogName + "-token-refresh", 1);
        }
      }
    }

    return refreshExecutor;
  }

  private Long expiresAtMillis(Map<String, String> properties) {
    if (properties.containsKey(OAuth2Properties.TOKEN_EXPIRES_IN_MS)) {
      long expiresInMillis =
          PropertyUtil.propertyAsLong(
              properties,
              OAuth2Properties.TOKEN_EXPIRES_IN_MS,
              OAuth2Properties.TOKEN_EXPIRES_IN_MS_DEFAULT);
      return System.currentTimeMillis() + expiresInMillis;
    } else {
      return null;
    }
  }


  @Override
  public void close() throws IOException {
    if (closeables != null) {
      closeables.close();
    }
  }
}