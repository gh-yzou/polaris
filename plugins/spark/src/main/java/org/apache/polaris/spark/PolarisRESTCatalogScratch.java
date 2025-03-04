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
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.Configurable;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.rest.*;
import org.apache.iceberg.rest.auth.AuthConfig;
import org.apache.iceberg.rest.auth.DefaultAuthSession;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.auth.OAuth2Util;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.apache.iceberg.util.EnvironmentUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.ThreadPools;
import org.apache.polaris.core.PolarisEndpoints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PolarisRESTCatalogScratch implements Configurable<Object>, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(PolarisRESTCatalog.class);
  private static final List<String> TOKEN_PREFERENCE_ORDER =
      ImmutableList.of(
          OAuth2Properties.ID_TOKEN_TYPE,
          OAuth2Properties.ACCESS_TOKEN_TYPE,
          OAuth2Properties.JWT_TOKEN_TYPE,
          OAuth2Properties.SAML2_TOKEN_TYPE,
          OAuth2Properties.SAML1_TOKEN_TYPE);

  private RESTClient restClient = null;
  private final Function<Map<String, String>, RESTClient> clientBuilder;
  private CloseableGroup closeables = null;
  private Set<Endpoint> endpoints;
  private OAuth2Util.AuthSession catalogAuth = null;
  private boolean keepTokenRefreshed = true;
  private ResourcePaths paths = null;
  private Object conf = null;

  // a lazy thread pool for token refresh
  private volatile ScheduledExecutorService refreshExecutor = null;

  private static final Set<Endpoint> DEFAULT_ENDPOINTS =
      ImmutableSet.<Endpoint>builder()
          .add(PolarisEndpoints.V1_CREATE_GENERIC_TABLE)
          .add(Endpoint.V1_LIST_TABLES)
          .add(Endpoint.V1_CREATE_TABLE)
          .add(Endpoint.V1_DELETE_TABLE)
          .add(Endpoint.V1_LOAD_TABLE)
          .build();

  public PolarisRESTCatalogScratch() {
    this(
        SessionCatalog.SessionContext.createEmpty(),
        config -> HTTPClient.builder(config).uri(config.get(CatalogProperties.URI)).build());
  }

  public PolarisRESTCatalogScratch(Function<Map<String, String>, RESTClient> clientBuilder) {
    this(SessionCatalog.SessionContext.createEmpty(), clientBuilder);
  }

  public PolarisRESTCatalogScratch(
      SessionCatalog.SessionContext context,
      Function<Map<String, String>, RESTClient> clientBuilder) {
    this.clientBuilder = clientBuilder;
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
          "Iceberg REST client is missing the OAuth2 server URI configuration and defaults to {}/{}. "
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
      this.endpoints = DEFAULT_ENDPOINTS;
    } else {
      this.endpoints = ImmutableSet.copyOf(config.endpoints());
    }

    this.keepTokenRefreshed =
        PropertyUtil.propertyAsBoolean(
            mergedProps,
            OAuth2Properties.TOKEN_REFRESH_ENABLED,
            OAuth2Properties.TOKEN_REFRESH_ENABLED_DEFAULT);
    this.paths = ResourcePaths.forCatalogProperties(mergedProps);

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

    this.closeables = new CloseableGroup();
    this.closeables.addCloseable(this.restClient);
    this.closeables.setSuppressCloseFailure(true);
  }

  @Override
  public void setConf(Object newConf) {
    this.conf = newConf;
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

  private void shutdownRefreshExecutor() {
    if (refreshExecutor != null) {
      ScheduledExecutorService service = refreshExecutor;
      this.refreshExecutor = null;

      List<Runnable> tasks = service.shutdownNow();
      tasks.forEach(
          task -> {
            if (task instanceof Future) {
              ((Future<?>) task).cancel(true);
            }
          });

      try {
        if (!service.awaitTermination(1, TimeUnit.MINUTES)) {
          LOG.warn("Timed out waiting for refresh executor to terminate");
        }
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while waiting for refresh executor to terminate", e);
        Thread.currentThread().interrupt();
      }
    }
  }

  private void checkNamespaceIsValid(Namespace namespace) {
    if (namespace.isEmpty()) {
      throw new NoSuchNamespaceException("Invalid namespace: %s", namespace);
    }
  }

  private void checkIdentifierIsValid(TableIdentifier tableIdentifier) {
    if (tableIdentifier.namespace().isEmpty()) {
      throw new NoSuchTableException("Invalid table identifier: %s", tableIdentifier);
    }
  }

  @Override
  public void close() throws IOException {
    shutdownRefreshExecutor();

    if (closeables != null) {
      closeables.close();
    }
  }

  public List<TableIdentifier> listTables(Namespace ns) {
    if (!endpoints.contains(Endpoint.V1_LIST_TABLES)) {
      return ImmutableList.of();
    }

    checkNamespaceIsValid(ns);
    Map<String, String> queryParams = Maps.newHashMap();
    ImmutableList.Builder<TableIdentifier> tables = ImmutableList.builder();
    String pageToken = "";

    do {
      queryParams.put("pageToken", pageToken);
      ListTablesResponse response =
          restClient.get(
              paths.tables(ns),
              queryParams,
              ListTablesResponse.class,
              Maps.newHashMap(),
              ErrorHandlers.namespaceErrorHandler());
      pageToken = response.nextPageToken();
      tables.addAll(response.identifiers());
    } while (pageToken != null);

    return tables.build();
  }

  public boolean dropTable(TableIdentifier identifier) {
    Endpoint.check(endpoints, Endpoint.V1_DELETE_TABLE);
    checkIdentifierIsValid(identifier);

    try {
      restClient.delete(
          paths.table(identifier), null, Maps.newHashMap(), ErrorHandlers.tableErrorHandler());
      return true;
    } catch (NoSuchTableException e) {
      return false;
    }
  }

  public Table createTable(TableIdentifier ident, String format, Map<String, String> props) {
    throw new NotImplementedException("createTable not implemented");
    // return delegate.createTable(ident, schema, spec, props);
  }

  public Table loadTable(TableIdentifier identifier) {
    throw new NotImplementedException("loadTable not implemented");
  }
}
