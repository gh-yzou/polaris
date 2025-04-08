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
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.rest.*;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.util.EnvironmentUtil;
import org.apache.polaris.core.PolarisEndpoints;
import org.apache.polaris.core.catalog.PolarisGenericTable;
import org.apache.polaris.spark.rest.CreateGenericTableRESTRequest;
import org.apache.polaris.spark.rest.LoadGenericTableRESTResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PolarisRESTCatalogMix implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(PolarisRESTCatalogMix.class);
  private static final List<String> TOKEN_PREFERENCE_ORDER =
      ImmutableList.of(
          OAuth2Properties.ID_TOKEN_TYPE,
          OAuth2Properties.ACCESS_TOKEN_TYPE,
          OAuth2Properties.JWT_TOKEN_TYPE,
          OAuth2Properties.SAML2_TOKEN_TYPE,
          OAuth2Properties.SAML1_TOKEN_TYPE);

  private RESTClient restClient = null;
  private CloseableGroup closeables = null;
  private Set<Endpoint> endpoints;
  private AuthSession catalogAuth = null;
  private PolarisResourcePaths paths = null;

  // a lazy thread pool for token refresh
  private static final Set<Endpoint> DEFAULT_ENDPOINTS =
      ImmutableSet.<Endpoint>builder()
          .add(PolarisEndpoints.V1_CREATE_GENERIC_TABLE)
          .add(PolarisEndpoints.V1_LOAD_GENERIC_TABLE)
          .add(Endpoint.V1_LIST_TABLES)
          .add(Endpoint.V1_DELETE_TABLE)
          .build();

  public PolarisRESTCatalogMix(Map<String, String> unresolved, AuthSession catalogAuth) {
    // resolve any configuration that is supplied by environment variables
    // note that this is only done for local config properties and not for properties from the
    // catalog service
    Map<String, String> props = EnvironmentUtil.resolveAll(unresolved);

    this.catalogAuth = catalogAuth;
    this.restClient =
        HTTPClient.builder(props)
            .uri(props.get(CatalogProperties.URI))
            .build()
            .withAuthSession(catalogAuth);

    ConfigResponse config;
    config = fetchConfig(this.restClient, catalogAuth, props);
    Map<String, String> mergedProps = config.merge(props);
    if (config.endpoints().isEmpty()) {
      this.endpoints = DEFAULT_ENDPOINTS;
    } else {
      this.endpoints = ImmutableSet.copyOf(config.endpoints());
    }

    this.paths = PolarisResourcePaths.forCatalogProperties(mergedProps);
    this.restClient =
        HTTPClient.builder(mergedProps)
            .uri(mergedProps.get(CatalogProperties.URI))
            .build()
            .withAuthSession(catalogAuth);

    this.closeables = new CloseableGroup();
    this.closeables.addCloseable(this.restClient);
    this.closeables.setSuppressCloseFailure(true);
  }

  private static ConfigResponse fetchConfig(
      RESTClient client, AuthSession initialAuth, Map<String, String> properties) {
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
        client
            .withAuthSession(initialAuth)
            .get(
                ResourcePaths.config(),
                queryParams.build(),
                ConfigResponse.class,
                RESTUtil.extractPrefixMap(properties, "header."),
                ErrorHandlers.defaultErrorHandler());
    configResponse.validate();
    return configResponse;
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
          restClient
              .withAuthSession(this.catalogAuth)
              .get(
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

  public PolarisSparkTable createTable(
      TableIdentifier ident, String format, Map<String, String> props) {
    LOG.warn("Create Table {} using format {} with properties {}", ident, format, props);
    Endpoint.check(endpoints, PolarisEndpoints.V1_CREATE_GENERIC_TABLE);
    CreateGenericTableRESTRequest request =
        new CreateGenericTableRESTRequest(ident.name(), format, null, props);

    LOG.warn(
        "Create Table REQUEST path {} request {}", paths.genericTables(ident.namespace()), request);
    LoadGenericTableRESTResponse response =
        restClient
            .withAuthSession(this.catalogAuth)
            .post(
                paths.genericTables(ident.namespace()),
                request,
                LoadGenericTableRESTResponse.class,
                Maps.newHashMap(),
                ErrorHandlers.tableErrorHandler());

    PolarisGenericTable genericTable =
        new PolarisGenericTable(
            response.getTable().getName(),
            response.getTable().getFormat(),
            response.getTable().getProperties(),
            10);

    return new PolarisSparkTable(genericTable);
  }

  public PolarisSparkTable loadTable(TableIdentifier identifier) {
    LOG.warn("load table {}", identifier);
    Endpoint.check(
        endpoints,
        PolarisEndpoints.V1_LOAD_GENERIC_TABLE,
        () ->
            new NoSuchTableException(
                "Unable to load table %s: Server does not support endpoint %s",
                identifier, PolarisEndpoints.V1_LOAD_GENERIC_TABLE));
    checkIdentifierIsValid(identifier);
    LoadGenericTableRESTResponse response =
        restClient
            .withAuthSession(this.catalogAuth)
            .get(
                paths.genericTable(identifier),
                null,
                LoadGenericTableRESTResponse.class,
                Maps.newHashMap(),
                ErrorHandlers.tableErrorHandler());

    PolarisGenericTable genericTable =
        new PolarisGenericTable(
            response.getTable().getName(),
            response.getTable().getFormat(),
            response.getTable().getProperties(),
            10);

    return new PolarisSparkTable(genericTable);
  }
}
