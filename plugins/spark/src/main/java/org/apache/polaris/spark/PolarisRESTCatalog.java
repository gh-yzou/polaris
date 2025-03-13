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
import java.util.Objects;
import java.util.Set;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.Configurable;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.rest.*;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.auth.OAuth2Util;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.apache.iceberg.shaded.com.github.benmanes.caffeine.cache.Cache;
import org.apache.iceberg.util.EnvironmentUtil;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.ThreadPools;
import org.apache.polaris.core.PolarisEndpoints;
import org.apache.polaris.core.catalog.PolarisGenericTable;
import org.apache.polaris.service.types.CreateGenericTableRequest;
import org.apache.polaris.service.types.LoadGenericTableResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PolarisRESTCatalog implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(PolarisRESTCatalog.class);
  private static final List<String> TOKEN_PREFERENCE_ORDER =
      ImmutableList.of(
          OAuth2Properties.ID_TOKEN_TYPE,
          OAuth2Properties.ACCESS_TOKEN_TYPE,
          OAuth2Properties.JWT_TOKEN_TYPE,
          OAuth2Properties.SAML2_TOKEN_TYPE,
          OAuth2Properties.SAML1_TOKEN_TYPE);

  private PolarisRESTClient restClient = null;
  private CloseableGroup closeables = null;
  private PolarisResourcePaths paths = null;

  private static final Set<Endpoint> DEFAULT_ENDPOINTS =
      ImmutableSet.<Endpoint>builder()
          .add(PolarisEndpoints.V1_CREATE_GENERIC_TABLE)
          .add(PolarisEndpoints.V1_LOAD_GENERIC_TABLE)
          .add(Endpoint.V1_LIST_TABLES)
          .add(Endpoint.V1_DELETE_TABLE)
          .build();

  public void initialize(PolarisRESTClient client, Map<String, String> properties) {
    LOG.warn("Initializing Polaris REST Catalog with properties: {}", properties);
    this.restClient = client;
    // this.catalogAuth = auth;
    // initiate a new rest client
    this.paths = PolarisResourcePaths.forCatalogProperties(client.getConfigs());
    this.closeables = new CloseableGroup();
    this.closeables.addCloseable(this.restClient);
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
    if (!this.restClient.getEndpoints().contains(Endpoint.V1_LIST_TABLES)) {
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
    Endpoint.check(this.restClient.getEndpoints(), Endpoint.V1_DELETE_TABLE);
    checkIdentifierIsValid(identifier);

    try {
      restClient.delete(
          paths.table(identifier), null, Maps.newHashMap(), ErrorHandlers.tableErrorHandler());
      return true;
    } catch (NoSuchTableException e) {
      return false;
    }
  }

  public PolarisSparkTable createTable(TableIdentifier ident, String format, Map<String, String> props) {
    LOG.warn("Create Table {} using format {} with properties {}", ident, format, props);
    // Endpoint.check(endpoints, PolarisEndpoints.V1_CREATE_GENERIC_TABLE);
    CreateGenericTableRequest request =
        CreateGenericTableRequest.builder()
            .setName(ident.name())
            .setProperties(props)
            .setFormat(format)
            .build();

    LOG.warn("Create Table REQUEST path {} request {}", paths.genericTables(ident.namespace()), request);
    LoadGenericTableResponse response =
        restClient.post(
            paths.genericTables(ident.namespace()),
            request,
            LoadGenericTableResponse.class,
            Maps.newHashMap(),
            ErrorHandlers.tableErrorHandler());

    PolarisGenericTable genericTable = new PolarisGenericTable(
        response.getTable().getName(),
        response.getTable().getFormat(),
        response.getTable().getProperties(),
        10);

    return new PolarisSparkTable(genericTable);
  }

  public PolarisSparkTable loadTable(TableIdentifier identifier) {
    LOG.warn("load table {}", identifier);
    Endpoint.check(
        this.restClient.getEndpoints(),
        PolarisEndpoints.V1_LOAD_GENERIC_TABLE,
        () ->
            new NoSuchTableException(
                "Unable to load table %s: Server does not support endpoint %s",
                identifier, PolarisEndpoints.V1_LOAD_GENERIC_TABLE));
    checkIdentifierIsValid(identifier);
    LoadGenericTableResponse response = restClient.get(
        paths.genericTable(identifier),
        null,
        LoadGenericTableResponse.class,
        Maps.newHashMap(),
        ErrorHandlers.tableErrorHandler());

    PolarisGenericTable genericTable = new PolarisGenericTable(
        response.getTable().getName(),
        response.getTable().getFormat(),
        response.getTable().getProperties(),
        10);

    return new PolarisSparkTable(genericTable);
  }
}
