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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.rest.*;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.auth.OAuth2Util;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.polaris.core.PolarisEndpoints;
import org.apache.polaris.core.catalog.PolarisGenericTable;
import org.apache.polaris.spark.rest.CreateGenericTableRESTRequest;
import org.apache.polaris.spark.rest.LoadGenericTableRESTResponse;
import org.apache.polaris.spark.utils.RESTClientInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters.*;

class PolarisRESTCatalogReflect implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(PolarisRESTCatalogReflect.class);
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
  private OAuth2Util.AuthSession catalogAuth = null;
  private PolarisResourcePaths paths = null;

  // a lazy thread pool for token refresh
  private volatile ScheduledExecutorService refreshExecutor = null;

  private static final Set<Endpoint> DEFAULT_ENDPOINTS =
      ImmutableSet.<Endpoint>builder()
          .add(PolarisEndpoints.V1_CREATE_GENERIC_TABLE)
          .add(PolarisEndpoints.V1_LOAD_GENERIC_TABLE)
          .add(Endpoint.V1_LIST_TABLES)
          .add(Endpoint.V1_DELETE_TABLE)
          .build();

  public PolarisRESTCatalogReflect(RESTClientInfo clientInfo) {
    this.restClient = clientInfo.getRestClient();
    this.catalogAuth = clientInfo.getCatalogAuth();
    this.endpoints = DEFAULT_ENDPOINTS;
    this.paths = new PolarisResourcePaths(clientInfo.getPrefix());
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
