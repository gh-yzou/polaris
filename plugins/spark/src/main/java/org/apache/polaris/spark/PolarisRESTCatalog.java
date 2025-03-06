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
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.Configurable;
import org.apache.iceberg.rest.*;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.polaris.core.PolarisEndpoints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PolarisRESTCatalog implements Configurable<Object> {
  private static final Logger LOG = LoggerFactory.getLogger(PolarisRESTCatalog.class);
  private static final List<String> TOKEN_PREFERENCE_ORDER =
      ImmutableList.of(
          OAuth2Properties.ID_TOKEN_TYPE,
          OAuth2Properties.ACCESS_TOKEN_TYPE,
          OAuth2Properties.JWT_TOKEN_TYPE,
          OAuth2Properties.SAML2_TOKEN_TYPE,
          OAuth2Properties.SAML1_TOKEN_TYPE);

  private RESTClient restClient = null;
  private Set<Endpoint> endpoints;
  private ResourcePaths paths = null;
  private Object conf = null;

  private static final Set<Endpoint> DEFAULT_ENDPOINTS =
      ImmutableSet.<Endpoint>builder()
          .add(PolarisEndpoints.V1_CREATE_GENERIC_TABLE)
          .add(PolarisEndpoints.V1_LOAD_GENERIC_TABLE)
          .add(Endpoint.V1_LIST_TABLES)
          .add(Endpoint.V1_DELETE_TABLE)
          .build();

  public void initialize(RESTClient client, Map<String, String> properties) {
    this.restClient = client;
    LOG.warn("Initializing Polaris REST Catalog with properties: {}", properties);
    this.paths = ResourcePaths.forCatalogProperties(properties);
    this.endpoints = DEFAULT_ENDPOINTS;
  }

  @Override
  public void setConf(Object newConf) {
    this.conf = newConf;
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

  public List<TableIdentifier> listTables(Namespace ns) {
    LOG.warn("Polaris REST Catalog listTables");
    if (!endpoints.contains(Endpoint.V1_LIST_TABLES)) {
      return ImmutableList.of();
    }

    checkNamespaceIsValid(ns);
    Map<String, String> queryParams = Maps.newHashMap();
    ImmutableList.Builder<TableIdentifier> tables = ImmutableList.builder();
    String pageToken = "";

    do {
      LOG.warn("Polaris REST Catalog listTables REST CALL to path {}", paths.tables(ns));
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
    // Endpoint.check(endpoints, PolarisEndpoints.V1_CREATE_GENERIC_TABLE);

    throw new NotImplementedException("createTable not implemented");
    // return delegate.createTable(ident, schema, spec, props);
  }

  public Table loadTable(TableIdentifier identifier) {
    throw new NotImplementedException("loadTable not implemented");
  }
}
