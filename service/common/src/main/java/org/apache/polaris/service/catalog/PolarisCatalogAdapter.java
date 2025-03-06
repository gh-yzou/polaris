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
package org.apache.polaris.service.catalog;

import com.google.common.collect.ImmutableSet;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.rest.Endpoint;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.polaris.core.PolarisConfigurationStore;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.PolarisEndpoints;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.transactional.TransactionalPersistence;
import org.apache.polaris.service.catalog.api.IcebergRestPolarisCatalogApi;
import org.apache.polaris.service.catalog.api.IcebergRestPolarisCatalogApiService;
import org.apache.polaris.service.context.CallContextCatalogFactory;
import org.apache.polaris.service.types.CreateGenericTableRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.Set;

/**
 * Implementation of the {@link IcebergRestPolarisCatalogApiService}.
 */
@RequestScoped
public class PolarisCatalogAdapter implements IcebergRestPolarisCatalogApiService {
  private static final Logger LOGGER = LoggerFactory.getLogger(PolarisCatalogAdapter.class);

  private static final Set<Endpoint> DEFAULT_ENDPOINTS =
      ImmutableSet.<Endpoint>builder()
          .add(PolarisEndpoints.V1_CREATE_GENERIC_TABLE)
          .build();

  private final RealmContext realmContext;
  private final CallContext callContext;
  private final CallContextCatalogFactory catalogFactory;
  private final PolarisEntityManager entityManager;
  private final PolarisMetaStoreManager metaStoreManager;
  private final TransactionalPersistence session;
  private final PolarisConfigurationStore configurationStore;
  private final PolarisDiagnostics diagnostics;
  private final PolarisAuthorizer polarisAuthorizer;
  private final IcebergCatalogPrefixParser prefixParser;

  @Inject
  public PolarisCatalogAdapter(
      RealmContext realmContext,
      CallContext callContext,
      CallContextCatalogFactory catalogFactory,
      PolarisEntityManager entityManager,
      PolarisMetaStoreManager metaStoreManager,
      TransactionalPersistence session,
      PolarisConfigurationStore configurationStore,
      PolarisDiagnostics diagnostics,
      PolarisAuthorizer polarisAuthorizer,
      IcebergCatalogPrefixParser prefixParser) {
    this.realmContext = realmContext;
    this.callContext = callContext;
    this.catalogFactory = catalogFactory;
    this.entityManager = entityManager;
    this.metaStoreManager = metaStoreManager;
    this.session = session;
    this.configurationStore = configurationStore;
    this.diagnostics = diagnostics;
    this.polarisAuthorizer = polarisAuthorizer;
    this.prefixParser = prefixParser;

    // FIXME: This is a hack to set the current context for downstream calls.
    CallContext.setCurrentContext(callContext);
  }

  private PolarisGenericTableCatalogHandler newHandlerWrapper(
      RealmContext realmContext, SecurityContext securityContext, String catalogName) {
    AuthenticatedPolarisPrincipal authenticatedPrincipal =
        (AuthenticatedPolarisPrincipal) securityContext.getUserPrincipal();
    if (authenticatedPrincipal == null) {
      throw new NotAuthorizedException("Failed to find authenticatedPrincipal in SecurityContext");
    }

    return new PolarisGenericTableCatalogHandler(
        callContext,
        entityManager,
        metaStoreManager,
        securityContext,
        catalogFactory,
        catalogName,
        polarisAuthorizer);
  }

  private static Namespace decodeNamespace(String namespace) {
    return RESTUtil.decodeNamespace(URLEncoder.encode(namespace, Charset.defaultCharset()));
  }

  @Override
  public Response createGenericTable(
      String prefix,
      String namespace,
      CreateGenericTableRequest createGenericTableRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    Namespace ns = decodeNamespace(namespace);
    LOGGER.info("createGenericTable with name {} and properties {}", createGenericTableRequest.getName(), createGenericTableRequest.getProperties());

    return Response.ok(
            newHandlerWrapper(realmContext, securityContext, prefix)
                .createGenericTable(ns, createGenericTableRequest))
        .build();
  }

  @Override
  public Response loadGenericTable(
      String prefix,
      String namespace,
      String table,
      RealmContext realmContext,
      SecurityContext securityContext) {
    LOGGER.info("loadGenericTable with namespace {}, name {}", namespace, table);
    Namespace ns = decodeNamespace(namespace);
    TableIdentifier tableIdentifier = TableIdentifier.of(ns, RESTUtil.decodeString(table));
    return Response.ok(
            newHandlerWrapper(realmContext, securityContext, prefix).loadGenericTable(tableIdentifier))
        .build();
  }
}