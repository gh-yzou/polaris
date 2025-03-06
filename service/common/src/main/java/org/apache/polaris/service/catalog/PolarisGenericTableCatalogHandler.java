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

import org.apache.iceberg.catalog.*;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.catalog.PolarisCatalogHelpers;
import org.apache.polaris.core.catalog.PolarisGenericTable;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.persistence.resolver.ResolverPath;
import org.apache.polaris.service.context.CallContextCatalogFactory;
import org.apache.polaris.service.types.CreateGenericTableRequest;
import org.apache.polaris.service.types.GenericTable;
import org.apache.polaris.service.types.LoadGenericTableResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.ws.rs.core.SecurityContext;
import java.util.Arrays;

public class PolarisGenericTableCatalogHandler extends PolarisBaseHandler implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(PolarisGenericTableCatalogHandler.class);

  /* private final CallContext callContext;
  private final PolarisEntityManager entityManager;
  private final PolarisMetaStoreManager metaStoreManager;
  private final String catalogName;
  private final AuthenticatedPolarisPrincipal authenticatedPrincipal;
  private final SecurityContext securityContext;
  private final PolarisAuthorizer authorizer;
  private final CallContextCatalogFactory catalogFactory;

  // Initialized in the authorize methods.
  private PolarisResolutionManifest resolutionManifest = null; */

  private PolarisGenericTableCatalog genericTableCatalog = null;

  public PolarisGenericTableCatalogHandler(
      CallContext callContext,
      PolarisEntityManager entityManager,
      PolarisMetaStoreManager metaStoreManager,
      SecurityContext securityContext,
      CallContextCatalogFactory catalogFactory,
      String catalogName,
      PolarisAuthorizer authorizer) {
    super(callContext, entityManager, metaStoreManager, securityContext, catalogFactory, catalogName, authorizer);
    /* this.callContext = callContext;
    this.entityManager = entityManager;
    this.metaStoreManager = metaStoreManager;
    this.catalogName = catalogName;
    PolarisDiagnostics diagServices = callContext.getPolarisCallContext().getDiagServices();
    diagServices.checkNotNull(securityContext, "null_security_context");
    diagServices.checkNotNull(securityContext.getUserPrincipal(), "null_user_principal");
    diagServices.check(
        securityContext.getUserPrincipal() instanceof AuthenticatedPolarisPrincipal,
        "invalid_principal_type",
        "Principal must be an AuthenticatedPolarisPrincipal");
    this.securityContext = securityContext;
    this.authenticatedPrincipal =
        (AuthenticatedPolarisPrincipal) securityContext.getUserPrincipal();
    this.authorizer = authorizer;
    this.catalogFactory = catalogFactory; */
  }

  public LoadGenericTableResponse createGenericTable(Namespace namespace, CreateGenericTableRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.CREATE_GENERIC_TABLE_DIRECT;
    TableIdentifier identifier = TableIdentifier.of(namespace, request.getName());
    authorizeCreateTableLikeUnderNamespaceOperationOrThrow(op, identifier);

    CatalogEntity catalog =
        CatalogEntity.of(
            resolutionManifest
                .getResolvedReferenceCatalogEntity()
                .getResolvedLeafEntity()
                .getEntity());
    if (isExternal(catalog)) {
      throw new BadRequestException("Cannot create table on external catalogs.");
    }

    PolarisGenericTable table = genericTableCatalog.createGenericTable(identifier, request.getFormat(), request.getProperties());

    GenericTable genericTable = GenericTable.builder()
        .setName(table.getName())
        .setFormat(table.getFormat())
        .setProperties(table.getProperties())
        .setCatalogRegisterAt(table.getRegisterTimeStamp())
        .build();
    LoadGenericTableResponse response = LoadGenericTableResponse.builder().setTable(genericTable).build();
    return response;
  }

  public LoadGenericTableResponse loadGenericTable(TableIdentifier identifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LOAD_TABLE;
    authorizeBasicTableLikeOperationOrThrow(op, PolarisEntitySubType.GENETIC_TABLE, identifier);

    PolarisGenericTable table = genericTableCatalog.loadGenericTable(identifier);

    GenericTable genericTable = GenericTable.builder()
        .setName(table.getName())
        .setFormat(table.getFormat())
        .setProperties(table.getProperties())
        .setCatalogRegisterAt(table.getRegisterTimeStamp())
        .build();
    return LoadGenericTableResponse.builder().setTable(genericTable).build();
  }

  @Override
  protected void initializeCatalog() {
    // initialize the catalog
    Catalog baseCatalog =
        catalogFactory.createCallContextCatalog(
            callContext, authenticatedPrincipal, securityContext, resolutionManifest);
    SupportsNamespaces namespaceCatalog =
        (baseCatalog instanceof SupportsNamespaces) ? (SupportsNamespaces) baseCatalog : null;

    this.genericTableCatalog = new PolarisGenericTableCatalog(
        entityManager,
        metaStoreManager,
        callContext,
        resolutionManifest,
        namespaceCatalog
    );
  }

  @Override
  public void close() throws Exception {}
}