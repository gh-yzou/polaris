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

import jakarta.ws.rs.core.SecurityContext;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.catalog.PolarisCatalogHelpers;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class PolarisBaseHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(PolarisBaseHandler.class);

  protected final CallContext callContext;
  protected final PolarisEntityManager entityManager;
  protected final PolarisMetaStoreManager metaStoreManager;
  protected final String catalogName;
  protected final AuthenticatedPolarisPrincipal authenticatedPrincipal;
  protected final SecurityContext securityContext;
  protected final PolarisAuthorizer authorizer;
  protected final CallContextCatalogFactory catalogFactory;

  // Initialized in the authorize methods.
  protected PolarisResolutionManifest resolutionManifest = null;

  protected PolarisBaseHandler(
      CallContext callContext,
      PolarisEntityManager entityManager,
      PolarisMetaStoreManager metaStoreManager,
      SecurityContext securityContext,
      CallContextCatalogFactory catalogFactory,
      String catalogName,
      PolarisAuthorizer authorizer) {
    this.callContext = callContext;
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
    this.catalogFactory = catalogFactory;
  }

  protected void authorizeCreateTableLikeUnderNamespaceOperationOrThrow(
      PolarisAuthorizableOperation op, TableIdentifier identifier) {
    Namespace namespace = identifier.namespace();

    resolutionManifest =
        entityManager.prepareResolutionManifest(callContext, securityContext, catalogName);
    resolutionManifest.addPath(
        new ResolverPath(Arrays.asList(namespace.levels()), PolarisEntityType.NAMESPACE),
        namespace);

    // When creating an entity under a namespace, the authz target is the namespace, but we must
    // also
    // add the actual path that will be created as an "optional" passthrough resolution path to
    // indicate that the underlying catalog is "allowed" to check the creation path for a
    // conflicting
    // entity.
    resolutionManifest.addPassthroughPath(
        new ResolverPath(
            PolarisCatalogHelpers.tableIdentifierToList(identifier),
            PolarisEntityType.TABLE_LIKE,
            true /* optional */),
        identifier);
    resolutionManifest.resolveAll();
    PolarisResolvedPathWrapper target = resolutionManifest.getResolvedPath(namespace, true);
    if (target == null) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }
    authorizer.authorizeOrThrow(
        authenticatedPrincipal,
        resolutionManifest.getAllActivatedCatalogRoleAndPrincipalRoles(),
        op,
        target,
        null /* secondary */);

    initializeCatalog();
  }

  protected void authorizeBasicTableLikeOperationOrThrow(
      PolarisAuthorizableOperation op, PolarisEntitySubType subType, TableIdentifier identifier) {
    resolutionManifest =
        entityManager.prepareResolutionManifest(callContext, securityContext, catalogName);

    // The underlying Catalog is also allowed to fetch "fresh" versions of the target entity.
    resolutionManifest.addPassthroughPath(
        new ResolverPath(
            PolarisCatalogHelpers.tableIdentifierToList(identifier),
            PolarisEntityType.TABLE_LIKE,
            true /* optional */),
        identifier);
    resolutionManifest.resolveAll();
    PolarisResolvedPathWrapper target =
        resolutionManifest.getResolvedPath(identifier, subType, true);
    if (target == null) {
      throw new NoSuchTableException("Table does not exist: %s", identifier);
    }
    authorizer.authorizeOrThrow(
        authenticatedPrincipal,
        resolutionManifest.getAllActivatedCatalogRoleAndPrincipalRoles(),
        op,
        target,
        null /* secondary */);

    initializeCatalog();
  }

  protected static boolean isExternal(CatalogEntity catalog) {
    return org.apache.polaris.core.admin.model.Catalog.TypeEnum.EXTERNAL.equals(
        catalog.getCatalogType());
  }

  protected void initializeCatalog() {}
}