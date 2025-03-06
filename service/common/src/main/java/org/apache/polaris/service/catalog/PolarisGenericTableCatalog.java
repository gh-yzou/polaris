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

import com.google.common.base.Objects;
import jakarta.ws.rs.core.SecurityContext;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.*;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.catalog.PolarisGenericTable;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.*;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifestCatalogView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

public class PolarisGenericTableCatalog implements GenericTableCatalog, Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(PolarisGenericTableCatalog.class);

  private CloseableGroup closeableGroup;
  private SupportsNamespaces namespaceCatalog = null;
  private final PolarisEntityManager entityManager;
  private final CallContext callContext;
  private final PolarisResolutionManifestCatalogView resolvedEntityView;
  private final CatalogEntity catalogEntity;
  // private final TaskExecutor taskExecutor;
  // private final SecurityContext securityContext;
  private final String catalogName;
  private long catalogId = -1;
  private PolarisMetaStoreManager metaStoreManager;

  PolarisGenericTableCatalog(
      PolarisEntityManager entityManager,
      PolarisMetaStoreManager metaStoreManager,
      CallContext callContext,
      PolarisResolutionManifestCatalogView resolvedEntityView,
      SupportsNamespaces namespaceCatalog) {
    this.namespaceCatalog = namespaceCatalog;
    this.entityManager = entityManager;
    this.callContext = callContext;
    this.resolvedEntityView = resolvedEntityView;
    this.catalogEntity =
        CatalogEntity.of(resolvedEntityView.getResolvedReferenceCatalogEntity().getRawLeafEntity());
    // this.securityContext = securityContext;
    // this.taskExecutor = taskExecutor;
    this.catalogId = catalogEntity.getId();
    this.catalogName = catalogEntity.getName();
    this.metaStoreManager = metaStoreManager;
  }

  @Override
  public PolarisGenericTable createGenericTable(TableIdentifier ident, String format, Map<String, String> props) {
    LOGGER.debug("doCommit for table {} with format {}, properties {}", ident, format, props);
    // TODO: Maybe avoid writing metadata if there's definitely a transaction conflict
    if (!namespaceCatalog.namespaceExists(ident.namespace())) {
      throw new NoSuchNamespaceException("Cannot create table %s. Namespace does not exist: %s", ident, ident.namespace());
    }

    PolarisResolvedPathWrapper resolvedView =
        resolvedEntityView.getPassthroughResolvedPath(ident, PolarisEntitySubType.VIEW);
    PolarisResolvedPathWrapper resolvedTable =
        resolvedEntityView.getPassthroughResolvedPath(ident, PolarisEntitySubType.TABLE);
    if (resolvedView != null) {
      throw new AlreadyExistsException("View with same name already exists: %s", ident);
    }
    if (resolvedTable != null) {
      throw new AlreadyExistsException("Iceberg table with same name already exists: %s", ident);
    }

    PolarisResolvedPathWrapper resolvedEntities =
        resolvedEntityView.getPassthroughResolvedPath(ident, PolarisEntitySubType.GENETIC_TABLE);
    GenericTableEntity entity =
        GenericTableEntity.of(resolvedEntities == null ? null : resolvedEntities.getRawLeafEntity());
    if (null == entity) {
      entity =
          new GenericTableEntity.Builder(ident, format, props)
              .setCatalogId(getCatalogId())
              .setSubType(PolarisEntitySubType.GENETIC_TABLE)
              .setId(
                  getMetaStoreManager().generateNewEntityId(getCurrentPolarisContext()).getId())
              .build();
    }
    createTableLike(ident, entity);

    return new PolarisGenericTable(
        entity.getName(),
        entity.getFormat(),
        entity.getTableProperties(),
        entity.getCreateTimestamp());
  }

  @Override
  public PolarisGenericTable loadGenericTable(TableIdentifier ident) {
    PolarisResolvedPathWrapper resolvedEntities =
        resolvedEntityView.getPassthroughResolvedPath(ident, PolarisEntitySubType.GENETIC_TABLE);
    GenericTableEntity entity =
        GenericTableEntity.of(resolvedEntities == null ? null : resolvedEntities.getRawLeafEntity());
    if (entity == null) {
      throw new NoSuchTableException("Generic Table %s does not exist", ident);
    }

    return new PolarisGenericTable(
        entity.getName(),
        entity.getFormat(),
        entity.getTableProperties(),
        entity.getCreateTimestamp());
  }

  long getCatalogId() {
    // TODO: Properly handle initialization
    if (catalogId <= 0) {
      throw new RuntimeException(
          "Failed to initialize catalogId before using catalog with name: " + catalogName);
    }
    return catalogId;
  }

  private PolarisMetaStoreManager getMetaStoreManager() {
    return metaStoreManager;
  }

  private PolarisCallContext getCurrentPolarisContext() {
    return callContext.getPolarisCallContext();
  }

  private void createTableLike(TableIdentifier identifier, PolarisEntity entity) {
    PolarisResolvedPathWrapper resolvedParent =
        resolvedEntityView.getResolvedPath(identifier.namespace());
    if (resolvedParent == null) {
      // Illegal state because the namespace should've already been in the static resolution set.
      throw new IllegalStateException(
          String.format("Failed to fetch resolved parent for TableIdentifier '%s'", identifier));
    }

    createTableLike(identifier, entity, resolvedParent);
  }

  private void createTableLike(
      TableIdentifier identifier, PolarisEntity entity, PolarisResolvedPathWrapper resolvedParent) {
    List<PolarisEntity> catalogPath = resolvedParent.getRawFullPath();

    if (entity.getParentId() <= 0) {
      // TODO: Validate catalogPath size is at least 1 for catalog entity?
      entity =
          new PolarisEntity.Builder(entity)
              .setParentId(resolvedParent.getRawLeafEntity().getId())
              .build();
    }
    entity =
        new PolarisEntity.Builder(entity).setCreateTimestamp(System.currentTimeMillis()).build();

    EntityResult res =
        getMetaStoreManager()
            .createEntityIfNotExists(
                getCurrentPolarisContext(), PolarisEntity.toCoreList(catalogPath), entity);
    if (!res.isSuccess()) {
      switch (res.getReturnStatus()) {
        case BaseResult.ReturnStatus.CATALOG_PATH_CANNOT_BE_RESOLVED:
          throw new NotFoundException("Parent path does not exist for %s", identifier);

        case BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS:
          throw new AlreadyExistsException("Table or View already exists: %s", identifier);

        default:
          throw new IllegalStateException(
              String.format(
                  "Unknown error status for identifier %s: %s with extraInfo: %s",
                  identifier, res.getReturnStatus(), res.getExtraInformation()));
      }
    }
    PolarisEntity resultEntity = PolarisEntity.of(res);
    LOGGER.debug("Created TableLike entity {} with TableIdentifier {}", resultEntity, identifier);
  }

  @Override
  public void close() throws IOException {
    if (closeableGroup != null) {
      closeableGroup.close();
    }
  }
}