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

import com.google.common.collect.Maps;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.polaris.spark.utils.CatalogClientUtils;
import org.apache.polaris.spark.utils.RESTClientInfo;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.CatalogTableType;
import org.apache.spark.sql.connector.catalog.*;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.*;
import scala.collection.JavaConverters;

import java.net.URI;
import java.util.*;

public class GenericTableSparkCatalog implements TableCatalog {
  private static final Logger LOG = LoggerFactory.getLogger(GenericTableSparkCatalog.class);

  private PolarisRESTCatalogReflect polarisCatalog = null;
  private String catalogName = null;

  public GenericTableSparkCatalog(PolarisRESTCatalogReflect polarisCatalog) {
    this.polarisCatalog = polarisCatalog;
  }

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    this.catalogName = name;
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    LOG.warn("Load table for {}", ident);
    // should check iceberg first
    try {
      PolarisSparkTable genericTable  = polarisCatalog.loadTable(buildIdentifier(ident));

      Map<String, String> properties = genericTable.properties();
      String format  = genericTable.format();
      String location = properties.get(TableCatalog.PROP_LOCATION);
      CatalogStorageFormat storageFormat = new CatalogStorageFormat(
          Option.apply(new URI(location)),
          Option.apply(format),
          Option.apply(format),
          Option.apply(null),
          false,
          JavaConverters.mapAsScalaMapConverter(properties).asScala().toMap(
              Predef.<Tuple2<String, String>>conforms()
          )
      );

      Map<String, String> emptyProperties = Maps.newHashMap();

      List<String> emptyStringList = new ArrayList<>();
      CatalogTable catalogTable = new CatalogTable(
          Spark3Util.toV1TableIdentifier(ident),
          CatalogTableType.MANAGED(),   // should use unity catalog logic to look into properties
          storageFormat,
          new StructType(),
          Option.apply(format),
          JavaConverters.asScalaIteratorConverter(emptyStringList.iterator())
              .asScala()
              .toSeq(),
          Option.apply(null),
          "",
          System.currentTimeMillis(),
          -1,
          "",
          JavaConverters.mapAsScalaMapConverter(properties).asScala().toMap(
              Predef.<Tuple2<String, String>>conforms()
          ),
          Option.apply(null),
          Option.apply(null),
          Option.apply(null),
          JavaConverters.asScalaIteratorConverter(emptyStringList.iterator())
              .asScala()
              .toSeq(),
          false,
          true,
          JavaConverters.mapAsScalaMapConverter(emptyProperties).asScala().toMap(
              Predef.<Tuple2<String, String>>conforms()
          ),
          Option.apply(null)
      );
      return new V1Table(catalogTable);
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(ident);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Table createTable(
      Identifier ident, StructType schema, Transform[] transforms, Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    String provider = properties.get("provider");
    try {
      return polarisCatalog.createTable(buildIdentifier(ident), provider, properties);
    } catch (AlreadyExistsException e) {
      throw new TableAlreadyExistsException(ident);
    }
  }

  @Override
  public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
    throw new NoSuchTableException(ident);
  }

  @Override
  public boolean dropTable(Identifier ident) {
    return false;
  }

  @Override
  public void renameTable(Identifier from, Identifier to)
      throws NoSuchTableException, TableAlreadyExistsException {
    throw new NoSuchTableException(from);
  }

  @Override
  public Identifier[] listTables(String[] namespace) {
    return polarisCatalog.listTables(Namespace.of(namespace)).stream()
        .map(ident -> Identifier.of(ident.namespace().levels(), ident.name()))
        .toArray(Identifier[]::new);
  }

  protected TableIdentifier buildIdentifier(Identifier identifier) {
    return Spark3Util.identifierToTableIdentifier(identifier);
  }
}