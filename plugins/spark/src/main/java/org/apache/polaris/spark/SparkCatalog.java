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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.OAuth2Util;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.polaris.spark.utils.CatalogClientUtils;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.*;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class SparkCatalog implements TableCatalog, SupportsNamespaces {
  private static final Set<String> DEFAULT_NS_KEYS = ImmutableSet.of(TableCatalog.PROP_OWNER);
  private String catalogName = null;
  private Catalog icebergCatalog = null;
  private PolarisRESTCatalogScratch polarisCatalog = null;
  private String[] defaultNamespace = null;
  private org.apache.iceberg.catalog.SupportsNamespaces asNamespaceCatalog = null;
  private org.apache.iceberg.catalog.ViewCatalog asViewCatalog = null;

  private boolean createParquetAsIceberg = false;
  private boolean createAvroAsIceberg = false;
  private boolean createOrcAsIceberg = false;

  protected Catalog buildIcebergCatalog(String name, CaseInsensitiveStringMap options) {
    Configuration conf = SparkUtil.hadoopConfCatalogOverrides(SparkSession.active(), name);
    Map<String, String> optionsMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    optionsMap.putAll(options.asCaseSensitiveMap());
    optionsMap.put(CatalogProperties.APP_ID, SparkSession.active().sparkContext().applicationId());
    optionsMap.put(CatalogProperties.USER, SparkSession.active().sparkContext().sparkUser());
    return CatalogUtil.buildIcebergCatalog(name, optionsMap, conf);
  }

  protected PolarisRESTCatalog buildPolarisCatalog(
      Catalog icebergCatalog, String name, CaseInsensitiveStringMap options) {
    Configuration conf = SparkUtil.hadoopConfCatalogOverrides(SparkSession.active(), name);
    Map<String, String> optionsMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    optionsMap.putAll(options.asCaseSensitiveMap());
    optionsMap.put(CatalogProperties.APP_ID, SparkSession.active().sparkContext().applicationId());
    optionsMap.put(CatalogProperties.USER, SparkSession.active().sparkContext().sparkUser());

    /* PolarisRESTCatalog catalog = new PolarisRESTCatalog();
    catalog.setConf(conf);
    // start hanging, need to investigate
    RESTClient icebergRestClient = CatalogClientUtils.getRestClient((RESTCatalog) icebergCatalog);
    OAuth2Util.AuthSession catalogAuth = CatalogClientUtils.getAuthSession((RESTCatalog) icebergCatalog);
    catalog.initialize(icebergRestClient, catalogAuth, optionsMap); */

    try {
      PolarisRESTClient restClient = new PolarisRESTClient();
      restClient.initialize(name, optionsMap);

      PolarisRESTCatalog catalog = new PolarisRESTCatalog();
      catalog.initialize(restClient, optionsMap);
      return catalog;
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize PolarisRESTClient", e);

    }
  }

  protected PolarisRESTCatalogScratch buildPolarisCatalogScratch(
      String name, CaseInsensitiveStringMap options) {
    Configuration conf = SparkUtil.hadoopConfCatalogOverrides(SparkSession.active(), name);
    Map<String, String> optionsMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    optionsMap.putAll(options.asCaseSensitiveMap());
    optionsMap.put(CatalogProperties.APP_ID, SparkSession.active().sparkContext().applicationId());
    optionsMap.put(CatalogProperties.USER, SparkSession.active().sparkContext().sparkUser());

    PolarisRESTCatalogScratch catalog = new PolarisRESTCatalogScratch();
    catalog.setConf(conf);
    catalog.initialize(name, optionsMap);

    return catalog;
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    this.catalogName = name;
    this.icebergCatalog = buildIcebergCatalog(name, options);
    // this.polarisCatalog = buildPolarisCatalog(this.icebergCatalog, name, options);
    this.polarisCatalog = buildPolarisCatalogScratch(name, options);

    this.asNamespaceCatalog = (org.apache.iceberg.catalog.SupportsNamespaces) this.icebergCatalog;
    this.asViewCatalog = (org.apache.iceberg.catalog.ViewCatalog) this.icebergCatalog;
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    // should check iceberg first
    try {
      return polarisCatalog.loadTable(buildIdentifier(ident));
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(ident);
    }
  }

  @Override
  public Table createTable(
      Identifier ident, StructType schema, Transform[] transforms, Map<String, String> properties)
      throws TableAlreadyExistsException {
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

  @Override
  public String[] defaultNamespace() {
    if (defaultNamespace != null) {
      return defaultNamespace;
    }

    return new String[0];
  }

  @Override
  public String[][] listNamespaces() {
    if (asNamespaceCatalog != null) {
      return asNamespaceCatalog.listNamespaces().stream()
          .map(Namespace::levels)
          .toArray(String[][]::new);
    }

    return new String[0][];
  }

  @Override
  public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
    if (asNamespaceCatalog != null) {
      try {
        return asNamespaceCatalog.listNamespaces(Namespace.of(namespace)).stream()
            .map(Namespace::levels)
            .toArray(String[][]::new);
      } catch (org.apache.iceberg.exceptions.NoSuchNamespaceException e) {
        throw new NoSuchNamespaceException(namespace);
      }
    }

    throw new NoSuchNamespaceException(namespace);
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(String[] namespace)
      throws NoSuchNamespaceException {
    if (asNamespaceCatalog != null) {
      try {
        return asNamespaceCatalog.loadNamespaceMetadata(Namespace.of(namespace));
      } catch (org.apache.iceberg.exceptions.NoSuchNamespaceException e) {
        throw new NoSuchNamespaceException(namespace);
      }
    }

    throw new NoSuchNamespaceException(namespace);
  }

  @Override
  public void createNamespace(String[] namespace, Map<String, String> metadata)
      throws NamespaceAlreadyExistsException {
    if (asNamespaceCatalog != null) {
      try {
        if (asNamespaceCatalog instanceof HadoopCatalog
            && DEFAULT_NS_KEYS.equals(metadata.keySet())) {
          // Hadoop catalog will reject metadata properties, but Spark automatically adds "owner".
          // If only the automatic properties are present, replace metadata with an empty map.
          asNamespaceCatalog.createNamespace(Namespace.of(namespace), ImmutableMap.of());
        } else {
          asNamespaceCatalog.createNamespace(Namespace.of(namespace), metadata);
        }
      } catch (AlreadyExistsException e) {
        throw new NamespaceAlreadyExistsException(namespace);
      }
    } else {
      throw new UnsupportedOperationException(
          "Namespaces are not supported by catalog: " + catalogName);
    }
  }

  @Override
  public void alterNamespace(String[] namespace, NamespaceChange... changes)
      throws NoSuchNamespaceException {
    if (asNamespaceCatalog != null) {
      Map<String, String> updates = Maps.newHashMap();
      Set<String> removals = Sets.newHashSet();
      for (NamespaceChange change : changes) {
        if (change instanceof NamespaceChange.SetProperty) {
          NamespaceChange.SetProperty set = (NamespaceChange.SetProperty) change;
          updates.put(set.property(), set.value());
        } else if (change instanceof NamespaceChange.RemoveProperty) {
          removals.add(((NamespaceChange.RemoveProperty) change).property());
        } else {
          throw new UnsupportedOperationException(
              "Cannot apply unknown namespace change: " + change);
        }
      }

      try {
        if (!updates.isEmpty()) {
          asNamespaceCatalog.setProperties(Namespace.of(namespace), updates);
        }

        if (!removals.isEmpty()) {
          asNamespaceCatalog.removeProperties(Namespace.of(namespace), removals);
        }

      } catch (org.apache.iceberg.exceptions.NoSuchNamespaceException e) {
        throw new NoSuchNamespaceException(namespace);
      }
    } else {
      throw new NoSuchNamespaceException(namespace);
    }
  }

  @Override
  public boolean dropNamespace(String[] namespace, boolean cascade)
      throws NoSuchNamespaceException {
    if (asNamespaceCatalog != null) {
      try {
        return asNamespaceCatalog.dropNamespace(Namespace.of(namespace));
      } catch (org.apache.iceberg.exceptions.NoSuchNamespaceException e) {
        throw new NoSuchNamespaceException(namespace);
      }
    }

    return false;
  }

  private boolean useIceberg(String provider) {
    if (provider == null || "iceberg".equalsIgnoreCase(provider)) {
      return true;
    } else if (createParquetAsIceberg && "parquet".equalsIgnoreCase(provider)) {
      return true;
    } else if (createAvroAsIceberg && "avro".equalsIgnoreCase(provider)) {
      return true;
    } else if (createOrcAsIceberg && "orc".equalsIgnoreCase(provider)) {
      return true;
    }

    return false;
  }

  protected TableIdentifier buildIdentifier(Identifier identifier) {
    return Spark3Util.identifierToTableIdentifier(identifier);
  }
}
