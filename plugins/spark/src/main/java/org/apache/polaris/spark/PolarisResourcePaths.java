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

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.rest.RESTUtil;

import java.util.Map;

public class PolarisResourcePaths {
  private static final Joiner SLASH = Joiner.on("/").skipNulls();
  private static final String PREFIX = "prefix";

  public static final String V1_GENERIC_TABLES = "/v1/{prefix}/namespaces/{namespace}/generic-tables";
  public static final String V1_GENERIC_TABLE = "/v1/{prefix}/namespaces/{namespace}/generic-tables/{table}";

  private final String prefix;

  public static PolarisResourcePaths forCatalogProperties(Map<String, String> properties) {
    return new PolarisResourcePaths((String)properties.get("prefix"));
  }

  public PolarisResourcePaths(String prefix) {
    this.prefix = prefix;
  }

  public String genericTables(Namespace ns) {
    return SLASH.join("v1", this.prefix, new Object[]{"namespaces", RESTUtil.encodeNamespace(ns), "generic-tables"});
  }

  public String genericTable(TableIdentifier ident) {
    return SLASH.join("v1", this.prefix, new Object[]{"namespaces", RESTUtil.encodeNamespace(ident.namespace()), "generic-tables", RESTUtil.encodeString(ident.name())});
  }

  public String tables(Namespace ns) {
    return SLASH.join("v1", this.prefix, new Object[]{"namespaces", RESTUtil.encodeNamespace(ns), "tables"});
  }

  public String table(TableIdentifier ident) {
    return SLASH.join("v1", this.prefix, new Object[]{"namespaces", RESTUtil.encodeNamespace(ident.namespace()), "tables", RESTUtil.encodeString(ident.name())});
  }

}