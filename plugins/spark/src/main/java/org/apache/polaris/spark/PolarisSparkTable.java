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

import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.polaris.core.catalog.PolarisGenericTable;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

public class PolarisSparkTable implements org.apache.spark.sql.connector.catalog.Table {
  private static final Logger LOG = LoggerFactory.getLogger(PolarisSparkTable.class);

  private final Set<TableCapability> capabilities =
      ImmutableSet.of(
          TableCapability.BATCH_READ,
          TableCapability.BATCH_WRITE,
          TableCapability.MICRO_BATCH_READ,
          TableCapability.STREAMING_WRITE,
          TableCapability.OVERWRITE_BY_FILTER,
          TableCapability.OVERWRITE_DYNAMIC);

  private final PolarisGenericTable genericTable;

  public PolarisSparkTable(PolarisGenericTable genericTable) {
    LOG.warn("Initialize PolarisSparkTable with table {} format {} properties {}", genericTable.getName(), genericTable.getFormat(), genericTable.getProperties());
    this.genericTable = genericTable;
  }

  @Override
  public String name() {
    return genericTable.getName();
  }

  @Override
  public StructType schema() {
    return null;
  }

  @Override
  public Transform[] partitioning() {
    return null;
  }

  @Override
  public Map<String, String> properties() {
    return genericTable.getProperties();
  }

  @Override
  public Set<TableCapability> capabilities() {
    return capabilities;
  }
}