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
package org.apache.polaris.core.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTUtil;

import java.util.Map;

public class GenericTableEntity extends TableLikeEntity {
  public static final String GENERIC_TABLE_FORMAT_SOURCE_KEY = "format";
  public static final String TABLE_PROPERTIES_KEY = "table-properties";

  public GenericTableEntity(PolarisBaseEntity sourceEntity) {
    super(sourceEntity);
  }

  public static GenericTableEntity of(PolarisBaseEntity sourceEntity) {
    if (sourceEntity != null) {
      return new GenericTableEntity(sourceEntity);
    }
    return null;
  }

  @JsonIgnore
  public String getFormat() {
    return getPropertiesAsMap().get(GENERIC_TABLE_FORMAT_SOURCE_KEY);
  }

  @JsonIgnore
  public Map<String, String> getTableProperties() {
    try {
      // Create an ObjectMapper instance
      ObjectMapper objectMapper = new ObjectMapper();
      String propertiesJsonString = getPropertiesAsMap().get(TABLE_PROPERTIES_KEY);

      Map<String, String> propertiesMap = objectMapper.readValue(propertiesJsonString, Map.class);
      return propertiesMap;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static class Builder extends PolarisEntity.BaseBuilder<GenericTableEntity, GenericTableEntity.Builder> {
    public Builder(TableIdentifier identifier, String format, Map<String, String> tableProperties) {
      super();
      setTableIdentifier(identifier);
      setFormat(format);
      setTableProperties(tableProperties);
      setType(PolarisEntityType.TABLE_LIKE);
      setSubType(PolarisEntitySubType.GENETIC_TABLE);
    }

    public Builder(GenericTableEntity original) {
      super(original);
    }

    @Override
    public GenericTableEntity build()  {
      return new GenericTableEntity(buildBase());
    }

    public GenericTableEntity.Builder setTableIdentifier(TableIdentifier identifier) {
      Namespace namespace = identifier.namespace();
      setParentNamespace(namespace);
      setName(identifier.name());
      return this;
    }

    public GenericTableEntity.Builder setParentNamespace(Namespace namespace) {
      if (namespace != null && !namespace.isEmpty()) {
        internalProperties.put(
            NamespaceEntity.PARENT_NAMESPACE_KEY, RESTUtil.encodeNamespace(namespace));
      }
      return this;
    }

    public GenericTableEntity.Builder setFormat(String format) {
      properties.put(GENERIC_TABLE_FORMAT_SOURCE_KEY, format);
      return this;
    }

    public GenericTableEntity.Builder setTableProperties(Map<String, String> tableProperties) {
      try {
        // Create an ObjectMapper instance
        ObjectMapper objectMapper = new ObjectMapper();
        // Convert the Map to JSON string
        String propertiesJsonString = objectMapper.writeValueAsString(tableProperties);
        properties.put(TABLE_PROPERTIES_KEY, propertiesJsonString);
        return this;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}