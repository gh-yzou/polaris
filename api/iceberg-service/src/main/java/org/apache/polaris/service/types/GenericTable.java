package org.apache.polaris.service.types;

import java.io.Serializable;
import java.util.Objects;
import java.util.ArrayList;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.HashMap;
import java.util.Map;
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
import io.swagger.annotations.*;


public class GenericTable {

  private String name;
  private String format;
  private Map<String, String> properties;
  private String doc;
  private Long catalogRegisterAt;

  public GenericTable() {}

    /**
     **/
    @ApiModelProperty(required = true, value = "")
    @JsonProperty(value = "name", required = true)
    public String getName() {
        return name;
    }
    
    /**
     **/
    @ApiModelProperty(required = true, value = "")
    @JsonProperty(value = "format", required = true)
    public String getFormat() {
        return format;
    }
    
    /**
     **/
    @ApiModelProperty(value = "")
    @JsonProperty(value = "properties")
    public Map<String, String> getProperties() {
        return properties;
    }
    
    /**
     **/
    @ApiModelProperty(value = "")
    @JsonProperty(value = "doc")
    public String getDoc() {
        return doc;
    }
    
    /**
     **/
    @ApiModelProperty(value = "")
    @JsonProperty(value = "catalog_register_at")
    public Long getCatalogRegisterAt() {
        return catalogRegisterAt;
    }
    
    @JsonCreator
    public GenericTable(@JsonProperty(value = "name", required = true) String name, @JsonProperty(value = "format", required = true) String format, @JsonProperty(value = "properties") Map<String, String> properties, @JsonProperty(value = "doc") String doc, @JsonProperty(value = "catalog_register_at") Long catalogRegisterAt) {
        this.name = name;
        this.format = format;
        this.properties = Objects.requireNonNullElse(properties, new HashMap<>());
        this.doc = doc;
        this.catalogRegisterAt = catalogRegisterAt;
    }


    public GenericTable(String name, String format) {
        this.name = name;
        this.format = format;
        this.properties = new HashMap<>();
        this.doc = null;
        this.catalogRegisterAt = null;
    }

    public static Builder builder() {
        return new Builder();
    }
    public static Builder builder(String name, String format) {
        return new Builder(name, format);
    }


    public static final class Builder {
      private String name;
      private String format;
      private Map<String, String> properties;
      private String doc;
      private Long catalogRegisterAt;
      private Builder() {
      }
      private Builder(String name, String format) {
        this.name = name;
        this.format = format;
      }

      public Builder setName(String name) {
        this.name = name;
        return this;
      }
      public Builder setFormat(String format) {
        this.format = format;
        return this;
      }
      public Builder setProperties(Map<String, String> properties) {
        this.properties = properties;
        return this;
      }
      public Builder setDoc(String doc) {
        this.doc = doc;
        return this;
      }
      public Builder setCatalogRegisterAt(Long catalogRegisterAt) {
        this.catalogRegisterAt = catalogRegisterAt;
        return this;
      }


      public GenericTable build() {
        GenericTable inst = new GenericTable(name, format, properties, doc, catalogRegisterAt);
        return inst;
      }
    }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GenericTable genericTable = (GenericTable) o;
    return Objects.equals(this.name, genericTable.name) &&
        Objects.equals(this.format, genericTable.format) &&
        Objects.equals(this.properties, genericTable.properties) &&
        Objects.equals(this.doc, genericTable.doc) &&
        Objects.equals(this.catalogRegisterAt, genericTable.catalogRegisterAt);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, format, properties, doc, catalogRegisterAt);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class GenericTable {\n");
    
    sb.append("    name: ").append(toIndentedString(name)).append("\n");
    sb.append("    format: ").append(toIndentedString(format)).append("\n");
    sb.append("    properties: ").append(toIndentedString(properties)).append("\n");
    sb.append("    doc: ").append(toIndentedString(doc)).append("\n");
    sb.append("    catalogRegisterAt: ").append(toIndentedString(catalogRegisterAt)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces
   * (except the first line).
   */
  private String toIndentedString(Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }
}
