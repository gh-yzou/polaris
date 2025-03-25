package org.apache.polaris.service.types;

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
import org.apache.iceberg.rest.RESTRequest;


public class CreateGenericTableRequest implements RESTRequest {

  private final String name;
  private final String format;
  private final Map<String, String> properties;
  private final String doc;

    @Override
    public void validate() {}

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
    
    @JsonCreator
    public CreateGenericTableRequest(@JsonProperty(value = "name", required = true) String name, @JsonProperty(value = "format", required = true) String format, @JsonProperty(value = "properties") Map<String, String> properties, @JsonProperty(value = "doc") String doc) {
        this.name = name;
        this.format = format;
        this.properties = Objects.requireNonNullElse(properties, new HashMap<>());
        this.doc = doc;
    }


    public CreateGenericTableRequest(String name, String format) {
        this.name = name;
        this.format = format;
        this.properties = new HashMap<>();
        this.doc = null;
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


      public CreateGenericTableRequest build() {
        CreateGenericTableRequest inst = new CreateGenericTableRequest(name, format, properties, doc);
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
    CreateGenericTableRequest createGenericTableRequest = (CreateGenericTableRequest) o;
    return Objects.equals(this.name, createGenericTableRequest.name) &&
        Objects.equals(this.format, createGenericTableRequest.format) &&
        Objects.equals(this.properties, createGenericTableRequest.properties) &&
        Objects.equals(this.doc, createGenericTableRequest.doc);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, format, properties, doc);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class CreateGenericTableRequest {\n");
    
    sb.append("    name: ").append(toIndentedString(name)).append("\n");
    sb.append("    format: ").append(toIndentedString(format)).append("\n");
    sb.append("    properties: ").append(toIndentedString(properties)).append("\n");
    sb.append("    doc: ").append(toIndentedString(doc)).append("\n");
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
