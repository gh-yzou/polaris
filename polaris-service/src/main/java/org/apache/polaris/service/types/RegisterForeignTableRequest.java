package org.apache.polaris.service.types;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModelProperty;
import jakarta.validation.constraints.NotNull;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
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

@jakarta.annotation.Generated(value = "org.openapitools.codegen.languages.JavaResteasyServerCodegen", date = "2024-12-09T15:15:08.147441-08:00[America/Los_Angeles]", comments = "Generator version: 7.6.0")public class RegisterForeignTableRequest {

  @NotNull
  private final String name;
  @NotNull
  private final String format;
  private final Map<String, Object> config;
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
    @JsonProperty(value = "config")
    public Map<String, Object> getConfig() {
        return config;
    }
    
    @JsonCreator
    public RegisterForeignTableRequest(@JsonProperty(value = "name", required = true) String name, @JsonProperty(value = "format", required = true) String format, @JsonProperty(value = "config") Map<String, Object> config) {
        this.name = name;
        this.format = format;
        this.config = Objects.requireNonNullElse(config, new HashMap<>());
    }


    public RegisterForeignTableRequest(String name, String format) {
        this.name = name;
        this.format = format;
        this.config = new HashMap<>();
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
      private Map<String, Object> config;
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
      public Builder setConfig(Map<String, Object> config) {
        this.config = config;
        return this;
      }


      public RegisterForeignTableRequest build() {
        RegisterForeignTableRequest inst = new RegisterForeignTableRequest(name, format, config);
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
    RegisterForeignTableRequest registerForeignTableRequest = (RegisterForeignTableRequest) o;
    return Objects.equals(this.name, registerForeignTableRequest.name) &&
        Objects.equals(this.format, registerForeignTableRequest.format) &&
        Objects.equals(this.config, registerForeignTableRequest.config);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, format, config);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class RegisterForeignTableRequest {\n");
    
    sb.append("    name: ").append(toIndentedString(name)).append("\n");
    sb.append("    format: ").append(toIndentedString(format)).append("\n");
    sb.append("    config: ").append(toIndentedString(config)).append("\n");
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
