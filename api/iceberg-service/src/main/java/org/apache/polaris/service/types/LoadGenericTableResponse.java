package org.apache.polaris.service.types;

import java.util.Objects;
import java.util.ArrayList;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.polaris.service.types.GenericTable;
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


public class LoadGenericTableResponse implements RESTResponse {

  private GenericTable table;
    /**
     **/
    @ApiModelProperty(required = true, value = "")
    @JsonProperty(value = "table", required = true)
    public GenericTable getTable() {
        return table;
    }

    public LoadGenericTableResponse() {
      // Required for Jackson deserialization
    }

    @JsonCreator
    public LoadGenericTableResponse(@JsonProperty(value = "table", required = true) GenericTable table) {
        this.table = table;
    }

    @Override
    public void validate() {}

    public static Builder builder() {
        return new Builder();
    }
    public static Builder builder(GenericTable table) {
        return new Builder(table);
    }


    public static final class Builder {
      private GenericTable table;
      private Builder() {
      }
      private Builder(GenericTable table) {
        this.table = table;
      }

      public Builder setTable(GenericTable table) {
        this.table = table;
        return this;
      }


      public LoadGenericTableResponse build() {
        LoadGenericTableResponse inst = new LoadGenericTableResponse(table);
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
    LoadGenericTableResponse loadGenericTableResponse = (LoadGenericTableResponse) o;
    return Objects.equals(this.table, loadGenericTableResponse.table);
  }

  @Override
  public int hashCode() {
    return Objects.hash(table);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class LoadGenericTableResponse {\n");
    
    sb.append("    table: ").append(toIndentedString(table)).append("\n");
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
