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

import org.apache.iceberg.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.iceberg.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.polaris.spark.rest.LoadGenericTableRESTResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestDeserialization {
  private ObjectMapper mapper;

  @BeforeEach
  public void setUp() {
    mapper = new ObjectMapper();
  }

  @Test
  public void testJsonFormat() throws JsonProcessingException {
    String json =
        "{\"table\":{\"name\":\"delta_table_country_test\",\"format\":\"delta\",\"doc\":null,\"properties\":{\"provider\":\"delta\",\"location\":\"file:/Users/yzou/Documents/delta-test/delta_table_country_local\",\"external\":\"true\",\"owner\":\"\"}}}";

    try {
      LoadGenericTableRESTResponse response =
          mapper.readValue(json, LoadGenericTableRESTResponse.class);
      System.out.println(response.getTable().getFormat());
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
  }
}
