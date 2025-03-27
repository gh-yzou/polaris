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
package org.apache.polaris.spark.utils;

import org.apache.iceberg.rest.RESTSerializers;
import org.apache.iceberg.shaded.com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import org.apache.iceberg.shaded.com.fasterxml.jackson.annotation.PropertyAccessor;
import org.apache.iceberg.shaded.com.fasterxml.jackson.core.JsonFactory;
import org.apache.iceberg.shaded.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.iceberg.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.iceberg.shaded.com.fasterxml.jackson.databind.PropertyNamingStrategies;
import org.apache.iceberg.shaded.com.fasterxml.jackson.databind.cfg.ConstructorDetector;

public class PolarisRESTObjectMapper {
  private static final JsonFactory FACTORY = new JsonFactory();
  private static final ObjectMapper MAPPER;
  private static volatile boolean isInitialized;

  private PolarisRESTObjectMapper() {}

  static ObjectMapper mapper() {
    if (!isInitialized) {
      synchronized (org.apache.polaris.spark.utils.PolarisRESTObjectMapper.class) {
        if (!isInitialized) {
          MAPPER.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
          MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
          MAPPER.setPropertyNamingStrategy(new PropertyNamingStrategies.KebabCaseStrategy());
          RESTSerializers.registerAll(MAPPER);
          isInitialized = true;
        }
      }
    }

    return MAPPER;
  }

  static {
    MAPPER = new ObjectMapper(FACTORY).setConstructorDetector(ConstructorDetector.USE_PROPERTIES_BASED);
    isInitialized = false;
  }
}
