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

package org.apache.polaris.core.catalog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class PolarisGenericTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(PolarisGenericTable.class);

  private final String name;
  private final String format;
  private final Map<String, String> properties;
  private final long registerTimeStamp;

  public PolarisGenericTable(String name, String format, Map<String, String> props, long registerTimeStamp) {
    this.name = name;
    this.format = format;
    this.properties = props;
    this.registerTimeStamp = registerTimeStamp;
  }

  public String getName() { return name; }

  public String getFormat() { return format; }

  public Map<String, String> getProperties() { return properties; }

  public long getRegisterTimeStamp() { return registerTimeStamp; }
}