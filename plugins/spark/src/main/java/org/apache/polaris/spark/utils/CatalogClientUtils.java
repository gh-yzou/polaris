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

import java.lang.reflect.Field;

import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.rest.*;
import org.apache.iceberg.rest.auth.OAuth2Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CatalogClientUtils {
  private static final Logger LOG = LoggerFactory.getLogger(CatalogClientUtils.class);

  public static RESTClient getRestClient(RESTCatalog icebergRestCatalog) {
    try {
      Field sessionCatalogField = icebergRestCatalog.getClass().getDeclaredField("sessionCatalog");
      sessionCatalogField.setAccessible(true);
      RESTSessionCatalog sessionCatalog =
          (RESTSessionCatalog) sessionCatalogField.get(icebergRestCatalog);

      Field clientField = sessionCatalog.getClass().getDeclaredField("client");
      clientField.setAccessible(true);

      return (RESTClient) clientField.get(sessionCatalog);
    } catch (Exception e) {
      throw new RuntimeException("Failed to get the REST client", e);
    }
  }

  public static OAuth2Util.AuthSession getAuthSession(RESTCatalog icebergRestCatalog) {
    try {
      Field sessionCatalogField = icebergRestCatalog.getClass().getDeclaredField("sessionCatalog");
      sessionCatalogField.setAccessible(true);
      RESTSessionCatalog sessionCatalog =
          (RESTSessionCatalog) sessionCatalogField.get(icebergRestCatalog);

      // log context
      Field contextField = icebergRestCatalog.getClass().getDeclaredField("context");
      contextField.setAccessible(true);
      SessionCatalog.SessionContext context = (SessionCatalog.SessionContext) contextField.get(icebergRestCatalog);
      LOG.warn("session catalog context with credential {}, properties {}", context.credentials(), context.properties());

      Field authField = sessionCatalog.getClass().getDeclaredField("catalogAuth");
      authField.setAccessible(true);
      return (OAuth2Util.AuthSession) authField.get(sessionCatalog);
    } catch (Exception e) {
      throw new RuntimeException("Failed to get the Auth session", e);
    }
  }
}
