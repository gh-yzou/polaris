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

import java.util.Map;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.AuthManager;
import org.apache.iceberg.rest.auth.AuthManagers;
import org.apache.iceberg.rest.auth.AuthSession;

public class PolarisAuthManager implements AuthManager {
  private AuthManager authManager = null;
  private AuthSession defaultAuthParent = null;

  public PolarisAuthManager(String name, Map<String, String> properties) {
    authManager = AuthManagers.loadAuthManager(name, properties);
  }

  public AuthSession getDefaultAuthParent() {
    return defaultAuthParent;
  }

  @Override
  public AuthSession initSession(RESTClient initClient, Map<String, String> properties) {
    return this.authManager.initSession(initClient, properties);
  }

  @Override
  public AuthSession catalogSession(RESTClient sharedClient, Map<String, String> properties) {
    this.defaultAuthParent = this.authManager.catalogSession(sharedClient, properties);
    return this.defaultAuthParent;
  }

  @Override
  public AuthSession tableSession(
      TableIdentifier table, Map<String, String> properties, AuthSession parent) {
    return this.authManager.tableSession(table, properties, parent);
  }

  @Override
  public AuthSession contextualSession(SessionCatalog.SessionContext context, AuthSession parent) {
    return this.authManager.contextualSession(context, parent);
  }

  public AuthSession contextualSession(SessionCatalog.SessionContext context) {
    return this.authManager.contextualSession(context, this.defaultAuthParent);
  }

  @Override
  public void close() {
    this.authManager.close();
  }
}
