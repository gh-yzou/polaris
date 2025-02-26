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

plugins {
    alias(libs.plugins.jandex)
}

val sparkMajorVersion = "3.5"
val scalaVersion = "2.12"

dependencies {
    implementation(project(":polaris-core"))
    implementation(project(":polaris-api-management-service"))
    implementation(project(":polaris-api-iceberg-service"))
    implementation(project(":polaris-service-common"))

    // implementation("com.fasterxml.jackson.core:jackson-annotations")
    // implementation("com.fasterxml.jackson.core:jackson-core")
    // implementation("com.fasterxml.jackson.core:jackson-databind")
    // spark dependencies
}

sourceSets {
    main {
        java.srcDirs("src/main/java")
    }
}

tasks.register<Jar>("createPolarisSparkJar") {
    archiveBaseName = "polaris-spark-runtime-${sparkMajorVersion}_${scalaVersion}"
    from(sourceSets.main.get().output)
}
