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

import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
  id("polaris-client")
  // id("com.gradleup.shadow")
  // alias(libs.plugins.jandex)
}

val sparkMajorVersion = "3.5"
val scalaVersion = "2.12"

dependencies {
  implementation(project(":polaris-core")) {
    exclude("org.apache.iceberg", "*")
  }
  // implementation(project(":polaris-api-management-service"))
  // implementation(project(":polaris-api-iceberg-service"))

  // implementation(libs.guava)
  // implementation(platform(libs.iceberg.bom))
  // implementation("org.apache.iceberg:iceberg-api:1.8.0")
  // implementation("org.apache.iceberg:iceberg-core:1.8.0")
  // implementation("org.apache.iceberg:iceberg-spark-3.5_2.12:1.8.0")
  implementation("org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.0")
  // implementation("org.apache.iceberg:iceberg-spark-extensions-3.5_2.12:1.8.0")

  compileOnly("org.apache.spark:spark-sql_${scalaVersion}:${sparkMajorVersion}.2") {
    // exclude log4j dependencies
    exclude("org.apache.logging.log4j", "log4j-slf4j2-impl")
    exclude("org.apache.logging.log4j", "log4j-api")
    exclude("org.apache.logging.log4j", "log4j-1.2-api")
    exclude("org.slf4j", "jul-to-slf4j")
  }
  // implementation("com.fasterxml.jackson.core:jackson-annotations")
  // implementation("com.fasterxml.jackson.core:jackson-core")
  // implementation("com.fasterxml.jackson.core:jackson-databind")
  // spark dependencies
}

sourceSets { main { java.srcDirs("src/main/java") } }

tasks.register<ShadowJar>("createPolarisSparkJar") {
  archiveClassifier = null
  // duplicatesStrategy = DuplicatesStrategy.INCLUDE
  archiveBaseName = "polaris-spark-runtime-${sparkMajorVersion}_${scalaVersion}"

  dependencies {
    exclude("META-INF/**")
  }

  from(sourceSets.main.get().output)
  configurations = listOf(project.configurations.runtimeClasspath.get())

  mergeServiceFiles()

  // exclude("META-INF/*.SF")
  // exclude("META-INF/*.DSA")
  // exclude("META-INF/*.RSA")

  // Optional: Minimize the JAR (remove unused classes from dependencies)
  minimize {
    exclude(dependency("org.apache.iceberg:iceberg-spark-*.*:.*"))
  }
}
