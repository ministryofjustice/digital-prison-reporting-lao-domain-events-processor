plugins {
  id("uk.gov.justice.hmpps.gradle-spring-boot") version "10.3.1"
  kotlin("jvm") version "2.4.0"
  kotlin("plugin.spring") version "2.4.0"
  kotlin("plugin.jpa") version "2.4.0"
}

val awsSdkVersion = "2.45.1"
val testContainersVersion = "1.21.4"

dependencies {
  implementation("uk.gov.justice.service.hmpps:hmpps-kotlin-spring-boot-starter:2.5.0")
  implementation("org.springframework.boot:spring-boot-starter-webclient")
  implementation("org.springdoc:springdoc-openapi-starter-webmvc-ui:3.0.3")
  implementation("uk.gov.justice.service.hmpps:hmpps-sqs-spring-boot-starter:7.3.2")
  implementation("org.springframework.boot:spring-boot-starter-data-jpa")

  implementation("software.amazon.awssdk:redshiftdata:$awsSdkVersion")
  implementation("com.amazon.redshift:redshift-jdbc4-no-awssdk:1.2.45.1069")

  implementation("org.flywaydb:flyway-core")

  testRuntimeOnly("org.flywaydb:flyway-database-postgresql")
  testImplementation("org.awaitility:awaitility-kotlin:4.3.0")
  testImplementation("uk.gov.justice.service.hmpps:hmpps-kotlin-spring-boot-starter-test:2.5.0")
  testImplementation("com.h2database:h2")
  testImplementation("javax.xml.bind:jaxb-api:2.4.0-b180830.0359")
  testImplementation("io.jsonwebtoken:jjwt:0.13.0")
  testImplementation("com.marcinziolo:kotlin-wiremock:2.1.1")
  testImplementation("org.testcontainers:postgresql:$testContainersVersion")
  testImplementation("org.testcontainers:junit-jupiter:$testContainersVersion")
  testImplementation("org.postgresql:postgresql:42.7.11")
  testImplementation("org.testcontainers:localstack:1.21.4")
  testImplementation("org.springframework.boot:spring-boot-starter-webflux-test")
  testImplementation("org.wiremock:wiremock-standalone:3.13.2")
  testImplementation("io.swagger.parser.v3:swagger-parser:2.1.44") {
    exclude(group = "io.swagger.core.v3")
  }
  testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.11.0")
}

repositories {
  mavenLocal()
  mavenCentral()
  maven("https://s3.amazonaws.com/redshift-maven-repository/release")
}

java {
  toolchain.languageVersion.set(JavaLanguageVersion.of(25))
}

kotlin {
  jvmToolchain(25)
}

tasks {
  withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
    compilerOptions.jvmTarget = org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_25
  }
}
