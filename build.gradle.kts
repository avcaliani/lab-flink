plugins {
    application
    java
    id("com.gradleup.shadow") version "8.3.6"
}

group = "br.avcaliani"
version = "1.0.0"

val flinkVersion = "2.0.0"
val flinkKafkaVersion = "4.0.0-2.0"
val jacksonVersion = "2.16.2" // Same version used by the Kafka connector.
val slf4jVersion = "1.7.36" // Same version used by Flink.
val lombokVersion = "1.18.36"
val junitVersion = "5.11.4"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

application {
    mainClass.set("br.avcaliani.lard_lad_exchange.App")
}

repositories {
    mavenCentral()
}

// Dependencies already shipped by the Flink cluster ("provided").
// They are available to compile, run and test, but never go into the uber jar.
val flinkProvided: Configuration by configurations.creating
configurations {
    compileOnly { extendsFrom(flinkProvided) }
    testImplementation { extendsFrom(flinkProvided) }
}
sourceSets {
    main { runtimeClasspath += flinkProvided }
}

dependencies {

    // Provided Dependency 👉 flinkProvided
    flinkProvided("org.apache.flink:flink-streaming-java:$flinkVersion")
    flinkProvided("org.apache.flink:flink-clients:$flinkVersion") // Needed to run outside the cluster.

    // CSV Connector
    flinkProvided("org.apache.flink:flink-csv:$flinkVersion")
    flinkProvided("org.apache.flink:flink-connector-files:$flinkVersion")

    // Kafka Connector
    implementation("org.apache.flink:flink-connector-kafka:$flinkKafkaVersion")

    // Used directly by the (de)serializers
    implementation("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:$jacksonVersion")
    flinkProvided("org.slf4j:slf4j-api:$slf4jVersion")
    compileOnly("com.google.code.findbugs:jsr305:3.0.2") // "javax.annotation.Nullable"

    // Lombok
    compileOnly("org.projectlombok:lombok:$lombokVersion")
    annotationProcessor("org.projectlombok:lombok:$lombokVersion")
    testCompileOnly("org.projectlombok:lombok:$lombokVersion")
    testAnnotationProcessor("org.projectlombok:lombok:$lombokVersion")

    // Tests
    testImplementation(platform("org.junit:junit-bom:$junitVersion"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.apache.flink:flink-test-utils:$flinkVersion")
    testImplementation("org.apache.flink:flink-streaming-java:$flinkVersion:tests")
    testImplementation("org.apache.flink:flink-runtime:$flinkVersion:tests")
    testImplementation("org.apache.flink:flink-core:$flinkVersion:tests")
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
}

tasks.test {
    useJUnitPlatform()
}

// Fat jar to be submitted to the cluster: "build/libs/<name>-<version>-uber.jar".
// Flink "provided" dependencies are not in the runtime classpath, so they are left out.
tasks.shadowJar {
    archiveClassifier.set("uber")
    mergeServiceFiles() // Merges duplicated "META-INF/services/*" instead of dropping them.
    dependencies {
        // Kafka client brings it, but the cluster already provides it.
        exclude(dependency("org.slf4j:slf4j-api"))
    }
    manifest {
        attributes["Main-Class"] = application.mainClass.get()
    }
}

tasks.register("uberJar") {
    group = "build"
    description = "Builds the uber jar (alias for shadowJar)."
    dependsOn(tasks.shadowJar)
}
