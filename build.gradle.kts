plugins {
    java
}

group = "org.itmo"
version = "0.0.1-SNAPSHOT"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(8)
    }
}


repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.hadoop:hadoop-common:3.2.1")
    implementation("org.apache.hadoop:hadoop-client:3.2.1")
}

tasks.withType<Test> {
    useJUnitPlatform()
}
