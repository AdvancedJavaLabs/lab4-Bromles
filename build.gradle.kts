plugins {
    java
}

group = "org.itmo"
version = "0.0.1"

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

    compileOnly("org.projectlombok:lombok:1.18.36")
    annotationProcessor("org.projectlombok:lombok:1.18.36")

    testCompileOnly("org.projectlombok:lombok:1.18.36")
    testAnnotationProcessor("org.projectlombok:lombok:1.18.36")
}

tasks.withType<Test> {
    useJUnitPlatform()
}
