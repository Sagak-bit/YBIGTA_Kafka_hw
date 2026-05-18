plugins {
    application
    java
}

repositories {
    mavenCentral()
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

dependencies {
    implementation("org.apache.kafka:kafka-clients:3.8.0")
    implementation("org.slf4j:slf4j-simple:2.0.13")
}

application {
    mainClass.set("lab.App")
}

// gradle run --args="produce demo alice 10"  형태로 인자 전달이 가능하도록
tasks.named<JavaExec>("run") {
    standardInput = System.`in`
}
