import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import org.gradle.internal.impldep.org.apache.maven.model.Build
import org.jetbrains.kotlin.contracts.model.structure.UNKNOWN_COMPUTATION.type
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import org.jetbrains.kotlin.serialization.js.DynamicTypeDeserializer.id

group = "highload"
version = "1.0-SNAPSHOT"

buildscript {
    repositories {
        mavenCentral()
        maven(url = "https://plugins.gradle.org/m2/")
    }
    dependencies {
        classpath("com.github.jengelman.gradle.plugins:shadow:4.0.3")
    }
}

plugins {
    kotlin("jvm") version "1.3.11"
    id("java")
    id("com.github.johnrengelman.shadow") version "4.0.3"
}

repositories {
    mavenCentral()
    maven("https://jitpack.io")
}

dependencies {
    compile(kotlin("stdlib-jdk8"))
//    compile("com.dslplatform", "dsl-json-java", "1.8.4")
//    kapt("com.dslplatform","dsl-json-java8","1.8.4")

    compile("com.squareup.okhttp", "okhttp", "2.7.5")

    compile("com.wizzardo.tools", "tools", "0.19")

    compile("org.agrona", "agrona", "0.9.29")

    compile("com.fasterxml.jackson.core", "jackson-core", "2.9.8")
    compile("com.fasterxml.jackson.module", "jackson-module-kotlin", "2.9.8")

    compile("org.jetbrains.kotlinx", "kotlinx-coroutines-core", "1.1.0")

    compile("org.rapidoid", "rapidoid-http-server", "5.5.5")
    compile("com.google.inject", "guice", "4.2.2")
    compile("com.google.inject.extensions", "guice-multibindings", "4.2.2")
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}

tasks.withType<ShadowJar> {
    archiveName = "server.jar"
    mergeServiceFiles()
    exclude("META-INF/.SF", "META-INF/.DSA", "META-INF/.RSA")
    manifest {
        attributes["Main-Class"] = "com.shaad.highload2018.StarterKt"
    }
}
