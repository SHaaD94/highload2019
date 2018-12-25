import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

group = "highload"
version = "1.0-SNAPSHOT"

buildscript {
    repositories {
        mavenCentral()
        maven(url = "https://plugins.gradle.org/m2/")
    }
    dependencies {
        classpath("com.github.jengelman.gradle.plugins:shadow:1.2.3")
    }
}

plugins {
    kotlin("jvm") version "1.3.10"
    id("com.github.johnrengelman.shadow") version "1.2.3"
}

repositories {
    mavenCentral()
}

dependencies {
    compile(kotlin("stdlib-jdk8"))
    compile("com.dslplatform:dsl-json:1.8.4")
    compile("org.rapidoid:rapidoid-http-server:jar:5.5.5")
    compile("com.google.inject:guice:jar:4.0")
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}
