import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import org.gradle.internal.impldep.org.apache.maven.model.Build
import org.jetbrains.kotlin.contracts.model.structure.UNKNOWN_COMPUTATION.type
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

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
    kotlin("jvm") version "1.3.10"
    id("com.github.johnrengelman.shadow")
}

repositories {
    mavenCentral()
}

dependencies {
    compile(kotlin("stdlib-jdk8"))
    compile("com.dslplatform", "dsl-json", "1.8.4")
    compile("org.rapidoid", "rapidoid-http-server", "5.5.5")
    compile("com.google.inject", "guice", "4.2.2")
    compile("com.google.inject.extensions", "guice-multibindings", "4.2.2")
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}

val shadowJar = tasks.withType<ShadowJar> {
    archiveName = "server.jar"
    mergeServiceFiles()
    exclude("META-INF/.SF", "META-INF/.DSA", "META-INF/.RSA")
    manifest {
        attributes["Main-Class"] = "com.shaad.highload2018.StarterKt"
    }
}


//shadowJar() {
//    archiveName = 'puzzle.jar'
//    mergeServiceFiles()
//    exclude 'META-INF/.SF'
//    exclude 'META-INF/.DSA'
//    exclude 'META-INF/.RSA'
//    manifest {
//        attributes 'Main-Class': 'ru.dgis.world.puzzle.StarterKt'
//    }
//}
//
//build.dependsOn shadowJar
//        task buildDocker(type: Docker) {
//    dependsOn build
//            push = environment != "local"
//    applicationName = "puzzle"
//    tagVersion = commitHash
//    dockerfile = file('deploy/docker/Dockerfile')
//    doFirst {
//        copy {
//            from 'build/libs/puzzle.jar'
//            into stageDir
//        }
//    }
//}
