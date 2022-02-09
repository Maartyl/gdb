import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
  kotlin("jvm") version "1.6.10"

  //kotlin("plugin.serialization") version "1.5.31"
}

repositories {
  google()
  mavenCentral()
}

allprojects {
  tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "11"
    kotlinOptions.freeCompilerArgs += arrayOf("-Xopt-in=kotlin.RequiresOptIn")
    kotlinOptions.apiVersion = "1.6"
    kotlinOptions.languageVersion = "1.6"
  }

  if (extensions.findByName("kotlin") != null)
    kotlin.sourceSets.all {
      languageSettings.optIn("kotlin.RequiresOptIn")
    }
}