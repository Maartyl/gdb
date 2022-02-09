plugins {
  kotlin("jvm")

  kotlin("plugin.serialization") version "1.6.0"
}

group = "com.github.maartyl.gdb"
version = "1.0"

repositories {
  mavenCentral()
}

dependencies {
  api(project(":gdbapi"))

  implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.6.0")
  implementation("org.jetbrains.kotlinx:kotlinx-collections-immutable:0.3.4")

  //implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.3.1")
  implementation("org.jetbrains.kotlinx:kotlinx-serialization-protobuf:1.3.2")

  //JVM impl
  api("org.mapdb:mapdb:3.0.8")
  //useful collections; already used inside mapdb == no extra dep
  implementation("com.google.guava:guava:31.0.1-jre")
}