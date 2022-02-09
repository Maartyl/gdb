plugins {
  kotlin("multiplatform")

  kotlin("plugin.serialization") version "1.6.0"
}

group = "com.github.maartyl.gdb"
version = "1.0"

repositories {
  mavenCentral()
}

kotlin {
  /* Targets configuration omitted.
  *  To find out how to configure the targets, please follow the link:
  *  https://kotlinlang.org/docs/reference/building-mpp-with-gradle.html#setting-up-targets */


  jvm {


  }

  sourceSets {
    val commonMain by getting {
      dependencies {
        implementation(kotlin("stdlib-common"))
        implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.6.0")

        api("org.jetbrains.kotlinx:kotlinx-serialization-core:1.3.1")
      }
    }
    val commonTest by getting {
      dependencies {
        implementation(kotlin("test-common"))
        implementation(kotlin("test-annotations-common"))
      }
    }

    val jvmMain by getting {

    }
  }
}