pluginManagement {
  repositories {
    google()
    gradlePluginPortal()
  }

}
rootProject.name = "gdb"

//gdb impl : Java kotlinX-seri MapDB
include("gdbjxm")
include("gdbapi")
