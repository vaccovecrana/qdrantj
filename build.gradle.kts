plugins { id("io.vacco.oss.gitflow") version "1.9.0" }

group = "io.vacco.qdrantj"
version = "0.2.0"

configure<io.vacco.oss.gitflow.GsPluginProfileExtension> {
  addJ8Spec()
  addClasspathHell()
  sharedLibrary(true, false)
}
