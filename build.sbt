val scala211Version = "2.11.8"
val scala210Version = "2.10.5"


val CommonSettings = Seq(
  organization := "com.theseventhsense",
//  version := "0.1.1-SNAPSHOT",
  version := "0.1.2",
//  isSnapshot := true,
  publishMavenStyle := true,
  bintrayOrganization := Some("7thsense"),
  licenses += ("MIT", url("http://opensource.org/licenses/MIT")),
  //crossScalaVersions := Seq(scala211Version),
  scalaVersion := scala211Version
)

lazy val root = project
  .in(file("."))
  .settings(CommonSettings)
  .settings(name := "utils-collections-all")
  .settings(libraryDependencies ++=  Dependencies.ScalaTest.value)
  .settings(libraryDependencies ++=  Dependencies.Spark.value)
  .dependsOn(core.jvm, spark)
  .aggregate(core.jvm, core.js, spark)

lazy val core = crossProject.crossType(CrossType.Pure)
  .in(file("./core"))
  .settings(name := "utils-collections")
  .settings(CommonSettings)

lazy val coreJVM = core.jvm

lazy val coreJS = core.js

lazy val spark = project
  .in(file("./spark"))
  .dependsOn(core.jvm)
  .settings(name := "utils-collections-spark")
  .settings(CommonSettings)
  .settings(libraryDependencies ++=  Dependencies.Spark.value)

initialCommands in (Test, console) := """ammonite.repl.Main.run("")"""
