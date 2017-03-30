val scala211Version = "2.11.8"
val scala210Version = "2.10.5"

val CommonSettings = Seq(
  organization := "com.theseventhsense",
  version := "0.1.13-SNAPSHOT",
  isSnapshot := version.value.contains("SNAPSHOT"),
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
  .settings(libraryDependencies ++= Dependencies.ScalaTest.value)
  .settings(libraryDependencies ++= Dependencies.Spark.value)
  .dependsOn(core.jvm, spark, mapdb, circe.jvm, akka)
  .aggregate(akka, core.jvm, core.js, circe.jvm, spark, mapdb)

lazy val core = crossProject
  .crossType(CrossType.Pure)
  .in(file("./core"))
  .settings(name := "utils-collections")
  .settings(CommonSettings)

lazy val coreJVM = core.jvm

lazy val coreJS = core.js

lazy val circe = crossProject
  .crossType(CrossType.Pure)
  .in(file("./circe"))
  .dependsOn(core)
  .settings(name := "utils-collections-circe")
  .settings(CommonSettings)
  .settings(libraryDependencies ++= Dependencies.Circe.value)
  .settings(libraryDependencies ++= Dependencies.ScalaTest.value)

lazy val spark = project
  .in(file("./spark"))
  .dependsOn(core.jvm)
  .settings(name := "utils-collections-spark")
  .settings(CommonSettings)
  .settings(libraryDependencies ++= Dependencies.Spark.value)

lazy val mapdb = project
  .in(file("./mapdb"))
  .dependsOn(core.jvm)
  .settings(name := "utils-collections-mapdb")
  .settings(CommonSettings)
  .dependsOn(circe.jvm % "provided")
  .settings(libraryDependencies ++= Dependencies.MapDB.value)
  .settings(libraryDependencies ++= Dependencies.ScalaTest.value)



lazy val circeJVM = circe.jvm

lazy val circeJS = circe.js

lazy val akka = project
  .in(file("./akka"))
  .dependsOn(core.jvm)
  .settings(name := "utils-collections-akka")
  .settings(CommonSettings)
  .dependsOn(circe.jvm % "provided")
  .settings(libraryDependencies ++= Dependencies.Akka.value)
  .settings(libraryDependencies ++= Dependencies.ScalaTest.value)

initialCommands in (Test, console) := """ammonite.repl.Main.run("")"""
