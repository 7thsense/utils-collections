import org.scalajs.sbtplugin.ScalaJSPlugin.autoImport._
import sbt._

object Dependencies {
  object Versions {
    val Akka = "2.4.11"
    val Cats = "0.9.0"
    val Circe = "0.7.0"
    val MapDB = "1.0.9"
    val ScalaTest = "3.0.0"
    val Spark = "2.1.0"
  }

  val Akka = Def.setting(Seq(
    "com.typesafe.akka" %% "akka-actor" % Versions.Akka,
    "com.typesafe.akka" %% "akka-stream" % Versions.Akka,
    "com.typesafe.akka" %% "akka-slf4j" % Versions.Akka
  ))

  val Cats = Def.setting(Seq(
    "org.typelevel" %% "cats" % Versions.Cats
  ))

  val Circe = Def.setting(Seq(
    "io.circe" %%% "circe-core" % Versions.Circe,
    "io.circe" %%% "circe-generic" % Versions.Circe,
    "io.circe" %%% "circe-parser" % Versions.Circe
  ))

  val MapDB = Def.setting(Seq(
    "org.mapdb" % "mapdb" % Versions.MapDB
  ))

  val ScalaTest = Def.setting(Seq(
    "org.scalatest" %%% "scalatest" % Versions.ScalaTest % "test"
  ))

  val Spark = Def.setting(Seq(
    "org.apache.spark" %% "spark-core" % Versions.Spark % "provided",
    "org.apache.spark" %% "spark-sql" % Versions.Spark % "provided"
  ))
}
