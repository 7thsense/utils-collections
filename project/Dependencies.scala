//import org.scalajs.sbtplugin.ScalaJSPlugin.autoImport._
import sbt._

object Dependencies {
  object Versions {
    val Cats = "0.8.0"
    val MapDB = "1.0.9"
    val ScalaTest = "3.0.0"
    val Spark = "2.0.1"
  }

  val Cats = Def.setting(Seq(
    "org.typelevel" %% "cats" % Versions.Cats
  ))

  val MapDB = Def.setting(Seq(
    "org.mapdb" % "mapdb" % Versions.MapDB
  ))

  val ScalaTest = Def.setting(Seq(
    "org.scalatest" %% "scalatest" % Versions.ScalaTest % "test"
  ))

  val Spark = Def.setting(Seq(
    "org.apache.spark" %% "spark-core" % Versions.Spark % "provided",
    "org.apache.spark" %% "spark-sql" % Versions.Spark % "provided"
  ))
}
