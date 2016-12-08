package com.theseventhsense.collections

import com.theseventhsense.collections._
import com.theseventhsense.collections.stdlib._
import com.theseventhsense.collections.spark._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{MustMatchers, WordSpec}

/**
  * Created by erik on 12/8/16.
  */
class BulkCollectionSpec extends WordSpec with MustMatchers {
  lazy val localConf = new SparkConf().setMaster("local[*]")

  lazy val spark = SparkSession.builder.config(localConf).getOrCreate()

  val input  = Seq("b", "a")
  val result = Seq("a", "b")

  def sortProgram[T](initial: BulkCollection[T]): Seq[T] = {
    val withKey   = initial.mapWithKey(x ⇒ x.hashCode)
    val sorted    = withKey.sorted
    val values    = sorted.values
    val collected = values.collect
    collected
  }

  def complexProgram[T](initial: BulkCollection[T]): Seq[Int] = {
    val withKey                                = initial.mapWithKey(x ⇒ x.hashCode)
    val sorted: KVBulkCollection[Int, T]       = withKey.sorted
    val amapped: KVBulkCollection[Int, String] = sorted.map { case (k, v) ⇒ (k, "a" + v) }
    val bmapped: KVBulkCollection[Int, String] = sorted.map { case (k, v) ⇒ (k, "b" + v) }
//    val joined = sorted.join(amapped)
    val unioned = amapped.union(bmapped)
    val folded  = unioned.foldByKey(0)((acc, item) ⇒ acc + 1)
    folded.values.collect
  }

  "utils-collectinos" when {
    "using stlib collections" should {
      "run the test program against stdlib collections" in {
        sortProgram(StdLibBulkCollection(input)) mustEqual result
      }
      "run a more complex program against stdlib col" in {
        complexProgram(StdLibBulkCollection(input)) mustEqual Seq(2, 2)
      }
    }
    "using spark collections" ignore {
      "run the test program against spark" in {
        val rdd = spark.sparkContext.parallelize(input)
        sortProgram(SparkBulkCollection(rdd)) mustEqual result
      }
    }
  }
}
