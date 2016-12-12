package com.theseventhsense.collections

import com.theseventhsense.collections.spark._
import com.theseventhsense.collections.stdlib._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{MustMatchers, WordSpec}

/**
  * Created by erik on 12/8/16.
  */
trait BulkCollectionSpec extends WordSpec with MustMatchers {

  val input  = Seq("b", "a")
  val result = Seq("a", "b")

  def inputCollection: BulkCollection[String]


  "utils-collectinos" should {
    "construct and collect a collection" in {
      inputCollection.collect mustEqual input
    }

    lazy val withKey = inputCollection.mapWithKey(x ⇒ x.hashCode)
    "map keys from a collection" in {
      withKey mustBe a[KVBulkCollection[Int, String]]
    }

    lazy val inputSorted: KVBulkCollection[Int, String] = withKey.sorted
    "sort a kv collection by key and get its value" in {
      inputSorted.values.collect mustEqual result
    }

    lazy val amapped: KVBulkCollection[Int, String] = inputSorted.mapKV { case (k, v) ⇒ (k, "a" + v) }
    "map over a collection's keys and values" in {
      amapped.values.collect mustEqual List("aa", "ab")

    }

    lazy val bmapped: KVBulkCollection[Int, String] = inputSorted.mapValues("b" + _)
    "map over a collection's values" in {
      bmapped.values.collect mustEqual List("ba", "bb")
    }

    "inner join two collections sharing a key" in {
      lazy val innerJoined: KVBulkCollection[Int, (String, String)] = inputSorted.innerJoin(amapped)
      innerJoined.collect mustEqual Seq((97, ("a", "aa")), (98, ("b", "ab")))
    }

    "left join two collections sharing a key" in {
      lazy val leftJoined: KVBulkCollection[Int, (String, Option[String])] = inputSorted.leftOuterJoin(amapped)
      leftJoined.collect mustEqual Seq((97, ("a", Some("aa"))), (98, ("b", Some("ab"))))
    }


    lazy val unioned     = amapped.union(bmapped)
    "union two identical collections" in {
      unioned.collect mustEqual Seq((97, "aa"), (98, "ab"), (97, "ba"), (98, "bb"))
    }

    "fold a collection by key" in {
      lazy val folded      = unioned.foldByKey(0)((acc, item) ⇒ acc + 1, (i1, i2) ⇒ 2)
      folded.values.collect mustEqual Seq(2, 2)

    }
  }
}

class StdLibBulkCollectionSpec extends BulkCollectionSpec {
  override def inputCollection = StdLibBulkCollection(input)

}

class SparkBulkCollectionSpec extends BulkCollectionSpec {
  private lazy val localConf      = new SparkConf().setMaster("local[*]")
  private implicit lazy val spark = SparkSession.builder.config(localConf).getOrCreate()
  override def inputCollection    = SparkBulkCollection(spark.sparkContext.parallelize(input))

}
