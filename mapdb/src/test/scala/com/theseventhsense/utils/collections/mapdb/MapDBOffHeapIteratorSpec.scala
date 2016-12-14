package com.theseventhsense.utils.collections.mapdb

import com.theseventhsense.utils.collections.mapdb.Implicits._
import MapDBOffHeapIterator.Implicits._
import com.theseventhsense.utils.collections.SizedIterator.Implicits._
import com.theseventhsense.utils.collections.mapdb.MapDBOffHeapIterator.SerializerProvider
import org.scalatest.{MustMatchers, WordSpec}

/**
 * Created by erik on 2/11/16.
 */
class MapDBOffHeapIteratorSpec extends WordSpec with MustMatchers {
  implicit val db = MapDBHelper.db

  case class D(k: String, v: Int) extends Ordered[D] {
    // Required as of Scala 2.11 for reasons unknown - the companion to Ordered
    // should already be in implicit scope
    import scala.math.Ordered.orderingToOrdered

    def compare(that: D): Int = (this.k, this.v) compare (that.k, that.v)
  }

  val data: Set[D] = Set(D("a", 1), D("a", 2), D("b", 4), D("c", 5), D("b", 1))

  def dataGroupBy(x: D): String = {
    x.k
  }

  val referenceGroups: Map[String, Set[D]] = data.groupBy(dataGroupBy)

  "the MapDBOffHeapIterator" should {
    "construct explicitly" in {
      val offHeapGroups = new MapDBOffHeapIterator(Set(D("a", 1)).sizedIterator)
      val groups = offHeapGroups.groupByOffHeap(dataGroupBy)
      groups.toSeq.map { case (k, v) => k -> v.toSeq } must contain theSameElementsAs Map("a" -> Seq(D("a", 1)))
    }
    "generate on-heap groups that match TraversableLike.groupBy" in {
      val onHeapGroups = data.sizedIterator.groupByOnHeap(dataGroupBy)
      onHeapGroups.toSeq.map { case (k, v) => k -> v.toSet }.toMap mustEqual referenceGroups
    }
    "generate off-heap groups that match TraversableLike.groupBy" in {
      val offHeapGroups = data.sizedIterator.groupByOffHeap(dataGroupBy)
      // force conversion to immutable with sets and compare
      val newGroups = offHeapGroups.toSeq.map { case (k, v) => k -> v.toSet }.toMap
      newGroups mustEqual referenceGroups
      newGroups.size mustEqual referenceGroups.size
    }
  }

}
