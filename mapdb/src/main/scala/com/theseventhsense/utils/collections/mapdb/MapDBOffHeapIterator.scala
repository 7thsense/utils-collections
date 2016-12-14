package com.theseventhsense.utils.collections.mapdb

import com.theseventhsense.utils.collections.GroupingIterator.Implicits._
import com.theseventhsense.utils.collections._
import com.theseventhsense.utils.collections.mapdb.MapDBHelper.DBWrapper
import org.mapdb.{Pump, Serializer}

import scala.collection.JavaConverters._
import scala.concurrent.Future

class MapDBOffHeapIterator[A](s: SizedIterator[A])(implicit orderingA: Ordering[A],
                                                   aProvider: MapDBOffHeapIterator.SerializerProvider[A]) {

  import MapDBOffHeapIterator._

  def groupByConditionally[K](
      keyFunction: A ⇒ K
  )(implicit db: DBWrapper, orderingK: Ordering[K]): Iterator[(K, Iterator[A])] = {
    if (s.size > MinimumOffheapSize) {
      groupByOffHeap(keyFunction)
    } else {
      groupByOnHeap(keyFunction)
    }
  }

  def groupByOnHeap[K](
      predicate: A ⇒ K
  )(implicit orderingK: Ordering[K]): Iterator[(K, Iterator[A])] = {
    s.toSeq.sorted.iterator.groupBy(predicate)
    //s.toSeq.groupBy(predicate).map { case (k, v) => (k, v.iterator) }.iterator
  }

  def groupByOffHeap[K](
      predicate: A ⇒ K
  )(implicit db: DBWrapper, orderingK: Ordering[K]): Iterator[(K, Iterator[A])] = {
    val sorted = Option(s).map { existingK ⇒
      val sortedK = Pump
        .sort[A](
          existingK.asJava,
          false,
          SortBatchSize,
          orderingA,
          aProvider.serializer
        )
        .asScala
      sortedK
    }
    sorted.getOrElse(Iterator.empty).groupBy(predicate)
  }

  def release(implicit db: DBWrapper): Unit = {
    releaseHandle(s.name)
  }

  def releaseConditionally(implicit db: DBWrapper): Unit = {
    releaseHandleConditionally(s.name, s.size)
  }
}

object MapDBOffHeapIterator {

  val SortBatchSize      = 100000
  val MinimumOffheapSize = 100000

//  val SortBatchSize = Try(Play.configuration
//    .getInt("seventhsense.utils.collections.mapdb.sort-batch-size").get)
//    .getOrElse(100000)
//  val MinimumOffheapSize = Try(Play.configuration
//    .getInt("seventhsense.utils.collections.mapdb.minimum-offheap-size").get)
//    .getOrElse(100000)

  def releaseHandle(name: String)(implicit db: DBWrapper): Unit = {
    db.handle.delete(name)
  }

  def releaseHandleConditionally(name: String, size: Int)(implicit db: DBWrapper): Unit = {
    if (size > MinimumOffheapSize) {
      db.handle.delete(name)
    }
  }

  case class OffheapReleasableIterable[A, C <: Iterable[A]](
      name: String,
      item: C
  )(implicit db: DBWrapper)
      extends ReleasableIterable[A, C] {
    def release: Future[Unit] = Future.successful(releaseHandle(name))
  }

  trait SerializerProvider[T] {
    def serializer: Serializer[T]
  }

  class JavaSerializerProvider[A] extends SerializerProvider[A] {
    def serializer: org.mapdb.Serializer[A] = Serializer.JAVA.asInstanceOf[Serializer[A]]
  }

  trait LowPriorityImplicits {
    implicit def serializerProvider[A]: SerializerProvider[A] = new JavaSerializerProvider[A]()
  }

  object Implicits extends LowPriorityImplicits {}

}
