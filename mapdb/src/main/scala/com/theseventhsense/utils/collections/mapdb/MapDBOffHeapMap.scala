package com.theseventhsense.utils.collections.mapdb

import java.util

import com.theseventhsense.utils.collections.OffHeapMap
import com.theseventhsense.utils.collections.mapdb.MapDBHelper.DBWrapper
import org.mapdb._

import scala.collection.JavaConverters._
import scala.util.Try

object MapDBOffHeapMap {
  lazy val persistentDb: DBWrapper = MapDBHelper.db

  trait MapMaker[T] {
    def make[V](db: DBWrapper, name: String): java.util.AbstractMap[T, V]
  }

  class GenericMapMaker[T] extends MapMaker[T] {
    def make[V](db: DBWrapper, name: String): java.util.AbstractMap[T, V] = {
      db.handle
        .createHashMap(name)
        .counterEnable()
        .make[T, V]().asInstanceOf[java.util.AbstractMap[T, V]]
    }
  }

  trait MapMakerLowPriority {
    implicit def getGenericMapMaker[T]: GenericMapMaker[T] = new GenericMapMaker[T]
  }

  object MapMaker extends MapMakerLowPriority {
    implicit object StringMapMaker extends MapMaker[String] {
      def make[V](db: DBWrapper, name: String): java.util.AbstractMap[String, V] = {
        db.handle
          .createTreeMap(name)
          .counterEnable()
          .makeStringMap[V]().asInstanceOf[java.util.AbstractMap[String, V]]
      }
    }

    implicit object LongMapMaker extends MapMaker[Long] {
      def make[V](db: DBWrapper, name: String): java.util.AbstractMap[Long, V] = {
        db.handle
          .createTreeMap(name)
          .counterEnable()
          .makeLongMap[V]().asInstanceOf[java.util.AbstractMap[Long, V]]
      }
    }

    implicit object DoubleMapMaker extends MapMaker[Double] {
      def make[V](db: DBWrapper, name: String): java.util.AbstractMap[Double, V] = {
        db.handle
          .createTreeMap(name)
          .comparator(Ordering.Double)
          .counterEnable()
          .make[Double, V]().asInstanceOf[java.util.AbstractMap[Double, V]]
      }
    }
  }
}

class MapDBOffHeapMap[K, V](
  dbParam: Option[DBWrapper] = None,
  valueSerializer: Option[Serializer[V]] = None,
  ordering: Option[Ordering[K]] = None
)(implicit mapMaker: MapDBOffHeapMap.MapMaker[K]) extends OffHeapMap[K, V] {

  val db = dbParam.getOrElse(MapDBHelper.db)

  private val underlying: util.AbstractMap[K, V] = mapMaker.make(db, this.hashCode().toString)

  override def entryIterator: Iterator[(K, V)] = if (!db.handle.isClosed) {
    underlying.entrySet().iterator().asScala.map(entry => (entry.getKey, entry.getValue))
  } else {
    Set.empty.iterator
  }

  override def iterator: Iterator[V] = if (!db.handle.isClosed) {
    underlying.values().iterator().asScala
  } else {
    Iterator.empty
  }

  override def size: Int = if (!db.handle.isClosed) {
    underlying match {
      case a: BTreeMap[K, V] => a.keySet().size()
      case b: HTreeMap[K, V] => b.keySet().size()
      case x => x.keySet.size()
    }
  } else {
    0
  }

  override def contains(key: K): Boolean = Try {
    if (!db.handle.isClosed) {
      underlying.containsKey(key)
    } else {
      false
    }
  }.recover {
    case t: Throwable =>
      //logger.warn(s"Error checking for  $key", t)
      false
  }.getOrElse(false)

  override def set(key: K, value: V): Unit = Try {
    if (!db.handle.isClosed) {
      underlying.put(key, value)
    }
  }.recover {
    case t =>
      //logger.warn(s"Error setting $key -> $value", t)
      ()
  }

  override def get(key: K): Option[V] = if (!db.handle.isClosed) {
    Option(underlying.get(key))
  } else {
    None
  }

  override def remove(key: K): Option[V] = if (!db.handle.isClosed) {
    Option(underlying.remove(key))
  } else {
    None
  }

  override def close(): Unit = Try {
    MapDBHelper.release(db)
  }.recover {
    case t =>
      //logger.warn("Failed closing db", t)
      ()
  }
}

