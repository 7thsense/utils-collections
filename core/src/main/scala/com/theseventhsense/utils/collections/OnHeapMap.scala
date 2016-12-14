package com.theseventhsense.utils.collections

import scala.collection.JavaConverters._
import scala.util.Try

/**
 * Created by erik on 2/12/16.
 */
class OnHeapMap[K, V](sorted: Boolean = false) extends OffHeapMap[K, V] {

  private var underlying = new java.util.HashMap[K, V]()

  override def entryIterator: Iterator[(K, V)] = underlying.entrySet()
    .iterator.asScala
    .map(entry => (entry.getKey, entry.getValue))

  override def iterator: Iterator[V] = underlying.values().iterator().asScala

  override def size: Int = underlying.size()

  override def contains(key: K): Boolean = Try {
    underlying.containsKey(key)
  }.recover {
    case t: Throwable =>
      //logger.warn(s"Error checking for  $key", t)
      false
  }.getOrElse(false)

  override def set(key: K, value: V): Unit = Try {
    underlying.put(key, value)
  }.recover {
    case t =>
      //logger.warn(s"Error setting $key -> $value", t)
      ()
  }

  override def get(key: K): Option[V] = Option(underlying.get(key))

  override def remove(key: K): Option[V] = Option(underlying.remove(key))

  override def close(): Unit = {
    underlying = null // scalastyle:ignore
  }
}
