package com.theseventhsense.utils.collections

import scala.collection.mutable

trait OffHeapCollection {
  def childSet[T](name: String): mutable.Set[T]
}

trait OffHeapMap[K, V] extends Iterable[V] {
  def iterator: Iterator[V]

  def entryIterator: Iterator[(K, V)]

  def size: Int

  def contains(key: K): Boolean

  def get(key: K): Option[V]

  def set(key: K, value: V): Unit

  def remove(key: K): Option[V]

  def close(): Unit
}
