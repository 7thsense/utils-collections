package com.theseventhsense.utils.collections

import scala.util.Random

/**
 * Created by erik on 3/31/16.
 */
class SizedIterator[T](
    iterator: Iterator[T],
    val name: String,
    originalSize: Int,
    onEmpty: () => Unit
) extends Iterator[T] {

  override def size: Int = originalSize

  override def hasNext: Boolean = {
    val result = iterator.hasNext
    if (!result) onEmpty
    result
  }

  override def next(): T = iterator.next

  def withOnEmpty(callback: () => Unit): SizedIterator[T] = {
    new SizedIterator(iterator, name, originalSize, callback)
  }
}

object SizedIterator {
  private def randomName = Random.alphanumeric.take(10).mkString
  def apply[T](iterable: Iterable[T]): SizedIterator[T] = {
    fromIterableWithName(iterable, randomName)
  }

  def fromIterableWithName[T](iterable: Iterable[T], name: String): SizedIterator[T] =
    new SizedIterator(iterable.iterator, name, iterable.size, () => ())

  def fromIterator[T](iterator: Iterator[T], size: Int): SizedIterator[T] =
    new SizedIterator(iterator, randomName, size, () => ())

  trait LowPriorityImplicits {
    implicit class RichIterable[T](iterable: Iterable[T]) {
      def sizedIterator: SizedIterator[T] = SizedIterator(iterable)
    }
  }

  trait Ops extends LowPriorityImplicits {
  }

  object Implicits extends Ops
}
