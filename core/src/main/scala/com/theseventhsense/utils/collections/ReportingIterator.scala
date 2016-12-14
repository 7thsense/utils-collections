package com.theseventhsense.utils.collections

/**
 * Created by erik on 3/29/16.
 */
class ReportingIterator[T](iterator: Iterator[T])(callback: (Int) => Unit) extends Iterator[T] {
  private var count = 0
  override def hasNext: Boolean = iterator.hasNext

  override def next(): T = {
    count += 1
    callback(count)
    iterator.next()
  }
}

object ReportingIterator {
  def apply[T](iterator: Iterator[T])(callback: (Int) => Unit): ReportingIterator[T] =
    new ReportingIterator(iterator)(callback)
}
