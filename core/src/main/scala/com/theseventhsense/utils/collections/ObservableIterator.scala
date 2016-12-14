package com.theseventhsense.utils.collections

trait Counter {
  def inc(): Unit
}
class ObservableIterator[T](iterator: Iterator[T], counter: Counter) extends Iterator[T] {
  override def hasNext: Boolean = iterator.hasNext

  override def next(): T = {
    counter.inc()
    iterator.next()
  }
}

