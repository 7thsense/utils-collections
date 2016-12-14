package com.theseventhsense.utils.collections

import scala.collection.mutable.ListBuffer

/**
 * Given a *SORTED* iterator of A, and a keyFunction A => K, GroupingIterator will return a lazy stream
 * of K -> Array[A], similar to the scala stdlib .groupBy function. The benefit of this implementation
 * is that it builds on the assumption of a sorted list and uses that to perform the grouping operation lazily.
 *
 * @param sourceIterator
 * @param predicate
 * @tparam K
 * @tparam A
 */
class StackSafeGroupingIterator[K, A](sourceIterator: Iterator[A], predicate: A => K)(implicit orderingK: Ordering[K])
  extends Iterator[(K, Iterator[A])] {
  private val iter = sourceIterator.buffered

  override def hasNext: Boolean = iter.hasNext

  override def next(): (K, Iterator[A]) = {
    val firstKey: K = predicate(iter.head)
    val group = new ListBuffer[A]
    while(iter.hasNext && orderingK.equiv(predicate(iter.head), firstKey)) {
      group.append(iter.next)
    }
    firstKey -> group.iterator
  }
}


object GroupingIterator {
  object Implicits {
    implicit class GroupableIterator[V](i: Iterator[V]) {
      def groupBy[K](keyFunction: V => K)(implicit orderingK: Ordering[K]): Iterator[(K, Iterator[V])] =
        new StackSafeGroupingIterator(i, keyFunction)
    }
  }
}
