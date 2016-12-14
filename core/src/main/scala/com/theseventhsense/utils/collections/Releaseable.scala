package com.theseventhsense.utils.collections

import scala.collection.IterableLike
import scala.concurrent.Future

/**
 * Created by erik on 3/31/16.
 */
trait Releasable {
  def release: Future[Unit]
}

trait ReleasableItem[T] extends Releasable {
  def item: T
}

trait ReleasableIterable[A, C <: Iterable[A]] extends ReleasableItem[C]
    with Iterable[A] {
  override def iterator: Iterator[A] = item.iterator
  def releasableIterator: SizedIterator[A] = SizedIterator(this).withOnEmpty(() => this.release)
}

case class NoOpReleasableIterable[A, C <: Iterable[A]](item: C) extends ReleasableIterable[A, C] {
  def release: Future[Unit] = Future.successful(())
}
