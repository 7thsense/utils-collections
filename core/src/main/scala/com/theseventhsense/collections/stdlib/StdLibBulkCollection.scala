package com.theseventhsense.collections.stdlib

import com.theseventhsense.collections.BulkCollection

import scala.reflect.ClassTag

/**
  * Created by erik on 12/8/16.
  */
case class StdLibBulkCollection[T](underlying: Seq[T]) extends BulkCollection[T] {

  override def collect: Seq[T] = underlying

  override def map[V](op: (T) ⇒ V)(implicit vCt: ClassTag[V]): BulkCollection[V] =
    StdLibBulkCollection(underlying.map(op))

  override def flatMap[U](op: (T) ⇒ TraversableOnce[U])(implicit vCt: ClassTag[U]): BulkCollection[U] =
    StdLibBulkCollection(underlying.flatMap(op))

  def aggregate[U](zero: U)(seqOp: (U, T) => U, combOp: (U, U) => U)(implicit uCt: ClassTag[U]): U =
    underlying.foldLeft(zero)(seqOp)

  override def mapWithKey[K](op: (T) ⇒ K)(implicit kCt: ClassTag[K]) =
    StdLibKVBulkCollection(underlying.map(t ⇒ (op(t), t)))

  override def size: Long = this.underlying.size

  override def filter(op: (T) ⇒ Boolean): BulkCollection[T] = StdLibBulkCollection(this.underlying.filter(op))
}
