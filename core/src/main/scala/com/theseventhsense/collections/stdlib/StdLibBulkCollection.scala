package com.theseventhsense.collections.stdlib

import com.theseventhsense.collections.BulkCollection

import scala.reflect.ClassTag

/**
  * Created by erik on 12/8/16.
  */
case class StdLibBulkCollection[T](underlying: Seq[T]) extends BulkCollection[T] {

  override def collect: Seq[T] = underlying

  override def mapWithKey[K](op: (T) ⇒ K)(implicit kCt: ClassTag[K]) =
    StdLibKVBulkCollection(underlying.map(t ⇒ (op(t), t)))
}
