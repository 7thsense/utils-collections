package com.theseventhsense.utils.collections.mapdb

import com.theseventhsense.utils.collections.SizedIterator
import com.theseventhsense.utils.collections.mapdb.MapDBOffHeapIterator.SerializerProvider

import scala.reflect.ClassTag

/**
 * Created by erik on 2/11/16.
 */
object Implicits {
  implicit class OffHeapIteratorOps[A](s: SizedIterator[A])(implicit
    aClassTag: ClassTag[A],
    orderingA: Ordering[A],
    provider: SerializerProvider[A])
      extends MapDBOffHeapIterator[A](s)
}
