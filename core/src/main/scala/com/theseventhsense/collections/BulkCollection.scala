package com.theseventhsense.collections

import scala.reflect.ClassTag

/**
  * Created by erik on 9/15/16.
  */

trait BulkCollection[T] {
  def collect: Seq[T]
  def map[V](op: (T) => V)(implicit vCt: ClassTag[V]): BulkCollection[V]
  def mapWithKey[K](op: (T) => K)(implicit kCt: ClassTag[K]): KVBulkCollection[K, T]
  def size: Long
  def filter(op: (T) => Boolean): BulkCollection[T]

}
