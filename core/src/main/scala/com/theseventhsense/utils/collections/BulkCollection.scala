package com.theseventhsense.utils.collections

import scala.reflect.ClassTag

/**
  * Created by erik on 9/15/16.
  */

trait BulkCollection[T] extends Serializable {
  def collect: Seq[T]
  def map[V](op: (T) => V)(implicit vCt: ClassTag[V]): BulkCollection[V]
  def flatMap[U](op: (T) ⇒ TraversableOnce[U])(implicit vCt: ClassTag[U]): BulkCollection[U]
  def aggregate[U](zero: U)(seqOp: (U, T) => U, combOp: (U, U) => U)(implicit uCt: ClassTag[U]): U
  def mapWithKey[K](op: (T) => K)(implicit kCt: ClassTag[K]): KVBulkCollection[K, T]
  def size: Long
  def filter(op: (T) => Boolean): BulkCollection[T]
  def count(op: (T) => Boolean): Long
}
