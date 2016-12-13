package com.theseventhsense.collections

import scala.collection.GenTraversableOnce
import scala.reflect.ClassTag

/**
  * Created by erik on 12/8/16.
  */
trait KVBulkCollection[K, V] {
  def sorted(implicit ordering: Ordering[K]): KVBulkCollection[K, V]
  def keys: BulkCollection[K]
  def values: BulkCollection[V]
  def foldByKey[T](zero: T)(aggOp: (T, V) => T, combOp: (T, T) => T)(implicit tCt: ClassTag[T]): KVBulkCollection[K, T]
  def flatMap[A, B](op: (K, V) ⇒ TraversableOnce[(A, B)])(implicit aCt: ClassTag[A], bCt: ClassTag[B]): KVBulkCollection[A,B]
  def filter(op: (K, V) => Boolean): KVBulkCollection[K,V]
  def mapKV[A, B](op: (K, V) ⇒ (A, B))(implicit aCt: ClassTag[A], bCt: ClassTag[B]): KVBulkCollection[A,B]
  def mapValues[T](op: (V) ⇒ (T))(implicit tCt: ClassTag[T]): KVBulkCollection[K, T]
  def innerJoin[B, C <: KVBulkCollection[K,B]](b: C)(implicit bCt: ClassTag[B]): KVBulkCollection[K, (V, B)]
  def leftOuterJoin[B, C <: KVBulkCollection[K,B]](b: C)(implicit bCt: ClassTag[B]): KVBulkCollection[K, (V, Option[B])]
  def union(b: KVBulkCollection[K, V]): KVBulkCollection[K, V]
  def collect: Seq[(K,V)]
  def size: Long
}
