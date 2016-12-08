package com.theseventhsense.collections

import scala.collection.GenTraversableOnce

/**
  * Created by erik on 12/8/16.
  */
trait KVBulkCollection[K, V] {
  def sorted(implicit ordering: Ordering[K]): KVBulkCollection[K, V]
  def keys: BulkCollection[K]
  def values: BulkCollection[V]
  def foldByKey[T](zero: T)(op: (T, V) => T): KVBulkCollection[K, T]
  def flatMap[A, B](op: (K, V) ⇒ GenTraversableOnce[(A, B)]): KVBulkCollection[A,B]
  def map[A, B](op: (K, V) ⇒ (A, B)): KVBulkCollection[A,B]
  def mapValues[T](op: (V) ⇒ (T)): KVBulkCollection[K, T]
  def join[B, C <: KVBulkCollection[K,B]](b: C): KVBulkCollection[K, (V, B)]
  def union(b: KVBulkCollection[K, V]): KVBulkCollection[K, V]
  def collect: Seq[(K,V)]
}
