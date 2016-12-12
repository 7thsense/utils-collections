package com.theseventhsense.collections.spark

import com.theseventhsense.collections.{BulkCollection, KVBulkCollection}
import org.apache.spark.rdd.RDD

import scala.collection.GenTraversableOnce
import scala.reflect.ClassTag



case class SparkBulkKVCollection[K, V](underlying: RDD[(K, V)])(implicit kCt: ClassTag[K], vCt: ClassTag[V])
    extends KVBulkCollection[K, V] {
  override def sorted(implicit ordering: Ordering[K]): SparkBulkKVCollection[K, V] =
    SparkBulkKVCollection(underlying.sortByKey())

  override def values = SparkBulkCollection(underlying.map(_._2))

  override def keys: BulkCollection[K] = ???

  override def foldByKey[T](zero: T)(op: (T, V) => T): KVBulkCollection[K, T] = ???

  override def flatMap[A, B](op: (K, V) => GenTraversableOnce[(A, B)]): KVBulkCollection[A, B] = ???

  override def filter(op: (K, V) => Boolean): KVBulkCollection[K, V] = ???

  override def map[A, B](op: (K, V) => (A, B)): KVBulkCollection[A, B] = ???

  override def mapValues[T](op: (V) => T): KVBulkCollection[K, T] = ???

  override def innerJoin[B, C <: KVBulkCollection[K, B]](b: C): KVBulkCollection[K, (V, B)] = ???

  override def collect: Seq[(K, V)] = ???

  override def union(b: KVBulkCollection[K, V]): KVBulkCollection[K, V] = ???

  override def size: Long = ???

  override def leftJoin[B, C <: KVBulkCollection[K, B]](b: C): KVBulkCollection[K, (V, Option[B])] = ???
}
