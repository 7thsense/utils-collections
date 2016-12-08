package com.theseventhsense.collections.stdlib

import com.theseventhsense.collections.KVBulkCollection

import scala.collection.GenTraversableOnce

/**
  * Created by erik on 12/8/16.
  */
case class StdLibKVBulkCollection[K, V](underlying: Seq[(K, V)]) extends KVBulkCollection[K, V] {
  override def sorted(implicit ordering: Ordering[K]): KVBulkCollection[K, V] =
    StdLibKVBulkCollection(underlying.sortBy(_._1)(ordering))
  override def keys   = StdLibBulkCollection(underlying.map(_._1))
  override def values = StdLibBulkCollection(underlying.map(_._2))

  override def foldByKey[A](initial: A)(op: (A, V) ⇒ A): StdLibKVBulkCollection[K, A] = {
    def keyOp(a: A, i: (K, V)): A = op(a, i._2)
    val folded                    = underlying.groupBy(_._1).map { case (k, vs) ⇒ (k, vs.foldLeft(initial)(keyOp)) }
    StdLibKVBulkCollection(folded.toSeq)
  }

  override def flatMap[A, B](op: (K, V) ⇒ GenTraversableOnce[(A, B)]): StdLibKVBulkCollection[A, B] =
    StdLibKVBulkCollection(underlying.flatMap { case (k, v) ⇒ op(k, v) })

  override def map[A, B](op: (K, V) ⇒ (A, B)): StdLibKVBulkCollection[A,B] =
    StdLibKVBulkCollection(underlying.map { case (k, v) ⇒ op(k, v) })

  override def mapValues[T](op: (V) ⇒ (T)): StdLibKVBulkCollection[K, T] =
    StdLibKVBulkCollection(underlying.map { case (k, v) ⇒ (k, op(v)) })

  override def join[B, C <: KVBulkCollection[K, B]](b: C): KVBulkCollection[K, (V, B)] = {
    val aMap       = this.collect
    val bMap       = b.collect
    val mergedKeys = (this.keys.collect ++ b.keys.collect).toSet
    val joined = mergedKeys.flatMap(k ⇒
      for {
        left  ← aMap.find(_._1 == k)
        right ← bMap.find(_._1 == k)
      } yield k -> (left._2 -> right._2))
    StdLibKVBulkCollection(joined.toSeq)
  }

  override def collect: Seq[(K, V)] = underlying

  override def union(b: KVBulkCollection[K, V]): KVBulkCollection[K, V] = StdLibKVBulkCollection(this.collect ++ b.collect)
}
