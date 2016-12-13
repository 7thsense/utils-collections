package com.theseventhsense.collections.stdlib

import com.theseventhsense.collections.KVBulkCollection

import scala.collection.GenTraversableOnce
import scala.reflect.ClassTag

/**
  * Created by erik on 12/8/16.
  */
case class StdLibKVBulkCollection[K, V](underlying: Seq[(K, V)]) extends KVBulkCollection[K, V] {
  override def sorted(implicit ordering: Ordering[K]): KVBulkCollection[K, V] =
    StdLibKVBulkCollection(underlying.sortBy(_._1)(ordering))
  override def keys   = StdLibBulkCollection(underlying.map(_._1))
  override def values = StdLibBulkCollection(underlying.map(_._2))

  override def foldByKey[T: ClassTag](zero: T)(aggOp: (T, V) => T, combOp: (T, T) => T): KVBulkCollection[K, T] = {
    def keyOp(a: T, i: (K, V)): T = {
      implicit val key = i._1
      aggOp(a, i._2)
    }
    val folded                    = underlying
      .groupBy(_._1)
      .map { case (k, vs) ⇒ (k, vs.foldLeft(zero)(keyOp)) }
    StdLibKVBulkCollection(folded.toSeq)
  }

  override def filter(op: (K, V) ⇒ Boolean): KVBulkCollection[K, V] =
    StdLibKVBulkCollection(underlying.filter { case (k, v) ⇒ op(k, v) })

  override def flatMap[A, B](op: (K, V) ⇒ TraversableOnce[(A, B)])(implicit aCt: ClassTag[A], bCt: ClassTag[B]): StdLibKVBulkCollection[A, B] =
    StdLibKVBulkCollection(underlying.flatMap { case (k, v) ⇒ op(k, v) })

  override def mapKV[A, B](op: (K, V) ⇒ (A, B))(implicit aCt: ClassTag[A], bCt: ClassTag[B]): StdLibKVBulkCollection[A, B] =
    StdLibKVBulkCollection(underlying.map { case (k, v) ⇒ op(k, v) })

  override def mapValues[T](op: (V) ⇒ (T))(implicit tCt: ClassTag[T]): StdLibKVBulkCollection[K, T] =
    StdLibKVBulkCollection(underlying.map { case (k, v) ⇒ (k, op(v)) })

  override def innerJoin[B, C <: KVBulkCollection[K, B]](b: C)(implicit bCt: ClassTag[B]): KVBulkCollection[K, (V, B)] = {
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

  override def leftOuterJoin[B, C <: KVBulkCollection[K, B]](b: C)(implicit bCt: ClassTag[B]): KVBulkCollection[K, (V, Option[B])] = {
    val aMap = this.collect
    val bMap = b.collect
    val joined = aMap.map {
      case (k, v) ⇒
        (k, (v, bMap.find(_._1 == k).map(_._2)))
    }
    StdLibKVBulkCollection(joined)
  }

  override def collect: Seq[(K, V)] = underlying

  override def union(b: KVBulkCollection[K, V]): KVBulkCollection[K, V] =
    StdLibKVBulkCollection(this.collect ++ b.collect)

  override def size: Long = this.underlying.size
}
