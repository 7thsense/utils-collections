package com.theseventhsense.collections.spark

import com.theseventhsense.collections.{BulkCollection, KVBulkCollection}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect._

case class SparkBulkKVCollection[K, V](underlying: RDD[(K, V)])(implicit kCt: ClassTag[K], vCt: ClassTag[V], spark: SparkSession)
    extends KVBulkCollection[K, V] {
  override def sorted(implicit ordering: Ordering[K]): SparkBulkKVCollection[K, V] =
    SparkBulkKVCollection(underlying.sortByKey())

  override def values = SparkBulkCollection(underlying.map(_._2))

  override def keys: BulkCollection[K] = SparkBulkCollection(underlying.map(_._1))

  override def foldByKey[T: ClassTag](zero: T)(aggOp: (T, V) ⇒ T, combOp: (T, T) ⇒ T): KVBulkCollection[K, T] =
    SparkBulkKVCollection(underlying.aggregateByKey(zero)(aggOp, combOp))

  override def flatMap[A, B](op: (K, V) ⇒ TraversableOnce[(A, B)])(implicit aCt: ClassTag[A], bCt: ClassTag[B]): KVBulkCollection[A, B] =
    SparkBulkKVCollection(underlying.flatMap{ case (k, v) => op(k, v) })

  override def filter(op: (K, V) ⇒ Boolean): KVBulkCollection[K, V] =
    SparkBulkKVCollection(underlying.filter(op.tupled))

  override def mapKV[A, B](op: (K, V) ⇒ (A, B))(implicit aCt: ClassTag[A],
                                                bCt: ClassTag[B]): KVBulkCollection[A, B] =
    SparkBulkKVCollection(underlying.map { case (k, v) ⇒ op(k, v) })

  override def mapValues[T](op: (V) ⇒ T)(implicit tCt: ClassTag[T]): KVBulkCollection[K, T] =
    SparkBulkKVCollection(underlying.map {
      case (k, v) ⇒
        (k, op(v))
    })

  override def collect: Seq[(K, V)] = underlying.collect

  override def union(b: KVBulkCollection[K, V]): KVBulkCollection[K, V] = b match {
    case SparkBulkKVCollection(underlyingB) ⇒
      SparkBulkKVCollection(underlying.union(underlyingB))
    case _ ⇒
      throw new Exception(s"$b is not a spark kv collection, can't union")
  }

  override def size: Long = underlying.count()

  override def innerJoin[B, C <: KVBulkCollection[K, B]](b: C)(implicit bCt: ClassTag[B]): KVBulkCollection[K, (V, B)] = {
    val bRdd: RDD[(K, B)] = b match {
      case SparkBulkKVCollection(bRdd: RDD[(K, B)]) => bRdd
      case _ => spark.sparkContext.parallelize(Seq.empty[(K, B)])
    }
    SparkBulkKVCollection(underlying.join(bRdd))
  }

  override def leftOuterJoin[B, C <: KVBulkCollection[K, B]](b: C)(implicit bCt: ClassTag[B]): KVBulkCollection[K, (V, Option[B])] = {
    val bRdd: RDD[(K, B)] = b match {
      case SparkBulkKVCollection(bRdd: RDD[(K, B)]) => bRdd
      case _ => spark.sparkContext.parallelize(Seq.empty[(K, B)])
    }
    SparkBulkKVCollection(underlying.leftOuterJoin(bRdd))
  }
}
