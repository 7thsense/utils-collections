package com.theseventhsense.utils.collections.spark

import com.theseventhsense.utils.collections.{BulkCollection, KVBulkCollection}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.reflect._

class SparkBulkKVCollection[K, V](
    underlying: RDD[(K, V)])(implicit kCt: ClassTag[K], vCt: ClassTag[V], spark: SparkSession)
    extends SparkBulkCollection[(K, V)](underlying)
    with KVBulkCollection[K, V] {
  override def sorted(implicit ordering: Ordering[K]): SparkBulkKVCollection[K, V] =
    SparkBulkKVCollection(underlying.sortByKey())

  override def values = SparkBulkCollection(underlying.map(_._2))

  override def keys: BulkCollection[K] = SparkBulkCollection(underlying.map(_._1))

  override def foldByKey[T](zero: T)(aggOp: (T, V) ⇒ T, combOp: (T, T) ⇒ T)(
      implicit tCt: ClassTag[T]): KVBulkCollection[K, T] =
    SparkBulkKVCollection(underlying.aggregateByKey(zero)(aggOp, combOp))

  override def flatMapKV[A, B](op: (K, V) ⇒ TraversableOnce[(A, B)])(implicit aCt: ClassTag[A],
                                                                     bCt: ClassTag[B]): KVBulkCollection[A, B] =
    SparkBulkKVCollection(underlying.flatMap { case (k, v) ⇒ op(k, v) })

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

  override def unionKV(b: KVBulkCollection[K, V]): KVBulkCollection[K, V] = b match {
    case x: SparkBulkKVCollection[K, V] ⇒
      val rdd: RDD[(K, V)] = underlying.union(x.underlying)
      SparkBulkKVCollection.apply[K, V](rdd)
    case _ ⇒
      throw new Exception(s"$b is not a spark kv collection, can't union")
  }

  override def innerJoin[B, C <: KVBulkCollection[K, B]](b: C)(
      implicit bCt: ClassTag[B]): KVBulkCollection[K, (V, B)] = {
    val bRdd: RDD[(K, B)] = b match {
      case x: SparkBulkKVCollection[K, B] ⇒ x.underlying
      case _                              ⇒ spark.sparkContext.parallelize(Seq.empty[(K, B)])
    }
    SparkBulkKVCollection(underlying.join(bRdd))
  }

  override def leftOuterJoin[B, C <: KVBulkCollection[K, B]](b: C)(
      implicit bCt: ClassTag[B]): KVBulkCollection[K, (V, Option[B])] = {
    val bRdd: RDD[(K, B)] = b match {
      case x: SparkBulkKVCollection[K, B] ⇒ x.underlying
      case _                              ⇒ spark.sparkContext.parallelize(Seq.empty[(K, B)])
    }
    SparkBulkKVCollection(underlying.leftOuterJoin(bRdd))
  }

  def persistKV(): SparkBulkKVCollection[K,V] = SparkBulkKVCollection(underlying.persist(StorageLevel.OFF_HEAP))
}

object SparkBulkKVCollection {
  def apply[K, V](underlying: RDD[(K, V)])(implicit kCt: ClassTag[K],
                                           vCt: ClassTag[V],
                                           spark: SparkSession): SparkBulkKVCollection[K, V] =
    new SparkBulkKVCollection(underlying)
}
