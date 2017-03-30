package com.theseventhsense.utils.collections.spark

import com.theseventhsense.utils.collections.{BulkCollection, KVBulkCollection}
import org.apache.spark.{HashPartitioner, Partitioner}
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

  override def flatMapKV[A, B](op: (K, V) ⇒ TraversableOnce[(A, B)])(
      implicit aCt: ClassTag[A],
      bCt: ClassTag[B]): KVBulkCollection[A, B] =
    SparkBulkKVCollection.flatMapKV(underlying, op)

  override def filter(op: (K, V) ⇒ Boolean): KVBulkCollection[K, V] =
    SparkBulkKVCollection(underlying.filter(op.tupled))

  override def mapKV[A, B](op: (K, V) ⇒ (A, B))(implicit aCt: ClassTag[A],
                                                bCt: ClassTag[B]): KVBulkCollection[A, B] =
    SparkBulkKVCollection.mapKV(underlying, op)

  override def mapValues[T](op: (V) ⇒ T)(implicit tCt: ClassTag[T]): KVBulkCollection[K, T] =
    SparkBulkKVCollection.mapValues(underlying, op)

  override def collect: Seq[(K, V)] = underlying.collect

  override def unionKV(b: KVBulkCollection[K, V]): KVBulkCollection[K, V] = b match {
    case x: SparkBulkKVCollection[K, V] ⇒
      val rdd: RDD[(K, V)] = underlying.union(x.underlying)
      SparkBulkKVCollection.apply[K, V](rdd)
    case _ ⇒
      throw new Exception(s"$b is not a spark kv collection, can't union")
  }

  override def innerJoin[B, C <: KVBulkCollection[K, B]](
      b: C)(implicit kOrd: Ordering[K], bCt: ClassTag[B]): KVBulkCollection[K, (V, B)] = {
    val bRdd: RDD[(K, B)] = b match {
      case x: SparkBulkKVCollection[K, B] ⇒ x.underlying
      case _                              ⇒ spark.sparkContext.emptyRDD[(K, B)]
    }
    //    implicit val cmsHasherK: CMSHasher[K] = CMSHasherInt.contramap(x ⇒ x.hashCode())
//    val joined = if(underlying.partitions.length > bRdd.partitions.length) {
//      val b = bRdd.repartition(underlying.partitions.size)
//      underlying.skewJoin[B](b, SparkBulkKVCollection.defaultPartitioner(underlying, b))
//    } else {
//      val u = underlying.repartition(bRdd.partitions.size)
//      u.skewJoin[B](bRdd, SparkBulkKVCollection.defaultPartitioner(u, bRdd))
//    }
    SparkBulkKVCollection(underlying.join(bRdd))
  }

  override def leftOuterJoin[B, C <: KVBulkCollection[K, B]](
      b: C)(implicit kOrd: Ordering[K], bCt: ClassTag[B]): KVBulkCollection[K, (V, Option[B])] = {
    val bRdd: RDD[(K, B)] = b match {
      case x: SparkBulkKVCollection[K, B] ⇒ x.underlying
      case _                              ⇒ spark.sparkContext.emptyRDD[(K, B)]
    }
//    implicit val cmsHasherK: CMSHasher[K] = CMSHasherInt.contramap(x ⇒ x.hashCode())
//    val joined = if(underlying.partitions.length > bRdd.partitions.length) {
//      val b = bRdd.repartition(underlying.partitions.size)
//      underlying.skewLeftOuterJoin(b, SparkBulkKVCollection.defaultPartitioner(underlying, b))
//    } else {
//      val u = underlying.repartition(bRdd.partitions.size)
//      u.skewLeftOuterJoin(bRdd, SparkBulkKVCollection.defaultPartitioner(u, bRdd))
//    }
    SparkBulkKVCollection(underlying.leftOuterJoin(bRdd))
  }

  override def subtractByKey[T](b: KVBulkCollection[K, T])(implicit tCt: ClassTag[T]): KVBulkCollection[K, V] = {
    SparkBulkKVCollection(underlying.subtractByKey(b.asInstanceOf[SparkBulkKVCollection[K, V]].underlying))
  }

  def persistKV(): SparkBulkKVCollection[K, V] =
    SparkBulkKVCollection(underlying.persist(StorageLevel.MEMORY_AND_DISK))
}

object SparkBulkKVCollection {
  def apply[K, V](underlying: RDD[(K, V)])(implicit kCt: ClassTag[K],
                                           vCt: ClassTag[V],
                                           spark: SparkSession): SparkBulkKVCollection[K, V] =
    new SparkBulkKVCollection(underlying)

  def mapValues[K, V, T](underlying: RDD[(K, V)], op: (V) ⇒ T)(implicit tCt: ClassTag[T],
                                                               kCt: ClassTag[K],
                                                               spark: SparkSession): KVBulkCollection[K, T] =
    SparkBulkKVCollection(underlying.map {
      case (k, v) ⇒
        (k, op(v))
    })

  def flatMapKV[K, V, A, B](underlying: RDD[(K, V)], op: (K, V) ⇒ TraversableOnce[(A, B)])(
      implicit aCt: ClassTag[A],
      bCt: ClassTag[B],
      spark: SparkSession): KVBulkCollection[A, B] =
    SparkBulkKVCollection(underlying.flatMap { case (k, v) ⇒ op(k, v) })

  def mapKV[K, V, A, B](underlying: RDD[(K, V)], op: (K, V) ⇒ (A, B))(
      implicit aCt: ClassTag[A],
      bCt: ClassTag[B],
      spark: SparkSession): KVBulkCollection[A, B] =
    SparkBulkKVCollection(underlying.map { case (k, v) ⇒ op(k, v) })

  // ignore the "spark.default.partitions" setting
  def defaultPartitioner(rdd: RDD[_], others: RDD[_]*): Partitioner = {
    val bySize = (Seq(rdd) ++ others).sortBy(_.partitions.length).reverse
    for (r <- bySize if r.partitioner.isDefined && r.partitioner.get.numPartitions > 0) {
      return r.partitioner.get
    }
    new HashPartitioner(bySize.head.partitions.length)
  }
}
