package com.theseventhsense.utils.collections.spark

import com.theseventhsense.utils.collections.BulkCollection
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

/**
  * Created by erik on 12/8/16.
  */
class SparkBulkCollection[T](_underlying: RDD[T])(implicit tCt: ClassTag[T], spark: SparkSession)
    extends BulkCollection[T] {
  override def collect: Seq[T] = underlying.collect()

  override def map[V](op: (T) ⇒ V)(implicit vCt: ClassTag[V]): BulkCollection[V] =
    SparkBulkCollection(underlying.map(op))

  override def flatMap[U](op: (T) ⇒ TraversableOnce[U])(implicit uCt: ClassTag[U]): BulkCollection[U] =
    SparkBulkCollection(underlying.flatMap(op))

  override def aggregate[U](zero: U)(seqOp: (U, T) ⇒ U, combOp: (U, U) ⇒ U)(implicit uCt: ClassTag[U]): U =
    underlying.aggregate(zero)(seqOp, combOp)

//  def groupBy[K](op: (T) => K)(implicit kCt: ClassTag[K]) =
//    underlying.groupBy(op)

  override def mapWithKey[K](op: (T) ⇒ K)(implicit kCt: ClassTag[K]) =
    SparkBulkKVCollection[K, T](underlying.map(t ⇒ (op(t), t)))

  override def size: Long = underlying.count()

  override def filter(op: (T) ⇒ Boolean): BulkCollection[T] = SparkBulkCollection(underlying.filter(op))

  override def count(op: (T) ⇒ Boolean): Long = underlying.filter(op).count()

  def underlying: RDD[T] = _underlying
}

object SparkBulkCollection {
  def apply[K](underlying: RDD[K])(implicit kCt: ClassTag[K], spark: SparkSession): SparkBulkCollection[K] =
    new SparkBulkCollection(underlying)
}
