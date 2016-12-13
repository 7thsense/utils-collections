package com.theseventhsense.collections.spark

import com.theseventhsense.collections.BulkCollection
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

/**
  * Created by erik on 12/8/16.
  */
case class SparkBulkCollection[T](underlying: RDD[T])(implicit tCt: ClassTag[T], spark: SparkSession)
    extends BulkCollection[T] {
  override def collect: Seq[T] = underlying.collect()

  override def map[V](op: (T) ⇒ V)(implicit vCt: ClassTag[V]): BulkCollection[V] =
    SparkBulkCollection(underlying.map(op))

  override def mapWithKey[K](op: (T) ⇒ K)(implicit kCt: ClassTag[K]) =
    SparkBulkKVCollection[K, T](underlying.map(t ⇒ (op(t), t)))

  override def size: Long = underlying.count()

  override def filter(op: (T) ⇒ Boolean): BulkCollection[T] = SparkBulkCollection(underlying.filter(op))

}
