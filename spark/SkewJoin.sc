import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import com.tresata.spark.skewjoin.Dsl._
import com.tresata.spark.skewjoin._
import com.twitter.algebird.{ CMS, CMSHasher, CMSMonoid }
import java.util.{ Random => JRandom }

val spark = SparkSession.builder
  .master("local")
  .appName("spark session example")
  .getOrCreate()

import spark.implicits._

def getReplicationFactors(random: JRandom, replication: Int, otherReplication: Int): Seq[(Int, Int)] = {
  require(replication > 0 && otherReplication > 0, "replication must be positive")
  val rand = random.nextInt(otherReplication)
  (0 until replication).map(rep => (rand, rep))
}

def createRddCMS[K](rdd: RDD[K], cmsMonoid: CMSMonoid[K]): CMS[K] =
  rdd.map(k => cmsMonoid.create(k)).reduce(cmsMonoid.plus(_, _))

val keys    = Range(0, 10)
val rValues = keys.map(k ⇒ (k.toInt, "r" + k))
val lValues = keys.flatMap(k ⇒ Range(0, scala.math.pow(k, 3).toInt).map(j ⇒ (k, "l" + j))).sortBy(_._1)
lValues.size
val rRdd = spark.sparkContext.parallelize(rValues)
val lRdd = spark.sparkContext.parallelize(lValues)
val partitioner         = Partitioner.defaultPartitioner(rRdd, lRdd)
val skewReplication     = DefaultSkewReplication()
val cmsParams           = CMSParams()
val numPartitions       = partitioner.numPartitions
//val leftCMS             = createRddCMS(lRdd.keys, cmsParams.getCMSMonoid)
val broadcastedLeftCMS  = spark.sparkContext.broadcast(createRddCMS(lRdd.keys, cmsParams.getCMSMonoid))
//val rightCMS = createRddCMS(lRdd.keys, cmsParams.getCMSMonoid)
val broadcastedRightCMS = spark.sparkContext.broadcast(createRddCMS(lRdd.keys, cmsParams.getCMSMonoid))

val rddSkewed = lRdd.mapPartitions{ it =>
  val random = new JRandom
  it.flatMap { kv =>
    val (leftReplication, rightReplication) = skewReplication.getReplications(
      broadcastedLeftCMS.value.frequency(kv._1).estimate,
      broadcastedRightCMS.value.frequency(kv._1).estimate,
      numPartitions)
    getReplicationFactors(random, leftReplication, rightReplication).map(rl =>((kv._1, rl.swap), kv._2))
  }
  it
}

//val otherSkewed = rRdd.mapPartitions{ it =>
//  val random = new JRandom
//  it.flatMap{ kv =>
//    val (leftReplication, rightReplication) = skewReplication.getReplications(
//      broadcastedLeftCMS.value.frequency(kv._1).estimate,
//      broadcastedRightCMS.value.frequency(kv._1).estimate,
//      numPartitions)
//    getReplicationFactors(random, rightReplication, leftReplication).map(lr => ((kv._1, lr), kv._2))
//  }
//}
//
//rddSkewed.cogroup(otherSkewed, partitioner).map(kv => (kv._1._1, kv._2))

//  rRdd
//    .skewJoin(lRdd, partitioner = partitioner, skewReplication = skewReplication, cmsParams = cmsParams)
//    .toDS
//    .groupBy("_1")
//    .count()
//    .orderBy("_1")
//    .show(11)
