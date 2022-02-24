package lsh

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

//custom class for partitioning
class CustomPartitioner(boundaries : Array[Int], partitions : Int) extends Partitioner {
  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = {
    val ret = boundaries
      .indexWhere(x => x >= key.asInstanceOf[Int])

    if (ret == -1) 0 else ret
  }
}

class BaseConstructionBalanced(sqlContext: SQLContext, data: RDD[(String, List[String])], seed : Int, partitions : Int) extends Construction {
  //minhash object
  val minhash = new MinHash(seed)
  //build buckets here
  private val buckets = minhash.execute(data)
    .map(x => (x._2, x._1))
    .groupByKey()
    .map(x => (x._1, x._2.toSet))

  def computeMinHashHistogram(queries : RDD[(String, Int)]) : Array[(Int, Int)] = {
    //compute histogram for target buckets of queries
    queries
      .map(x => (x._2, x._1))
      .groupByKey()
      .map(x => (x._1, x._2.size))
      .sortByKey()
      .collect()
  }

  def computePartitions(histogram : Array[(Int, Int)]) : Array[Int] = {
    //compute the boundaries of bucket partitions
    var boundaries = Array.empty[Int]
    val num_queries = histogram.map(x => x._2).sum.toFloat
    val threshold = (num_queries/partitions).ceil

    var sum = 0
    for (i <- histogram.indices) {
      sum = sum + histogram(i)._2
      if ((sum >= threshold) | (i == histogram.length - 1)) {
        sum = 0
        boundaries = boundaries :+ histogram(i)._1
      }
    }
    boundaries
  }

  override def eval(queries: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    //compute near neighbors with load balancing here
    val h_queries = minhash.execute(queries)
    val boundaries = computePartitions(
      computeMinHashHistogram(h_queries))

    val partitioner = new CustomPartitioner(boundaries, partitions)
    val part_buckets = buckets.partitionBy(partitioner)
    val part_queries = h_queries.map(x => (x._2, x._1)).partitionBy(partitioner)

    part_queries
      .join(part_buckets)
      .map(x => x._2)
  }
}