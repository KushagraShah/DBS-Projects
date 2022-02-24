package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession

class BaseConstructionBroadcast(sqlContext: SQLContext, data: RDD[(String, List[String])], seed : Int) extends Construction with Serializable {
  //minhash object
  val minhash = new MinHash(seed)
  //context for broadcasting
  val master = "local[*]"
  private val spark = SparkSession.builder.appName("Project2").master(master).getOrCreate
  //build buckets here
  private val buckets = spark.sparkContext.broadcast(
    minhash.execute(data)
      .map(x => (x._2, x._1))
      .groupByKey()
      .map(x => (x._1, x._2.toSet))
      .collect()
      .toMap)

  override def eval(queries: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    //compute near neighbors here
    minhash.execute(queries)
      .map(x => (x._2, x._1))
      .map(x => (x._2, buckets.value(x._1)))
  }
}
