package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

class BaseConstruction(sqlContext: SQLContext, data: RDD[(String, List[String])], seed : Int) extends Construction {
  //minhash object
  val minhash = new MinHash(seed)
  //build buckets here
  private val buckets = minhash.execute(data)
    .map(x => (x._2, x._1))
    .groupByKey()
    .map(x => (x._1, x._2.toSet))

  override def eval(queries: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    //compute near neighbors here
    minhash.execute(queries)
      .map(x => (x._2, x._1))
      .join(buckets)
      .map(x => x._2)
  }
}
