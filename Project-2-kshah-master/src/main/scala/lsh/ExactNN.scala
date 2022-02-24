package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

class ExactNN(sqlContext: SQLContext, data: RDD[(String, List[String])], threshold : Double) extends Construction with Serializable {
  //calculate Jaccard similarity
  def getJaccard(input1: List[String], input2: List[String]): Double = {
    val inter = input1.intersect(input2).toSet.size.toDouble
    val union = input1.union(input2).toSet.size.toDouble
    val jaccard = inter/union
    jaccard
  }

  override def eval(rdd: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    //compute exact near neighbors here
    rdd
      .cartesian(data)
      .filter(x => getJaccard(x._1._2, x._2._2) > threshold)
      .map(x => (x._1._1, x._2._1))
      .groupByKey()
      .map(x => (x._1, x._2.toSet))
  }
}
