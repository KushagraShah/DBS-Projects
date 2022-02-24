package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File

object Main {
  def generate(sc : SparkContext, input_file : String, output_file : String, fraction : Double) : Unit = {
    val rdd_corpus = sc
      .textFile(input_file)
      .sample(false, fraction)

    rdd_corpus.coalesce(1).saveAsTextFile(output_file)
  }

  def recall(ground_truth : RDD[(String, Set[String])], lsh_truth : RDD[(String, Set[String])]) : Double = {
    val recall_vec = ground_truth
      .join(lsh_truth)
      .map(x => (x._1, x._2._1.intersect(x._2._2).size, x._2._1.size))
      .map(x => (x._2.toDouble/x._3.toDouble, 1))
      .reduce((x,y) => (x._1+y._1, x._2+y._2))

    val avg_recall = recall_vec._1/recall_vec._2

    avg_recall
  }

  def precision(ground_truth : RDD[(String, Set[String])], lsh_truth : RDD[(String, Set[String])]) : Double = {
    val precision_vec = ground_truth
      .join(lsh_truth)
      .map(x => (x._1, x._2._1.intersect(x._2._2).size, x._2._2.size))
      .map(x => (x._2.toDouble/x._3.toDouble, 1))
      .reduce((x,y) => (x._1+y._1, x._2+y._2))

    val avg_precision = precision_vec._1/precision_vec._2

    avg_precision
  }

  def construction1(SQLContext: SQLContext, rdd_corpus : RDD[(String, List[String])]) : Construction = {
    //implement construction1 composition here
    val lsh1 =  new BaseConstruction(SQLContext, rdd_corpus, 42)
    val lsh2 =  new BaseConstruction(SQLContext, rdd_corpus, 43)
    val lsh3 =  new BaseConstruction(SQLContext, rdd_corpus, 44)
    val lsh4 =  new BaseConstruction(SQLContext, rdd_corpus, 45)
    val lsh = new ANDConstruction(List(lsh1, lsh2, lsh3, lsh4))
    lsh
  }

  def construction2(SQLContext: SQLContext, rdd_corpus : RDD[(String, List[String])]) : Construction = {
    //implement construction2 composition here
    val lsh1 =  new BaseConstruction(SQLContext, rdd_corpus, 41)
    val lsh2 =  new BaseConstruction(SQLContext, rdd_corpus, 42)
    val lsh3 =  new BaseConstruction(SQLContext, rdd_corpus, 43)
    val lsh4 =  new BaseConstruction(SQLContext, rdd_corpus, 44)
    val lsh5 =  new BaseConstruction(SQLContext, rdd_corpus, 45)
    val lsh = new ORConstruction(List(lsh1, lsh2, lsh3, lsh4, lsh5))
    lsh
  }

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\winutil\\")
    val conf = new SparkConf().setAppName("app").setMaster("local[*]")
    implicit val sc: SparkContext = SparkContext.getOrCreate(conf)
    implicit val sqlContext: SQLContext = new org.apache.spark.sql.SQLContext(sc)
    sc.setLogLevel("ERROR")

    //type your queries here
    // EMPTY
  }
}
