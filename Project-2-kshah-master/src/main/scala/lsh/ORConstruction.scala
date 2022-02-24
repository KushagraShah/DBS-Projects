package lsh
import org.apache.spark.rdd.RDD

class ORConstruction(children: List[Construction]) extends Construction {
  override def eval(rdd: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    //compute OR construction results here
    children
      .map(x => x.eval(rdd))
      .reduce((base1, base2) => {
        base1
          .join(base2.distinct())
          .map(x => (x._1, x._2._1.union(x._2._2)))
      })
  }
}
