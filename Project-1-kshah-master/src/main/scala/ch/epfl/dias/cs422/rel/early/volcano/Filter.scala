package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, Tuple}
import org.apache.calcite.rex.RexNode

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Filter]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Filter protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    condition: RexNode
) extends skeleton.Filter[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](input, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  /**
    * Function that, evaluates the predicate [[condition]]
    * on a (non-NilTuple) tuple produced by the [[input]] operator
    */
  lazy val predicate: Tuple => Boolean = {
    val evaluator = eval(condition, input.getRowType)
    (t: Tuple) => evaluator(t).asInstanceOf[Boolean]
  }

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    // Initialize the input
    input.open()
  }


  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    // Iterate infinitively
    while(true) {
      // Get the next input tuple
      val next_tuple = input.next()

      if (next_tuple == NilTuple) {
        // Return nothing if there are no more available tuples
        return NilTuple
      } else if (predicate(next_tuple.get)) {
        // Return the tuple which passes the 'predicate' test
        return next_tuple
      }
    }
    NilTuple
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    // De-initialize the input
    input.close()
  }
}
