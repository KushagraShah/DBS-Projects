package ch.epfl.dias.cs422.rel.early.volcano.rle

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilRLEentry, RLEentry, Tuple}
import org.apache.calcite.rex.RexNode

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Filter]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator]]
  */
class RLEFilter protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator,
    condition: RexNode
) extends skeleton.Filter[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
    ](input, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator {

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
  override def next(): Option[RLEentry] = {
    // Iterate infinitively
    while(true) {
      // Get next input RLE entry
      val next_entry = input.next()

      if (next_entry == NilRLEentry) {
        // Return nothing if there are no more available entries
        return NilRLEentry
      } else if (predicate(next_entry.get.value)) {
        // Return the RLE entry for which the tuple passes the 'predicate' test
        return next_entry
      }
    }

    // Code will never reach here
    NilRLEentry
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    // De-initialize the input
    input.close()
  }
}
