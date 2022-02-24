package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Elem, Tuple}
import org.apache.calcite.rex.RexNode

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Filter]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator]]
  */
class Filter protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
    condition: RexNode
) extends skeleton.Filter[
      ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
    ](input, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator {

  /**
    * Function that, evaluates the predicate [[condition]]
    * on a (non-NilTuple) tuple produced by the [[input]] operator
    */
  lazy val predicate: Tuple => Boolean = {
    val evaluator = eval(condition, input.getRowType)
    (t: Tuple) => evaluator(t).asInstanceOf[Boolean]
  }

  // Filter does not prune tuples. False entries are untouched.

  /**
   * @inheritdoc
   */
  def execute(): IndexedSeq[Column] = {
    // Create an empty filtered sequence
    var filt_seq = IndexedSeq.empty[IndexedSeq[Elem]]
    // Get the row sequence by taking a transpose
    val row_seq = input.toIndexedSeq.transpose

    // Iterate over each row
    for(row <- row_seq) {
      // Initialize as row to keep the 'false' entries
      var filt_row = row
      // Check the 'predicate' only for the 'true' entries
      if(row.last.asInstanceOf[Boolean]) {
        // Make it 'false' is it does not pass the 'predicate'
        if(!predicate(row.init)) {
          filt_row = row.init :+ false
        }
      }
      // Append the filtered row to the sequence
      filt_seq = filt_seq :+ filt_row
    }

    // Return the transpose of the sequence as the result
    filt_seq.transpose
  }
}
