package ch.epfl.dias.cs422.rel.early.columnatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import org.apache.calcite.rex.RexNode

/**
  * @inheritdoc
  *
  * NOTE: in column-at-a-time execution with selection vectors, the
  * filter does not prune the tuples, only marks the corresponding
  * entries in the selection vector as false.
  * Removing tuples will be penalized.
  *
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Filter]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator]]
  */
class Filter protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
    condition: RexNode
) extends skeleton.Filter[
      ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator
    ](input, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator {

  /**
    * Function that, evaluates the predicate [[condition]]
    * on a (non-NilTuple) tuple produced by the [[input]] operator
    */
  lazy val mappredicate: IndexedSeq[HomogeneousColumn] => Array[Boolean] = {
    val evaluator = map(condition, input.getRowType, isFilterCondition = true)
    (t: IndexedSeq[HomogeneousColumn]) => unwrap[Boolean](evaluator(t))
  }

  // Filter does not prune tuples. False entries are untouched.

  /**
    * @inheritdoc
    */
  def execute(): IndexedSeq[HomogeneousColumn] = {
    // Get the input sequence of homogenous columns
    val inp_seq = input.execute()
    // Return empty sequence in case of empty input
    if(inp_seq.isEmpty) {
      return IndexedSeq[HomogeneousColumn]()
    }

    // Run the predicate test for the full input at once
    val predicates = mappredicate(inp_seq.init)
    // Fetch the selection vector as a sequence
    val sel_vector = inp_seq.last.toVector
    // Create an empty sequence to update the selection vector
    var updated_sv = Vector.empty[Elem]

    // Iterate over the selection vector
    for(i <- sel_vector.indices) {
      if(sel_vector(i).asInstanceOf[Boolean]) {
        // Append the predicate for 'true' entries
        updated_sv = updated_sv :+ predicates(i)
      } else {
        // Do not update the 'false' entries
        updated_sv = updated_sv :+ false
      }
    }

    // Append the updated selection vector to the input sequence
    val filt_seq = inp_seq.init :+ toHomogeneousColumn(updated_sv)
    // Return the filtered sequence
    filt_seq
  }
}
