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

  // Create a global empty list to pass the results from open() to next()
  var passed_inputs = List.empty[Tuple]
  // Initialize a count for the next() block
  var count = 0

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    // Initialize count for the next() block
    count = 0
    // Initialize the input
    input.open()
    // Create a list of inputs
    val inputList = input.toList

    // Iterate over every entry in the input list
    for (input_tuple <- inputList) {
      if (predicate(input_tuple)) {
        // Append the input tuple to the list if it passes the 'predicate'
        passed_inputs = passed_inputs :+ input_tuple
      }
    }
    // To avoid multiple copies in multiple iterations
    passed_inputs = passed_inputs.distinct

    // De-initialize the input
    input.close()
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    if (count == passed_inputs.length) {
      // Return nothing if the entire passed inputs list has been traversed
      NilTuple
    } else {
      // Update the count and return the current entry (-1 because update comes first)
      count += 1
      Option(passed_inputs(count-1))
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    // Empty
  }
}
