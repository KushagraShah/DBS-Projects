package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, Tuple}
import org.apache.calcite.rex.RexNode

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Join(
    left: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    right: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    condition: RexNode
) extends skeleton.Join[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](left, right, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  // Create an empty global list for the results
  var result_list = Vector.empty[Tuple]
  // Initialize a count for the next() block
  var count = 0

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    // Initialize the count and results list for open()
    count = 0
    result_list = Vector.empty[Tuple]

    // Initialize left and right inputs as lists
    val left_list = left.toVector
    val right_list = right.toVector
    // Get the left and right keys
    val left_keys = getLeftKeys
    val right_keys = getRightKeys

    // Create hashmaps for the left and right inputs, group by keys
    val leftMap = left_list.groupBy(tuple => left_keys.map(i => tuple(i)))
    val rightMap = right_list.groupBy(tuple => right_keys.map(i => tuple(i)))

    // Iterate over each entry in the left hashmap
    for(left_entry <- leftMap) {
      // Get the key we want to compare
      val comp_key = left_entry._1
      // Check if the right hashmap contains the same keys
      if(rightMap.contains(comp_key)) {
        // Iterate over all left and right elements with that key
        for(left_tuple <- leftMap(comp_key)) {
          for(right_tuple <- rightMap(comp_key)) {
            // Append the concatenated tuple pair to the result
            val concat_tuple = left_tuple ++ right_tuple
            result_list = result_list :+ concat_tuple
          }
        }
      }
    }
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    if (count == result_list.length) {
      // Return nothing if the entire results list has been traversed
      NilTuple
    } else {
      // Update the count and return the current entry (-1 because update comes first)
      count += 1
      Option(result_list(count-1))
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    // Empty
  }
}
