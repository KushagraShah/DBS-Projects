package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, Tuple}
import org.apache.calcite.rex.RexNode

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join.getLeftKeys]]
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join.getRightKeys]]
  */
class Join(
    left: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    right: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    condition: RexNode
) extends skeleton.Join[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](left, right, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  // Create empty global lists for the left and right tuples
  var left_list = List.empty[Tuple]
  var right_list = List.empty[Tuple]
  // Create an empty global list for the results
  var result_list = List.empty[Tuple]

  // Initialize a count for the next() block
  var count = 0

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    // Initialize count for the next() block
    count = 0

    // Initialize the left input and pass it to a global list
    left.open()
    left_list = left.toList
    left.close()
    // Initialize the right input and pass it to a global list
    right.open()
    right_list = right.toList
    right.close()
    // Get the left and right keys
    val left_keys = getLeftKeys
    val right_keys = getRightKeys

    // 1. Iterate over all left tuples
    for (left_tuple <- left_list) {
      // 2. Iterate over all right tuples
      for (right_tuple <- right_list) {
        // Initialize a flag to indicate if the tuple pair should be passed
        var flag = true

        // 3. Iterate over each key in the left/right keys
        for (left_key <- left_keys) {
          // Get the right key using the left key's index value
          val index = left_keys.indexOf(left_key)
          val right_key = right_keys(index)

          // Convert the tuple elements at the current key to comparables
          val left_comparable = left_tuple(left_key).asInstanceOf[Comparable[Any]]
          val right_comparable = right_tuple(right_key).asInstanceOf[Comparable[Any]]

          // Check if any of the key elements of the tuple pair are not equal
          if (left_comparable.compareTo(right_comparable) != 0) {
            // Set the flag to false if that's the case
            flag = false
          }
        }

        if (flag) {
          // The flag will be true only if the tuple pair is equal for ALL key elements
          // Pass the concatenated tuple pair to the results list
          result_list = result_list :+ (left_tuple ++ right_tuple)
        }
      }
    }

    // To avoid multiple copies in multiple iterations
    result_list = result_list.distinct
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
