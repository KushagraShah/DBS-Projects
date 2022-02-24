package ch.epfl.dias.cs422.rel.early.volcano.rle

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilRLEentry, RLEentry}
import org.apache.calcite.rex.RexNode

/**
  * @inheritdoc
  *
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator]]
  */
class RLEJoin(
    left: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator,
    right: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator,
    condition: RexNode
) extends skeleton.Join[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
    ](left, right, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator {

  // Create an empty list to store the results
  var result_list = Vector.empty[RLEentry]
  // Initialize a count for the new result startVID
  var nSID = 0
  // Initialize a count for the next() block
  var count = 0

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    // Initialize the counts and the result list for open()
    result_list = Vector.empty[RLEentry]
    nSID = 0
    count = 0

    // Initialize the input lists
    val left_list = left.toVector
    val right_list = right.toVector
    // Get the left and right keys
    val left_keys = getLeftKeys
    val right_keys = getRightKeys

    // Create hashmaps for the left and right inputs, group by keys
    val leftMap = left_list.groupBy(tuple => left_keys.map(i => tuple.value(i)))
    val rightMap = right_list.groupBy(tuple => right_keys.map(i => tuple.value(i)))

    // Iterate over each entry in the left hashmap
    for(left_entry <- leftMap) {
      // Get the key we want to compare
      val comp_key = left_entry._1
      // Check if the right hashmap contains the same keys
      if(rightMap.contains(comp_key)) {
        // Iterate over all left and right elements with that key
        for(left_tuple <- leftMap(comp_key)) {
          for(right_tuple <- rightMap(comp_key)) {
            // Calculate the result entry parameters and add it to the results list
            val nLen = left_tuple.length * right_tuple.length // multiplication
            val nVal = left_tuple.value ++ right_tuple.value  // concatenation
            result_list = result_list :+ RLEentry(nSID, nLen, nVal)
            // Increment the StartVID for the next result entry
            nSID += 1
          }
        }
      }
    }
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[RLEentry] = {
    if (count == result_list.length) {
      // Return nothing if the entire results list has been traversed
      NilRLEentry
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
