package ch.epfl.dias.cs422.rel.early.columnatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import org.apache.calcite.rex.RexNode

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator]]
  */
class Join(
    left: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
    right: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
    condition: RexNode
) extends skeleton.Join[
      ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator
    ](left, right, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator {

  // Function to return only the 'true' entries of the input sequence (with the selection vectors dropped)
  def get_trueSeq (inSeq: IndexedSeq[IndexedSeq[Elem]]) : IndexedSeq[IndexedSeq[Elem]] = {
    // Create an empty sequence for the output
    var outSeq = IndexedSeq.empty[IndexedSeq[Elem]]

    // Iterate over all input entries
    for (entry <- inSeq) {
      if(entry.last.asInstanceOf[Boolean]) {
        // Append the entry if the selection vector is true
        // Note, the selection vector entry is removed
        outSeq = outSeq :+ entry.init
      }
    }
    // Return the calculated output sequence
    outSeq
  }

  /**
   * @inheritdoc
   */
  override def execute(): IndexedSeq[HomogeneousColumn] = {
    // Get the left and right row sequences of homogeneous columns
    val left_seq = left.execute().transpose
    val right_seq = right.execute().transpose
    // Remove the 'false' entries from both the sequences
    val nLeft_seq = get_trueSeq(left_seq)
    val nRight_seq = get_trueSeq(right_seq)
    // Create empty sequences for the result in different formats
    var result = IndexedSeq.empty[IndexedSeq[Elem]]
    var result_hCol = IndexedSeq.empty[HomogeneousColumn]
    // Get the left and right keys
    val left_keys = getLeftKeys
    val right_keys = getRightKeys

    // Create hashmaps for the left and right inputs, group by keys
    val leftMap = nLeft_seq.groupBy(tuple => left_keys.map(i => tuple(i)))
    val rightMap = nRight_seq.groupBy(tuple => right_keys.map(i => tuple(i)))

    // Iterate over each entry in the left hashmap
    for(left_entry <- leftMap) {
      // Get the key we want to compare
      val comp_key = left_entry._1
      // Check if the right hashmap contains the same keys
      if(rightMap.contains(comp_key)) {
        // Iterate over all left and right elements with that key
        for(left_tuple <- leftMap(comp_key)) {
          for(right_tuple <- rightMap(comp_key)) {
            // Create the concatenated tuple pair with 'true' selection vector
            val concat_tuple = (left_tuple ++ right_tuple) :+ true
            // Append it to the result sequence
            result = result :+ concat_tuple
          }
        }
      }
    }

    // Convert the result to sequence of homogenous columns
    for(column <- result.transpose) {
      result_hCol = result_hCol :+ toHomogeneousColumn(column)
    }
    // Return the result
    result_hCol
  }
}
