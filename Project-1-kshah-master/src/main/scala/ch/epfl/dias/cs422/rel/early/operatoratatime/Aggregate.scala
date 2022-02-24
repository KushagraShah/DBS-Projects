package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Elem}
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Aggregate]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator]]
  */
class Aggregate protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
    groupSet: ImmutableBitSet,
    aggCalls: IndexedSeq[AggregateCall]
) extends skeleton.Aggregate[
      ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
    ](input, groupSet, aggCalls)
    with ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator {

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
  override def execute(): IndexedSeq[Column] = {
    // Create an empty sequence for the result
    var result = IndexedSeq.empty[IndexedSeq[Elem]]
    // Get the row sequence by taking a transpose
    val row_seq = input.toIndexedSeq.transpose
    // Remove the 'false' entries from the sequence
    val nRow_seq = get_trueSeq(row_seq)

    // Actual aggregate computation below
    if (nRow_seq.isEmpty) {
      // Use aggEmptyValue if the row sequence is empty; append 'true' selection vector
      val empty = aggCalls.map(x => aggEmptyValue(x)) :+ true
      result = result :+ empty.toVector
    } else { // Group the inputs based on the groupSet and compute the aggregate values otherwise

      // Store the grouped tuples from the groupSet in a hashmap
      val groupsMap = nRow_seq.groupBy(tuple => groupSet.asScala.toList.map(i => tuple(i)))

      // 1. Iterate over each group using the hashmap keys
      for (key <- groupsMap.keys.iterator) {
        // Initialize a list with the group key to store the result
        var single_result = key.toVector

        // 2. Iterate over all aggCalls
        for (agg <- aggCalls) {
          // Get the group corresponding to the current key
          val group = groupsMap(key)
          // Initialize a variable with the first argument to collect cumulative results
          var cumulative = agg.getArgument(group.head)

          // 3. Iterate over all tuples in the group (from 1 since head(0) is already stored)
          for (i <- 1 until group.length) {
            // Compute the aggregate value cumulatively
            cumulative = aggReduce(cumulative, agg.getArgument(group(i)), agg)
          } // end of 3

          // Append the aggregate value for each agg call
          single_result = single_result :+ cumulative
        } // end of 2

        // Append the 'true' selection vector
        single_result = single_result :+ true
        // Append the result for each group
        result = result :+ single_result
      } // end of 1
    } // end of else

    // Return the transpose of the sequence as the result
    result.transpose
  }
}
