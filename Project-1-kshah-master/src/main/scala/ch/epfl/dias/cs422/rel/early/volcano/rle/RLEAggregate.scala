package ch.epfl.dias.cs422.rel.early.volcano.rle

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilRLEentry, RLEentry}
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Aggregate]]
  * @see [[ch.epfl.dias.cs422.helpers.rex.AggregateCall]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator]]
  */
class RLEAggregate protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator,
    groupSet: ImmutableBitSet,
    aggCalls: IndexedSeq[AggregateCall]
) extends skeleton.Aggregate[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
    ](input, groupSet, aggCalls)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator {

  // Create a global list for passing the results from open() to next()
  var result = Vector.empty[Vector[RelOperator.Elem]]
  // Initialize a count for the next() block
  var count = 0

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    // Initialize the count and results list for the open() block
    result = Vector.empty[Vector[RelOperator.Elem]]
    count = 0
    // Initialize the input and create a list
    val inputList = input.toVector

    if (inputList.isEmpty) {
      // Use aggEmptyValue if the inputList is empty
      val empty = aggCalls.map(x => aggEmptyValue(x))
      result = result :+ empty.toVector
    } else { // Group the inputs based on the groupSet and compute the aggregate values otherwise

      // Store the grouped tuples from the groupSet in a hashmap
      val groupsMap = inputList.groupBy(entry => groupSet.asScala.toList.map(i => entry.value(i)))

      // 1. Iterate over each group using the hashmap keys
      for (key <- groupsMap.keys.iterator) {
        // Initialize a list with the group key to store the result
        var single_result = key.toVector

        // 2. Iterate over all aggCalls
        for (agg <- aggCalls) {
          // Get the group corresponding to the current key
          val group = groupsMap(key)

          // Initialize a variable with the first argument to collect cumulative results
          var cumulative = agg.getArgument(group.head.value, group.head.length)

          // 3. Iterate over all tuples in the group (from 1 since head(0) is already stored)
          for (i <- 1 until group.length) {
            // Compute the aggregate value cumulatively
            cumulative = aggReduce(cumulative, agg.getArgument(group(i).value, group(i).length), agg)
          } // end of 3

          // Append the aggregate value for each agg call
          single_result = single_result :+ cumulative
        } // end of 2

        // Append the result for each group
        result = result :+ single_result
      } // end of 1
    }
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[RLEentry] = {
    if (count == result.length) {
      // Return nothing if the entire result list has been traversed
      NilRLEentry
    } else {
      // Update the count and return the current entry (-1 because update comes first)
      count += 1
      Option(RLEentry(count, 1, result(count-1)))
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    // Empty
  }
}
