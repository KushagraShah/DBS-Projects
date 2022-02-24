package ch.epfl.dias.cs422.rel.early.columnatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import org.apache.calcite.rel.RelCollation

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Sort]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator]]
  */
class Sort protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
    collation: RelCollation,
    offset: Option[Int],
    fetch: Option[Int]
) extends skeleton.Sort[
      ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator
    ](input, collation, offset, fetch)
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

  // Comparator function to be used in SortWith
  def comparator (tuple1: Tuple, tuple2: Tuple): Boolean = {
    // Get the list of collations
    val collation_list = collation.getFieldCollations

    // Iterate over all collations
    for (i <- 0 until collation_list.size()) {
      // Get the ith collation from the list
      val coll = collation_list.get(i)
      // Convert the tuple elements at sorting key to comparables
      val t1_comparable = tuple1(coll.getFieldIndex).asInstanceOf[Comparable[Any]]
      val t2_comparable = tuple2(coll.getFieldIndex).asInstanceOf[Comparable[Any]]

      if (t1_comparable.compareTo(t2_comparable) < 0) {
        // This is the case where tuple1 < tuple2
        if (coll.direction.isDescending) {
          // Return false for descending collation
          return false
        } else {
          // Return true for ascending collation
          return true
        }
      } else if (t1_comparable.compareTo(t2_comparable) > 0) {
        // This is the case where tuple1 > tuple 2
        if (coll.direction.isDescending) {
          // Return true for descending collation
          return true
        } else {
          // Return false for ascending collation
          return false
        }
      }
    } // End of loop

    // The program will reach this point only if all the entries are equal
    // There is no need to sort in this case, hence, return false
    false
  }

  /**
   * @inheritdoc
   */
  override def execute(): IndexedSeq[HomogeneousColumn] = {
    // Get the input row sequence of homogenous columns
    val row_seq = input.execute().transpose
    // Remove the 'false' entries from the sequence
    val nRow_seq = get_trueSeq(row_seq)
    // Create empty sequences for the result in different formats
    var result = IndexedSeq.empty[IndexedSeq[Elem]]
    var result_hCol = IndexedSeq.empty[HomogeneousColumn]

    // Sort the sequence using the comparator function
    val sorted = nRow_seq.sortWith(comparator)

    // Initialize the start and end for the output range
    var start_range = 0
    var end_range = sorted.length
    // Assign the output ranges based on the availability of 'offset' and 'fetch'
    (offset.nonEmpty, fetch.nonEmpty) match {
      // Offset and Fetch
      case (true, true) =>
        start_range = Math.min(offset.get, sorted.length)
        end_range = Math.min(offset.get + fetch.get, sorted.length)
      // Offset but no Fetch
      case (true, false) =>
        start_range = Math.min(offset.get, sorted.length)
        end_range = sorted.length
      // no Offset but Fetch
      case (false, true) =>
        start_range = 0
        end_range = Math.min(fetch.get, sorted.length)
      // no Offset and no Fetch
      case _ =>
        start_range = 0
        end_range = sorted.length
    }

    // Iterate over the output range
    for (i <- start_range until end_range) {
      // Append the 'true' selection vector
      val tmp = sorted(i) :+ true
      // Append the sorted tuples to the result
      result = result :+ tmp
    }

    // Convert the result to sequence of homogenous columns
    for(column <- result.transpose) {
      result_hCol = result_hCol :+ toHomogeneousColumn(column)
    }
    // Return the result
    result_hCol
  }
}
