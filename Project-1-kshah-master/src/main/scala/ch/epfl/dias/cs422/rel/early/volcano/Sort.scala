package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Elem, NilTuple, Tuple}
import org.apache.calcite.rel.RelCollation

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Sort]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Sort protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    collation: RelCollation,
    offset: Option[Int],
    fetch: Option[Int]
) extends skeleton.Sort[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](input, collation, offset, fetch)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  // Using a global list to pass the sorted result from open() to next() block
  private var sorted = Vector[Tuple]()
  // Initialize a count for the next() block
  var count = 0

  // Define a comparison function to be used in the open() block
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
  override def open(): Unit = {
    // Initialize the input and create a list
    val inputList = input.toVector

    // Initialize the results list for the open() block
    sorted = Vector[Tuple]()

    // Sort the input list using the comparator function and store in a list
    sorted = inputList.sortWith(comparator)

    // Initialize the count based on the option offset
    if (offset.isEmpty) {
      count = 0
    } else {
      count = offset.get
    }
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    // Create a variable for the end-case to stop the next() block
    var end_count = sorted.length
    // Assign the end count based on the availability of the offset and fetch inputs
    (offset.nonEmpty, fetch.nonEmpty) match {
      case (true, true) => end_count = offset.get + fetch.get
      case (false, true) => end_count = fetch.get
      case _ => end_count = sorted.length
    }

    if (count == sorted.length || count == end_count) {
      // Return nothing if the required length of the list has been traversed
      NilTuple
    } else {
      // Update the count and return the current entry (-1 because update comes first)
      count += 1
      Option(sorted(count-1))
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    // Empty
  }
}
