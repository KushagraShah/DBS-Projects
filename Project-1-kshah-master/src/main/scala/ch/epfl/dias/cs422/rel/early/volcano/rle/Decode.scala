package ch.epfl.dias.cs422.rel.early.volcano.rle

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilRLEentry, NilTuple, RLEentry, Tuple}

/**
  * @inheritdoc
  *
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Decode]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator]]
  */
class Decode protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
) extends skeleton.Decode[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
      ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
    ](input)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  // Initialize an RLE entry and counts
  var curr_entry : Option[RLEentry] = NilRLEentry
  var total : Long = 0
  var curr : Long = 0

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    // Initialize the input
    input.open()
    // Initialize the entry and counts for open()
    curr_entry = NilRLEentry
    total = 0
    curr = 0
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    // For the first encounter with an RLE entry
    if(curr == total) {
      // Get the next input entry, return nothing if no more entries are available
      val next_entry = input.next().getOrElse(return NilTuple)
      // Update the entry and the counts
      curr_entry = Option(next_entry)
      total = next_entry.length
      curr = 0
    }

    // For the continued encounter with an RLE entry
    if(curr < total) {
      // Update the count and return the current value
      curr += 1
      return Option(curr_entry.get.value)
    }

    // Code will never reach here
    NilTuple
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    // De-initialize the input
    input.close()
  }
}
