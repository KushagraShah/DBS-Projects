package ch.epfl.dias.cs422.rel.early.volcano.rle

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilRLEentry, NilTuple, RLEentry}

/**
  * @inheritdoc
  *
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Reconstruct]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator]]
  */
class Reconstruct protected (
    left: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator,
    right: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
) extends skeleton.Reconstruct[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
    ](left, right)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator {

  // Initialize RLE entries to track the flow
  var curr_left : Option[RLEentry] = NilRLEentry
  var curr_right : Option[RLEentry] = NilRLEentry

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    // Initialize the inputs
    left.open()
    right.open()

    // Get the first set of input entries
    curr_left = left.next()
    curr_right = right.next()
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[RLEentry] = {
    // Iterate infinitively
    while(true) {
      // Return nothing if any of the entries is empty
      if (curr_left == NilRLEentry || curr_right == NilRLEentry) return NilRLEentry

      // Find critical points to check overlap
      val max_SID = Math.max(curr_left.get.startVID, curr_right.get.startVID)
      val min_EID = Math.min(curr_left.get.endVID, curr_right.get.endVID)
      // Calculate overlapping features (to return)
      val nSID = max_SID
      val nLen = min_EID-max_SID+1
      val nVal = curr_left.get.value ++ curr_right.get.value

      // Update entries if they are completely overlapped
      if (curr_left.get.endVID == min_EID) curr_left = left.next()
      if (curr_right.get.endVID == min_EID) curr_right = right.next()

      // Check overlap: [max_SID < min_EID+1] <--> [max_SID <= min_EID]
      if (max_SID <= min_EID) {
        // Return the overlapping part of the entries
        return Option(RLEentry(nSID, nLen, nVal))
      }
    }

    // Code will never reach here
    NilRLEentry
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    // De-initialize the inputs
    left.close()
    right.close()
  }
}
