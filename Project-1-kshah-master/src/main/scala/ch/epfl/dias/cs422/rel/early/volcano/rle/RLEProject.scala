package ch.epfl.dias.cs422.rel.early.volcano.rle

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilRLEentry, RLEentry, Tuple}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexNode

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  *
  * Note that this in an RLE operator, so it receives [[ch.epfl.dias.cs422.helpers.rel.RelOperator.RLEentry]] and
  * produces [[ch.epfl.dias.cs422.helpers.rel.RelOperator.RLEentry]]
  *
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Project]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator]]
  */
class RLEProject protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator,
    projects: java.util.List[_ <: RexNode],
    rowType: RelDataType
) extends skeleton.Project[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
    ](input, projects, rowType)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator {

  /**
    * Function that, when given a (non-NilTuple) tuple produced by the [[input]] operator,
    * it returns a new tuple composed of the evaluated projections [[projects]]
    */
  lazy val evaluator: Tuple => Tuple =
    eval(projects.asScala.toIndexedSeq, input.getRowType)

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    // Initialize the input
    input.open()
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[RLEentry] = {
    // Get the next RLE entry
    val next_entry = input.next()

    if (next_entry == NilRLEentry) {
      // Return nothing if there are no more available entries
      NilRLEentry
    } else {
      // Fetch the parameters from the next tuple to be returned
      val nSID = next_entry.get.startVID
      val nLen = next_entry.get.length
      val nVal = evaluator(next_entry.get.value)
      // Return the RLE entry with the 'evaluated' tuple
      Option(RLEentry(nSID, nLen, nVal))
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    // De-initialize the input
    input.close()
  }
}
