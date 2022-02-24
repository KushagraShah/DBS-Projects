package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexNode

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Project]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator]]
  */
class Project protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
    projects: java.util.List[_ <: RexNode],
    rowType: RelDataType
) extends skeleton.Project[
      ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
    ](input, projects, rowType)
    with ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator {

  /**
    * Function that, when given a (non-NilTuple) tuple produced by the [[input]] operator,
    * it returns a new tuple composed of the evaluated projections [[projects]]
    */
  lazy val evaluator: Tuple => Tuple = {
    eval(projects.asScala.toIndexedSeq, input.getRowType)
  }

  // Project does not prune tuples. False entries are untouched.

  /**
   * @inheritdoc
   */
  def execute(): IndexedSeq[Column] = {
    // Create an empty projected sequence
    var proj_seq = IndexedSeq.empty[IndexedSeq[Elem]]
    // Get the row sequence by taking a transpose
    val row_seq = input.toIndexedSeq.transpose

    // Iterate over each row
    for(row <- row_seq) {
      // Get the projected row, even for the 'false' entries
      // This needs to be done to maintain the correct size
      val proj_row = evaluator(row.init) :+ row.last
      // Append the projection to the sequence
      proj_seq = proj_seq :+ proj_row.toVector
    }

    // Return the transpose of the sequence as the result
    proj_seq.transpose
  }
}
