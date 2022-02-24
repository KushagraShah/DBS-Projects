package ch.epfl.dias.cs422.rel.early.columnatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexNode

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Project]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator]]
  */
class Project protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
    projects: java.util.List[_ <: RexNode],
    rowType: RelDataType
) extends skeleton.Project[
      ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator
    ](input, projects, rowType)
    with ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator {

  /**
    * Function that, when given a (non-NilTuple) tuple produced by the [[input]] operator,
    * it returns a new tuple composed of the evaluated projections [[projects]]
   *
   * FIXME
    */
  lazy val evals: IndexedSeq[IndexedSeq[HomogeneousColumn] => HomogeneousColumn] =
    projects.asScala.map(e => map(e, input.getRowType, isFilterCondition = false)).toIndexedSeq

  // Project does not prune tuples. False entries are untouched.

  /**
   * @inheritdoc
   */
  def execute(): IndexedSeq[HomogeneousColumn] = {
    // Get the input sequence of homogenous columns
    val inp_seq = input.execute()
    // Return empty sequence in case of empty input
    if(inp_seq.isEmpty) {
      return IndexedSeq[HomogeneousColumn]()
    }

    // Get the projected sequence using the map-based evaluator
    var proj_seq = evals.map(in => in(inp_seq.init))
    // Append the selection vectors to the result
    proj_seq = proj_seq :+ inp_seq.last

    // Return the projected sequence
    proj_seq
  }
}
