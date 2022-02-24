package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, Tuple}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexNode

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Project]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Project protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    projects: java.util.List[_ <: RexNode],
    rowType: RelDataType
) extends skeleton.Project[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](input, projects, rowType)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

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
  override def next(): Option[Tuple] = {
    // Get the next input tuple
    val next_tuple = input.next()

    if (next_tuple == NilTuple) {
      // Return nothing if there are no more available tuples
      NilTuple
    } else {
      // Return the 'evaluated' tuple for every next tuple
      Option(evaluator(next_tuple.get))
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
