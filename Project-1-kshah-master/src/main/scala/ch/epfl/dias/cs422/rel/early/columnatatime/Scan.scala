package ch.epfl.dias.cs422.rel.early.columnatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import ch.epfl.dias.cs422.helpers.store.{ColumnStore, ScannableTable, Store}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Scan]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator]]
  */
class Scan protected(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    tableToStore: ScannableTable => Store
) extends skeleton.Scan[
      ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator
    ](cluster, traitSet, table)
    with ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator {

  protected val scannable: ColumnStore = tableToStore(
    table.unwrap(classOf[ScannableTable])
  ).asInstanceOf[ColumnStore]

  // Scan does not prune tuples.

  /**
   * @inheritdoc
   */
  def execute(): IndexedSeq[HomogeneousColumn] = {
    // Create empty variables to store the scanned sequence and the selection vector
    var scan_seq = IndexedSeq.empty[HomogeneousColumn]
    var sel_vector = IndexedSeq.empty[Boolean]

    // Get the number of data rows and columns
    val num_rows = table.getRowCount.toInt
    val num_cols = getRowType.getFieldCount

    // Append scannable columns to the sequence
    for (i <- 0 until num_cols) {
      val next_col = scannable.asInstanceOf[ColumnStore].getColumn(i)
      scan_seq = scan_seq :+ next_col
    }

    // Fill the selection vector with all 'true' entries
    for (i <- 0 until num_rows) {
      sel_vector = sel_vector :+ true
    }

    // Return the scanned sequence along with the selection vector
    scan_seq = scan_seq :+ sel_vector
    scan_seq
  }
}
