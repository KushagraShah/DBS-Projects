package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Elem, NilTuple, Tuple}
import ch.epfl.dias.cs422.helpers.store.rle.RLEStore
import ch.epfl.dias.cs422.helpers.store.{ScannableTable, Store}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}

import scala.collection.convert.ImplicitConversions.`seq AsJavaList`

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Scan]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Scan protected (
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    tableToStore: ScannableTable => Store
) extends skeleton.Scan[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](cluster, traitSet, table)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  protected val scannable: Store = tableToStore(
    table.unwrap(classOf[ScannableTable])
  )

//  /**
//   * Helper function (you do not have to use it or implement it)
//   * It's purpose is to show how to convert the [[scannable]] to a
//   * specific [[Store]].
//   *
//   * @param rowId row number (starting from 0)
//   * @return the row as a Tuple
//   */
//  private def getRow(rowId: Int): Tuple = {
//    scannable match {
//      case rleStore: RLEStore =>
//        /**
//         * For this project, it's safe to assume scannable will always
//         * be a [[RLEStore]].
//         */
//        ???
//    }
//  }

  // Create an empty data buffer to store the results
  private var data_buff = scala.collection.mutable.IndexedBuffer.empty[Tuple]
  // Initialize a count for the next() block
  var count = 0

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    // Initialize the data buffer for open()
    data_buff = scala.collection.mutable.IndexedBuffer.empty[Tuple]
    // Initialize count for next()
    count = 0

    // Get the number of rows and columns
    val num_cols = getRowType.getFieldCount
    val num_rows = table.getRowCount.toInt

    // Fill the data buffer with empty lists (to be filled later)
    for(i <- 0 until num_rows) {
      val tmp = Vector.empty[Elem]
      data_buff = data_buff :+ tmp
    }

    // Get the scannable RLE Store object
    val scanRLE = scannable.asInstanceOf[RLEStore]

    // 1. Iterate over all columns ('fields')
    for(colId <- 0 until num_cols) {
      // Fetch the RLE column
      val RLE_Col = scanRLE.getRLEColumn(colId)
      // Initialize a counter for rows
      var rowId = 0

      // 2. Iterate over all entries in the RLE column
      for(entry <- RLE_Col) {
        // 3. Iterate to decompress the RLE Column
        for(i <- 0 until entry.length.toInt) {
          // Add the entry value for each row
          data_buff(rowId) = data_buff(rowId) :+ entry.value.get(0)
          rowId += 1
        }
      }
    }
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    if(count == data_buff.length) {
      // Return nothing if the entire buffer has been traversed
      NilTuple
    } else {
      // Update the count and return the current entry (-1 because update comes first)
      count += 1
      Option(data_buff(count-1))
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    // Empty
  }
}
