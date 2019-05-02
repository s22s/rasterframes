/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2019 Astraea, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *     [http://www.apache.org/licenses/LICENSE-2.0]
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.locationtech.rasterframes.expressions

import java.nio.ByteBuffer

import org.locationtech.rasterframes.expressions.TileAssembler.TileBuffer
import org.locationtech.rasterframes.util._
import geotrellis.raster.{DataType => _, _}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, ImplicitCastInputTypes}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, TypedColumn}
import spire.syntax.cfor._
import org.locationtech.rasterframes.TileType

/**
 * Aggregator for reassembling tiles from from exploded form
 *
 * @since 9/24/17
 */
@ExpressionDescription(
  usage = "_FUNC_(colIndex, rowIndex, cellValue, tileCols, tileRows) - Assemble tiles from set of column and row indices and cell values.",
  arguments = """
  Arguments:
    * colIndex - column to place the cellValue in the generated tile
    * rowIndex - row to place the cellValue in the generated tile
    * cellValue - numeric value to place in the generated tile at colIndex and rowIndex
    * tileCols - number of columns in the generated tile
    * tileRows - number of rows in the generated tile""",
  examples = """
  Examples:
    > SELECT _FUNC_(column_index, row_index, cell0, 10, 10) as tile;
       ...
    > SELECT _FUNC_(column_index, row_index, tile, 10, 10) as tile2
      FROM (SELECT rf_explode_tiles(rf_make_constant_tile(4, 10, 10, 'int8raw')) as tile)
      ...
             """
)
case class TileAssembler(
  colIndex: Expression,
  rowIndex: Expression,
  cellValue: Expression,
  tileCols: Expression,
  tileRows: Expression,
  mutableAggBufferOffset: Int = 0,
  inputAggBufferOffset: Int = 0)
    extends TypedImperativeAggregate[TileBuffer] with ImplicitCastInputTypes {

  def this(colIndex: Expression,
           rowIndex: Expression,
           cellValue: Expression,
           tileCols: Expression,
           tileRows: Expression) = this(colIndex, rowIndex, cellValue, tileCols, tileRows, 0, 0)

  override def children: Seq[Expression] = Seq(colIndex, rowIndex, cellValue, tileCols, tileRows)

  override def inputTypes = Seq(ShortType, ShortType, DoubleType, ShortType, ShortType)

  override def prettyName: String = "rf_assemble_tiles"

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def nullable: Boolean = true

  override def dataType: DataType = TileType

  override def createAggregationBuffer(): TileBuffer = new TileBuffer(Array.empty)

  @inline
  private def toIndex(col: Int, row: Int, tileCols: Short): Int = row * tileCols + col

  override def update(inBuf: TileBuffer, input: InternalRow): TileBuffer = {
    val tc = tileCols.eval(input).asInstanceOf[Short]
    val tr = tileRows.eval(input).asInstanceOf[Short]

    val buffer = if (inBuf.isEmpty) {
      TileBuffer(tc, tr)
    } else inBuf

    val col = colIndex.eval(input).asInstanceOf[Short]
    require(col < tc, s"`tileCols` is $tc, but received index value $col")
    val row = rowIndex.eval(input).asInstanceOf[Short]
    require(row < tr, s"`tileRows` is $tr, but received index value $row")

    val cell = cellValue.eval(input).asInstanceOf[Double]
    buffer.cellBuffer.put(toIndex(col, row, tc), cell)
    buffer
  }

  override def merge(inBuf: TileBuffer, input: TileBuffer): TileBuffer = {

    val buffer = if (inBuf.isEmpty) {
      val (cols, rows) = input.tileSize
      TileBuffer(cols, rows)
    } else inBuf

    val (tileCols, tileRows) = buffer.tileSize
    val left = buffer.cellBuffer
    val right = input.cellBuffer
    cfor(0)(_ < tileRows, _ + 1) { row =>
      cfor(0)(_ < tileCols, _ + 1) { col =>
        val cell: Double = right.get(toIndex(col, row, tileCols))
        if (isData(cell)) {
          left.put(toIndex(col, row, tileCols), cell)
        }
      }
    }
    buffer
  }

  override def eval(buffer: TileBuffer): InternalRow = {
    // TODO: figure out how to eliminate copies here.
    val result = buffer.cellBuffer
    val length = result.capacity()
    val cells = Array.ofDim[Double](length)
    result.get(cells)
    val (tileCols, tileRows) = buffer.tileSize
    val tile = ArrayTile(cells, tileCols.toInt, tileRows.toInt)
    TileType.serialize(tile)
  }

  override def serialize(buffer: TileBuffer): Array[Byte] = buffer.serialize()
  override def deserialize(storageFormat: Array[Byte]): TileBuffer = new TileBuffer(storageFormat)
}

object TileAssembler {
  import org.locationtech.rasterframes.encoders.StandardEncoders._

  def apply(
    columnIndex: Column,
    rowIndex: Column,
    cellData: Column,
    tileCols: Column,
    tileRows: Column): TypedColumn[Any, Tile] =
    new Column(new TileAssembler(columnIndex.expr, rowIndex.expr, cellData.expr, tileCols.expr,
        tileRows.expr)
        .toAggregateExpression())
      .as(cellData.columnName)
      .as[Tile]

  private val indexPad = 2 * java.lang.Short.BYTES

  class TileBuffer(val storage: Array[Byte]) {

    def isEmpty = storage.isEmpty

    def cellBuffer = ByteBuffer.wrap(storage, 0, storage.length - indexPad).asDoubleBuffer()
    private def indexBuffer =
      ByteBuffer.wrap(storage, storage.length - indexPad, indexPad).asShortBuffer()

    def reset(): Unit = {
      val cells = cellBuffer
      val length = cells.capacity()
      cfor(0)(_ < length, _ + 1) { idx =>
        cells.put(idx, doubleNODATA)
      }
    }

    def serialize(): Array[Byte] = storage

    def tileSize: (Short, Short) = {
      val indexes = indexBuffer
      (indexes.get(0), indexes.get(1))
    }

  }
  object TileBuffer {
    def apply(tileCols: Short, tileRows: Short): TileBuffer = {
      val cellPad = tileCols * tileRows * java.lang.Double.BYTES
      val buf = new TileBuffer(Array.ofDim[Byte](cellPad + indexPad))
      buf.reset()
      buf.indexBuffer.put(0, tileCols).put(1, tileRows)
      buf
    }
  }

}
