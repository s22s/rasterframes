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

package astraea.spark.rasterframes.expressions.aggstats

import astraea.spark.rasterframes.expressions.tilestats.Sum
import astraea.spark.rasterframes.functions._
import geotrellis.raster.{Tile, isData}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, _}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.rf.TileUDT
import org.apache.spark.sql.types.{DoubleType, LongType, Metadata}
import org.apache.spark.sql.{Column, TypedColumn}

/**
 * Cell mean aggregate function
 * 
 * @since 10/5/17
 */
@ExpressionDescription(
  usage = "_FUNC_(tile) - Computes the mean of all cell values.",
  examples = """
    Examples:
      > SELECT _FUNC_(tile);
         ....
  """)
case class CellMeanAggregate(child: Expression) extends DeclarativeAggregate {
  private val TileType = new TileUDT()
  override def nodeName: String = "agg_mean"

  private lazy val sum =
    AttributeReference("sum", DoubleType, false, Metadata.empty)()
  private lazy val count =
    AttributeReference("count", LongType, false, Metadata.empty)()

  override lazy val aggBufferAttributes = Seq(sum, count)

  val initialValues = Seq(
    Literal(0.0),
    Literal(0L)
  )

  private val dataCellCounts = udf(dataCells)
  // Cant' figure out why this is necessary to properly handle
  // null rows. If we use `tilestats.Sum`
  private val tileSum: Tile ⇒ Double = (t: Tile) ⇒ {
    var sum: Double = 0.0
    t.foreachDouble(z ⇒ if(isData(z)) sum = sum + z)
    sum
  }
  private val SumCells = (tileCol: Expression ) => ScalaUDF(
    tileSum, DoubleType, Seq(tileCol), Seq(TileType), Some("cell_sum")
  )

  val updateExpressions = Seq(
    // TODO: Figure out why this doesn't work. See above.
    //If(IsNull(child), sum , Add(sum, Sum(child))),
    If(IsNull(child), sum , Add(sum, SumCells(child))),
    If(IsNull(child), count, Add(count, dataCellCounts(new Column(child)).expr))
  )

  val mergeExpressions = Seq(
    sum.left + sum.right,
    count.left + count.right
  )

  val evaluateExpression = sum / new Cast(count, DoubleType)

  def inputTypes = Seq(TileType)

  def nullable = child.nullable

  def dataType = DoubleType

  def children = Seq(child)
}

object CellMeanAggregate {
  import astraea.spark.rasterframes.encoders.SparkDefaultEncoders._
  /** Computes the column aggregate mean. */
  def apply(tile: Column): TypedColumn[Any, Double] =
    new Column(new CellMeanAggregate(tile.expr).toAggregateExpression()).as[Double]
}



