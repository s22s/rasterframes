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

package org.locationtech.rasterframes.expressions.aggstats

import org.locationtech.rasterframes.expressions.UnaryRasterAggregate
import org.locationtech.rasterframes.expressions.tilestats.{DataCells, Sum}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, _}
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
case class CellMeanAggregate(child: Expression) extends UnaryRasterAggregate {
  override def nodeName: String = "rf_agg_mean"

  private lazy val sum =
    AttributeReference("sum", DoubleType, false, Metadata.empty)()
  private lazy val count =
    AttributeReference("count", LongType, false, Metadata.empty)()

  override lazy val aggBufferAttributes = Seq(sum, count)

  override val initialValues = Seq(
    Literal(0.0),
    Literal(0L)
  )

  // Cant' figure out why we can't just use the Expression directly
  // this is necessary to properly handle null rows. For example,
  // if we use `tilestats.Sum` directly, we get an NPE when the stage is executed.
  private val DataCellCounts = tileOpAsExpression("rf_data_cells", DataCells.op)
  private val SumCells = tileOpAsExpression("sum_cells", Sum.op)

  override val updateExpressions = Seq(
    // TODO: Figure out why this doesn't work. See above.
    //If(IsNull(child), sum , Add(sum, Sum(child))),
    If(IsNull(child), sum , Add(sum, SumCells(child))),
    If(IsNull(child), count, Add(count, DataCellCounts(child)))
  )

  override val mergeExpressions = Seq(
    sum.left + sum.right,
    count.left + count.right
  )

  override val evaluateExpression = sum / new Cast(count, DoubleType)

  override def dataType = DoubleType
}

object CellMeanAggregate {
  import org.locationtech.rasterframes.encoders.StandardEncoders.PrimitiveEncoders.doubleEnc
  /** Computes the column aggregate mean. */
  def apply(tile: Column): TypedColumn[Any, Double] =
    new Column(new CellMeanAggregate(tile.expr).toAggregateExpression()).as[Double]
}



