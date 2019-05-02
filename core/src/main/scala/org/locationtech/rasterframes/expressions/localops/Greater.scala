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
package org.locationtech.rasterframes.expressions.localops

import org.locationtech.rasterframes._
import org.locationtech.rasterframes.expressions.BinaryLocalRasterOp
import geotrellis.raster.Tile
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Column, TypedColumn}

@ExpressionDescription(
  usage = "_FUNC_(lhs, rhs) - Performs cell-wise greater-than (>) test between two tiles.",
  arguments = """
  Arguments:
    * lhs - first tile argument
    * rhs - second tile argument""",
  examples = """
  Examples:
    > SELECT _FUNC_(tile1, tile2);
       ..."""
)
case class Greater(left: Expression, right: Expression) extends BinaryLocalRasterOp with CodegenFallback  {
  override val nodeName: String = "rf_local_greater"
  override protected def op(left: Tile, right: Tile): Tile = left.localGreater(right)
  override protected def op(left: Tile, right: Double): Tile = left.localGreater(right)
  override protected def op(left: Tile, right: Int): Tile = left.localGreater(right)
}

object Greater {
  def apply(left: Column, right: Column): TypedColumn[Any, Tile] =
    new Column(Greater(left.expr, right.expr)).as[Tile]

  def apply[N: Numeric](tile: Column, value: N): TypedColumn[Any, Tile] =
    new Column(Greater(tile.expr, lit(value).expr)).as[Tile]
}