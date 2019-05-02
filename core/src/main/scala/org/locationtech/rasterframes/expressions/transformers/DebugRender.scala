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

package org.locationtech.rasterframes.expressions.transformers

import org.locationtech.rasterframes.expressions.UnaryRasterOp
import org.locationtech.rasterframes.util.TileAsMatrix
import geotrellis.raster.Tile
import geotrellis.raster.render.ascii.AsciiArtEncoder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.sql.{Column, TypedColumn}
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.rasterframes.model.TileContext

abstract class DebugRender(asciiArt: Boolean) extends UnaryRasterOp
   with CodegenFallback with Serializable {
   override def dataType: DataType = StringType

   override protected def eval(tile: Tile, ctx: Option[TileContext]): Any = {
     UTF8String.fromString(if (asciiArt)
       s"\n${tile.renderAscii(AsciiArtEncoder.Palette.NARROW)}\n"
     else
       s"\n${tile.renderMatrix(6)}\n"
     )
   }
}

object DebugRender {
  import org.locationtech.rasterframes.encoders.StandardEncoders.PrimitiveEncoders.stringEnc

  @ExpressionDescription(
    usage = "_FUNC_(tile) - Coverts the contents of the given tile an ASCII art string rendering",
    arguments = """
  Arguments:
    * tile - tile to render"""
  )
  case class RenderAscii(child: Expression) extends DebugRender(true) {
    override def nodeName: String = "rf_render_ascii"
  }
  object RenderAscii {
    def apply(tile: Column): TypedColumn[Any, String] =
      new Column(RenderAscii(tile.expr)).as[String]
  }

  @ExpressionDescription(
    usage = "_FUNC_(tile) - Coverts the contents of the given tile to a 2-d array of numberic values",
    arguments = """
  Arguments:
    * tile - tile to render"""
  )
  case class RenderMatrix(child: Expression) extends DebugRender(false) {
    override def nodeName: String = "rf_render_matrix"
  }
  object RenderMatrix {
    def apply(tile: Column): TypedColumn[Any, String] =
      new Column(RenderMatrix(tile.expr)).as[String]
  }
}
