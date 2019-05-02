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

package org.locationtech.rasterframes.model

import geotrellis.raster.{ArrayTile, ConstantTile, Tile}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.{BinaryType, StructField, StructType}
import org.locationtech.rasterframes.encoders.CatalystSerializer._
import org.locationtech.rasterframes.encoders.{CatalystSerializer, CatalystSerializerEncoder}
import org.locationtech.rasterframes.ref.RasterRef
import org.locationtech.rasterframes.ref.RasterRef.RasterRefTile
import org.locationtech.rasterframes.tiles.ProjectedRasterTile.ConcreteProjectedRasterTile

/** Represents the union of binary cell datas or a reference to the data.*/
case class Cells(data: Either[Array[Byte], RasterRef]) {
  def isRef: Boolean = data.isRight

  /** Convert cells into either a RasterRefTile or an ArrayTile. */
  def toTile(ctx: TileDataContext): Tile = {
    data.fold(
      bytes => ArrayTile.fromBytes(bytes, ctx.cellType, ctx.dimensions.cols, ctx.dimensions.rows),
      ref => RasterRefTile(ref)
    )
  }
}

object Cells {

  /** Extracts the Cells from a Tile. */
  def apply(t: Tile): Cells = {
    t match {
      case prt: ConcreteProjectedRasterTile =>
        apply(prt.t)
      case ref: RasterRefTile =>
        Cells(Right(ref.rr))
      case const: ConstantTile =>
        // TODO: Create mechanism whereby constant tiles aren't expanded.
        Cells(Left(const.toArrayTile().toBytes))
      case o =>
        Cells(Left(o.toBytes))
    }
  }

  implicit def cellsSerializer: CatalystSerializer[Cells] = new CatalystSerializer[Cells] {
    override def schema: StructType =
      StructType(
        Seq(
          StructField("cells", BinaryType, true),
          StructField("ref", schemaOf[RasterRef], true)
        ))
    override protected def to[R](t: Cells, io: CatalystSerializer.CatalystIO[R]): R = io.create(
      t.data.left.getOrElse(null),
      t.data.right.map(rr => io.to(rr)).right.getOrElse(null)
    )
    override protected def from[R](t: R, io: CatalystSerializer.CatalystIO[R]): Cells = {
      if (!io.isNullAt(t, 0))
        Cells(Left(io.getByteArray(t, 0)))
      else if (!io.isNullAt(t, 1))
        Cells(Right(io.get[RasterRef](t, 1)))
      else throw new IllegalArgumentException("must be eithe cell data or a ref, but not null")
    }
  }

  implicit def encoder: ExpressionEncoder[Cells] = CatalystSerializerEncoder[Cells]()
}
