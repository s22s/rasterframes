/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2018 Astraea, Inc.
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

package org.locationtech.rasterframes.ref

import org.locationtech.rasterframes._
import org.locationtech.rasterframes.expressions.accessors._
import org.locationtech.rasterframes.expressions.transformers._
import RasterRef.RasterRefTile
import geotrellis.raster.Tile
import geotrellis.vector.Extent
import org.apache.spark.sql.Encoders
import org.locationtech.rasterframes.TestEnvironment

/**
 *
 *
 * @since 8/22/18
 */
//noinspection TypeAnnotation
class RasterRefSpec extends TestEnvironment with TestData {

  def sub(e: Extent) = {
    val c = e.center
    val w = e.width
    val h = e.height
    Extent(c.x, c.y, c.x + w * 0.01, c.y + h * 0.01)
  }

  trait Fixture {
    val src = RasterSource(remoteCOGSingleband1)
    val fullRaster = RasterRef(src, 0, None)
    val subExtent = sub(src.extent)
    val subRaster = RasterRef(src, 0, Some(subExtent))
  }

  import spark.implicits._

  implicit val enc = Encoders.tuple(Encoders.scalaInt, RasterRef.rrEncoder)
  describe("GetCRS Expression") {
    it("should read from RasterRef") {
      new Fixture {
        val ds = Seq((1, fullRaster)).toDF("index", "ref")
        val crs = ds.select(GetCRS($"ref"))
        assert(crs.count() === 1)
        assert(crs.first() !== null)
      }
    }
    it("should read from sub-RasterRef") {
      new Fixture {
        val ds = Seq((1, subRaster)).toDF("index", "ref")
        val crs = ds.select(GetCRS($"ref"))
        assert(crs.count() === 1)
        assert(crs.first() !== null)
      }
    }
  }

  describe("GetDimensions Expression") {
    it("should read from RasterRef") {
      new Fixture {
        val ds = Seq((1, fullRaster)).toDF("index", "ref")
        val dims = ds.select(GetDimensions($"ref"))
        assert(dims.count() === 1)
        assert(dims.first() !== null)
      }
    }
    it("should read from sub-RasterRef") {
      new Fixture {
        val ds = Seq((1, subRaster)).toDF("index", "ref")
        val dims = ds.select(GetDimensions($"ref"))
        assert(dims.count() === 1)
        assert(dims.first() !== null)
      }
    }

    it("should read from RasterRefTile") {
      new Fixture {
        val ds = Seq((1, RasterRefTile(fullRaster): Tile)).toDF("index", "ref")
        val dims = ds.select(GetDimensions($"ref"))
        assert(dims.count() === 1)
        assert(dims.first() !== null)
      }
    }
    it("should read from sub-RasterRefTiles") {
      new Fixture {
        val ds = Seq((1, RasterRefTile(subRaster): Tile)).toDF("index", "ref")
        val dims = ds.select(GetDimensions($"ref"))
        assert(dims.count() === 1)
        assert(dims.first() !== null)
      }
    }
  }

  describe("rf_extent") {
    it("should read from RasterRef") {
      import spark.implicits._
      new Fixture {
        val ds = Seq((1, fullRaster)).toDF("index", "ref")
        val extent = ds.select(rf_extent($"ref"))
        assert(extent.count() === 1)
        assert(extent.first() !== null)
      }
    }
    it("should read from sub-RasterRef") {
      import spark.implicits._
      new Fixture {
        val ds = Seq((1, subRaster)).toDF("index", "ref")
        val extent = ds.select(rf_extent($"ref"))
        assert(extent.count() === 1)
        assert(extent.first() !== null)
      }
    }
  }

  describe("RasterRef") {
    it("should delay reading") {
      new Fixture {
        assert(subRaster.cellType === src.cellType)
      }
    }
    it("should support subextents") {
      new Fixture {
        assert(subRaster.cols.toDouble === src.cols * 0.01 +- 2.0)
        assert(subRaster.rows.toDouble === src.rows * 0.01 +- 2.0)
        //subRaster.tile.rescale(0, 255).renderPng().write("target/foo1.png")
      }
    }
    it("should be realizable") {
      new Fixture {
        assert(subRaster.tile.statistics.map(_.dataCells) === Some(subRaster.cols * subRaster.rows))
      }
    }

    it("should Java serialize") {
      new Fixture {
        import java.io._

        val buf = new java.io.ByteArrayOutputStream()
        val out = new ObjectOutputStream(buf)
        out.writeObject(subRaster)
        out.close()
        val data = buf.toByteArray
        val in = new ObjectInputStream(new ByteArrayInputStream(data))
        val recovered = in.readObject()
        assert(subRaster === recovered)
      }
    }
  }

  describe("CreateRasterRefs") {
    it("should convert and expand RasterSource") {
      new Fixture {
        import spark.implicits._
        val df = Seq(src).toDF("src")
        val refs = df.select(RasterSourceToRasterRefs(Some(NOMINAL_TILE_DIMS), Seq(0), $"src"))
        assert(refs.count() > 1)
      }
    }

    it("should work with tile realization") {
      new Fixture {
        import spark.implicits._
        val df = Seq(src).toDF("src")
        val refs = df.select(RasterSourceToRasterRefs(Some(NOMINAL_TILE_DIMS), Seq(0), $"src"))
        assert(refs.count() > 1)
      }
    }
  }
}
