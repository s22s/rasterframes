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

package org.locationtech.rasterframes.util

import org.locationtech.rasterframes.expressions.TileAssembler
import org.locationtech.rasterframes.expressions.accessors._
import org.locationtech.rasterframes.expressions.aggstats._
import org.locationtech.rasterframes.expressions.generators._
import org.locationtech.rasterframes.expressions.localops._
import org.locationtech.rasterframes.expressions.tilestats._
import org.locationtech.rasterframes.expressions.transformers._
import org.locationtech.rasterframes.stats._
import org.locationtech.rasterframes.{functions => F}
import org.locationtech.jts.geom.Geometry
import geotrellis.proj4.CRS
import geotrellis.raster.mapalgebra.local.LocalTileBinaryOp
import geotrellis.raster.{CellType, Tile}
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.functions.{lit, udf}
import org.apache.spark.sql.rf.VersionShims._
import org.apache.spark.sql.{Column, SQLContext, TypedColumn, rf}

/**
 * UDFs for working with Tiles in Spark DataFrames.
 *
 * @since 4/3/17
 */
object ZeroSevenCompatibilityKit {
  import org.locationtech.rasterframes.encoders.StandardEncoders._

  trait RasterFunctions {
    private val delegate = new org.locationtech.rasterframes.RasterFunctions {}
    // format: off
    /** Create a row for each cell in Tile. */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def explodeTiles(cols: Column*): Column = delegate.rf_explode_tiles(cols: _*)

    /** Create a row for each cell in Tile with random sampling and optional seed. */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def explodeTilesSample(sampleFraction: Double, seed: Option[Long], cols: Column*): Column =
      ExplodeTiles(sampleFraction, seed, cols)

    /** Create a row for each cell in Tile with random sampling (no seed). */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def explodeTilesSample(sampleFraction: Double, cols: Column*): Column =
      ExplodeTiles(sampleFraction, None, cols)

    /** Query the number of (cols, rows) in a Tile. */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def tileDimensions(col: Column): Column = GetDimensions(col)

    @Experimental
    /** Convert array in `arrayCol` into a Tile of dimensions `cols` and `rows`*/
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def arrayToTile(arrayCol: Column, cols: Int, rows: Int) = withAlias("rf_array_to_tile", arrayCol)(
      udf[Tile, AnyRef](F.arrayToTile(cols, rows)).apply(arrayCol)
    )

    /** Create a Tile from a column of cell data with location indexes and preform cell conversion. */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def assembleTile(columnIndex: Column, rowIndex: Column, cellData: Column, tileCols: Int, tileRows: Int, ct: CellType): TypedColumn[Any, Tile] =
      convertCellType(TileAssembler(columnIndex, rowIndex, cellData, lit(tileCols), lit(tileRows)), ct).as(cellData.columnName).as[Tile]

    /** Create a Tile from  a column of cell data with location indexes. */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def assembleTile(columnIndex: Column, rowIndex: Column, cellData: Column, tileCols: Column, tileRows: Column): TypedColumn[Any, Tile] =
      TileAssembler(columnIndex, rowIndex, cellData, tileCols, tileRows)

    /** Extract the Tile's cell type */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def cellType(col: Column): TypedColumn[Any, CellType] = GetCellType(col)

    /** Change the Tile's cell type */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def convertCellType(col: Column, cellType: CellType): TypedColumn[Any, Tile] =
      SetCellType(col, cellType)

    /** Change the Tile's cell type */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def convertCellType(col: Column, cellTypeName: String): TypedColumn[Any, Tile] =
      SetCellType(col, cellTypeName)

    /** Convert a bounding box structure to a Geometry type. Intented to support multiple schemas. */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def boundsGeometry(bounds: Column): TypedColumn[Any, Geometry] = ExtentToGeometry(bounds)

    /** Assign a `NoData` value to the Tiles. */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def withNoData(col: Column, nodata: Double) = withAlias("withNoData", col)(
      udf[Tile, Tile](F.withNoData(nodata)).apply(col)
    ).as[Tile]

    /**  Compute the full column aggregate floating point histogram. */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def aggHistogram(col: Column): TypedColumn[Any, CellHistogram] = delegate.rf_agg_approx_histogram(col)

    /** Compute the full column aggregate floating point statistics. */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def aggStats(col: Column): TypedColumn[Any, CellStatistics] = delegate.rf_agg_stats(col)

    /** Computes the column aggregate mean. */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def aggMean(col: Column) = CellMeanAggregate(col)

    /** Computes the number of non-NoData cells in a column. */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def aggDataCells(col: Column): TypedColumn[Any, Long] = delegate.rf_agg_data_cells(col)

    /** Computes the number of NoData cells in a column. */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def aggNoDataCells(col: Column): TypedColumn[Any, Long] = delegate.rf_agg_no_data_cells(col)

    /** Compute the Tile-wise mean */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def tileMean(col: Column): TypedColumn[Any, Double] = delegate.rf_tile_mean(col)

    /** Compute the Tile-wise sum */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def tileSum(col: Column): TypedColumn[Any, Double] = delegate.rf_tile_sum(col)

    /** Compute the minimum cell value in tile. */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def tileMin(col: Column): TypedColumn[Any, Double] = delegate.rf_tile_min(col)

    /** Compute the maximum cell value in tile. */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def tileMax(col: Column): TypedColumn[Any, Double] = delegate.rf_tile_max(col)

    /** Compute TileHistogram of Tile values. */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def tileHistogram(col: Column): TypedColumn[Any, CellHistogram] = delegate.rf_tile_histogram(col)

    /** Compute statistics of Tile values. */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def tileStats(col: Column): TypedColumn[Any, CellStatistics] = delegate.rf_tile_stats(col)

    /** Counts the number of non-NoData cells per Tile. */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def dataCells(tile: Column): TypedColumn[Any, Long] = delegate.rf_data_cells(tile)

    /** Counts the number of NoData cells per Tile. */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def noDataCells(tile: Column): TypedColumn[Any, Long] = delegate.rf_no_data_cells(tile)

    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def isNoDataTile(tile: Column): TypedColumn[Any, Boolean] = delegate.rf_is_no_data_tile(tile)

    /** Compute cell-local aggregate descriptive statistics for a column of Tiles. */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localAggStats(col: Column): Column = delegate.rf_agg_local_stats(col)

    /** Compute the cell-wise/local max operation between Tiles in a column. */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localAggMax(col: Column): TypedColumn[Any, Tile] = delegate.rf_agg_local_max(col)

    /** Compute the cellwise/local min operation between Tiles in a column. */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localAggMin(col: Column): TypedColumn[Any, Tile] = delegate.rf_agg_local_min(col)

    /** Compute the cellwise/local mean operation between Tiles in a column. */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localAggMean(col: Column): TypedColumn[Any, Tile] = delegate.rf_agg_local_mean(col)

    /** Compute the cellwise/local count of non-NoData cells for all Tiles in a column. */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localAggDataCells(col: Column): TypedColumn[Any, Tile] = delegate.rf_agg_local_data_cells(col)

    /** Compute the cellwise/local count of NoData cells for all Tiles in a column. */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localAggNoDataCells(col: Column): TypedColumn[Any, Tile] = delegate.rf_agg_local_no_data_cells(col)

    /** Cellwise addition between two Tiles. */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localAdd(left: Column, right: Column): Column = delegate.rf_local_add(left, right)

    /** Cellwise addition of a scalar to a tile. */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localAddScalar[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] = delegate.rf_local_add(tileCol, value)

    /** Cellwise subtraction between two Tiles. */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localSubtract(left: Column, right: Column): Column = delegate.rf_local_subtract(left, right)

    /** Cellwise subtraction of a scalar from a tile. */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localSubtractScalar[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] = delegate.rf_local_subtract(tileCol, value)
    /** Cellwise multiplication between two Tiles. */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localMultiply(left: Column, right: Column): Column = delegate.rf_local_multiply(left, right)

    /** Cellwise multiplication of a tile by a scalar. */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localMultiplyScalar[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] = delegate.rf_local_multiply(tileCol, value)

    /** Cellwise division between two Tiles. */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localDivide(left: Column, right: Column): Column = delegate.rf_local_divide(left, right)

    /** Cellwise division of a tile by a scalar. */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localDivideScalar[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] = delegate.rf_local_divide(tileCol, value)
    /** Perform an arbitrary GeoTrellis `LocalTileBinaryOp` between two Tile columns. */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localAlgebra(op: LocalTileBinaryOp, left: Column, right: Column):
    TypedColumn[Any, Tile] =
      withAlias(opName(op), left, right)(
        udf[Tile, Tile, Tile](op.apply).apply(left, right)
      ).as[Tile]

    /** Compute the normalized difference of two tile columns */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def normalizedDifference(left: Column, right: Column): TypedColumn[Any, Tile] = delegate.rf_normalized_difference(left, right)

    /** Constructor for constant tile column */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def makeConstantTile(value: Number, cols: Int, rows: Int, cellType: String): TypedColumn[Any, Tile] =
      udf(() => F.makeConstantTile(value, cols, rows, cellType)).apply().as(s"constant_$cellType").as[Tile]

    /** Alias for column of constant tiles of zero */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def tileZeros(cols: Int, rows: Int, cellType: String = "float64"): TypedColumn[Any, Tile] =
      udf(() => F.tileZeros(cols, rows, cellType)).apply().as(s"zeros_$cellType").as[Tile]

    /** Alias for column of constant tiles of one */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def tileOnes(cols: Int, rows: Int, cellType: String = "float64"): TypedColumn[Any, Tile] =
      udf(() => F.tileOnes(cols, rows, cellType)).apply().as(s"ones_$cellType").as[Tile]

    /** Where the mask tile equals the mask value, replace values in the source tile with NODATA */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def maskByValue(sourceTile: Column, maskTile: Column, maskValue: Column): TypedColumn[Any, Tile] =
      delegate.rf_mask_by_value(sourceTile, maskTile, maskValue)

    /** Where the mask tile DOES NOT contain NODATA, replace values in the source tile with NODATA */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def inverseMask(sourceTile: Column, maskTile: Column): TypedColumn[Any, Tile] =
      delegate.rf_inverse_mask(sourceTile, maskTile)

    /** Reproject a column of geometry from one CRS to another. */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def reprojectGeometry(sourceGeom: Column, srcCRS: CRS, dstCRS: CRS): TypedColumn[Any, Geometry] =
      delegate.st_reproject(sourceGeom, srcCRS, dstCRS)

    /** Render Tile as ASCII string for debugging purposes. */
    @Experimental
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def renderAscii(col: Column): TypedColumn[Any, String] = delegate.rf_render_ascii(col)

    /** Cellwise less than value comparison between two tiles. */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localLess(left: Column, right: Column): TypedColumn[Any, Tile] =
      delegate.rf_local_less(left, right)


    /** Cellwise less than value comparison between a tile and a scalar. */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localLessScalar[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] = delegate.rf_local_less(tileCol, value)

    /** Cellwise less than or equal to value comparison between a tile and a scalar. */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localLessEqual(left: Column, right: Column): TypedColumn[Any, Tile]  = delegate.rf_local_less_equal(left, right)

    /** Cellwise less than or equal to value comparison between a tile and a scalar. */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localLessEqualScalar[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] = delegate.rf_local_less_equal(tileCol, value)

    /** Cellwise greater than value comparison between two tiles. */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localGreater(left: Column, right: Column): TypedColumn[Any, Tile] =
      delegate.rf_local_greater(left, right)

    /** Cellwise greater than value comparison between a tile and a scalar. */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localGreaterScalar[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] = delegate.rf_local_greater(tileCol, value)

    /** Cellwise greater than or equal to value comparison between two tiles. */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localGreaterEqual(left: Column, right: Column): TypedColumn[Any, Tile] = delegate.rf_local_greater_equal(left, right)

    /** Cellwise greater than or equal to value comparison between a tile and a scalar. */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localGreaterEqualScalar[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] = delegate.rf_local_greater_equal(tileCol, value)

    /** Cellwise equal to value comparison between two tiles. */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localEqual(left: Column, right: Column): TypedColumn[Any, Tile] = delegate.rf_local_equal(left, right)

    /** Cellwise equal to value comparison between a tile and a scalar. */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localEqualScalar[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] = delegate.rf_local_equal(tileCol, value)

    /** Cellwise inequality comparison between two tiles. */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localUnequal(left: Column, right: Column): TypedColumn[Any, Tile] = delegate.rf_local_unequal(left, right)

    /** Cellwise inequality comparison between a tile and a scalar. */
    @deprecated("Part of 0.7.x compatibility kit, to be removed after 0.8.x. Please use \"snake_case\" variant instead.", "0.8.0")
    def localUnequalScalar[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] = delegate.rf_local_unequal(tileCol, value)
  }

  def register(sqlContext: SQLContext): Unit = {

    /** Unary expression builder builder. */
    def ub[A, B](f: A => B)(a: Seq[A]): B = f(a.head)
    /** Binary expression builder builder. */
    def bb[A, B](f: (A, A) => B)(a: Seq[A]): B = f(a.head, a.last)

    // Expression-oriented functions have a different registration scheme
    // Currently have to register with the `builtin` registry due to Spark data hiding.
    val registry: FunctionRegistry = rf.registry(sqlContext)
    registry.registerFunc("rf_explodeTiles", ExplodeTiles.apply(1.0, None, _))
    registry.registerFunc("rf_cellType", ub(GetCellType.apply))
    registry.registerFunc("rf_convertCellType", bb(SetCellType.apply))
    registry.registerFunc("rf_tileDimensions", ub(GetDimensions.apply))
    registry.registerFunc("rf_boundsGeometry", ub(ExtentToGeometry.apply))
    registry.registerFunc("rf_localAdd", bb(Add.apply))
    registry.registerFunc("rf_localSubtract", bb(Subtract.apply))
    registry.registerFunc("rf_localMultiply", bb(Multiply.apply))
    registry.registerFunc("rf_localDivide", bb(Divide.apply))
    registry.registerFunc("rf_normalizedDifference", bb(NormalizedDifference.apply))
    registry.registerFunc("rf_localLess", bb(Less.apply))
    registry.registerFunc("rf_localLessEqual", bb(LessEqual.apply))
    registry.registerFunc("rf_localGreater", bb(Greater.apply))
    registry.registerFunc("rf_localGreaterEqual", bb(GreaterEqual.apply))
    registry.registerFunc("rf_localEqual", bb(Equal.apply))
    registry.registerFunc("rf_localUnequal", bb(Unequal.apply))
    registry.registerFunc("rf_tileSum", ub(Sum.apply))
    registry.registerFunc("rf_dataCells", ub(DataCells.apply))
    registry.registerFunc("rf_noDataCells", ub(NoDataCells.apply))
    registry.registerFunc("rf_isNoDataTile", ub(IsNoDataTile.apply))
    registry.registerFunc("rf_tileMin", ub(TileMin.apply))
    registry.registerFunc("rf_tileMax", ub(TileMax.apply))
    registry.registerFunc("rf_tileMean", ub(TileMean.apply))
    registry.registerFunc("rf_tileStats", ub(TileStats.apply))
    registry.registerFunc("rf_tileHistogram", ub(TileHistogram.apply))
    registry.registerFunc("rf_aggStats", ub(CellStatsAggregate.CellStatsAggregateUDAF.apply))
    registry.registerFunc("rf_aggHistogram", ub(HistogramAggregate.HistogramAggregateUDAF.apply))
    registry.registerFunc("rf_localAggStats", ub(LocalStatsAggregate.LocalStatsAggregateUDAF.apply))
    registry.registerFunc("rf_renderAscii", ub(DebugRender.RenderMatrix.apply))
    registry.registerFunc("rf_localAggMax", ub(LocalTileOpAggregate.LocalMaxUDAF.apply))
    registry.registerFunc("rf_localAggMin", ub(LocalTileOpAggregate.LocalMinUDAF.apply))
    registry.registerFunc("rf_localAggCount", ub(LocalCountAggregate.LocalDataCellsUDAF.apply))
    registry.registerFunc("rf_localAggMean", ub(LocalMeanAggregate.apply))

    sqlContext.udf.register("rf_makeConstantTile", F.makeConstantTile)
    sqlContext.udf.register("rf_tileZeros", F.tileZeros)
    sqlContext.udf.register("rf_tileOnes", F.tileOnes)
    sqlContext.udf.register("rf_cellTypes", F.cellTypes)
    sqlContext.udf.register("rf_reprojectGeometry", F.reprojectGeometryCRSName)
  }
}
