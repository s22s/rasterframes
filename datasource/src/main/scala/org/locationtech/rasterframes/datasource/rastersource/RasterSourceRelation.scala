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

package org.locationtech.rasterframes.datasource.rastersource

import org.locationtech.rasterframes._
import org.locationtech.rasterframes.encoders.CatalystSerializer._
import org.locationtech.rasterframes.expressions.transformers.{RasterRefToTile, RasterSourceToRasterRefs, URIToRasterSource}
import org.locationtech.rasterframes.util._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.functions._
import org.locationtech.rasterframes.datasource.rastersource.RasterSourceDataSource.RasterSourceTable
import org.locationtech.rasterframes.expressions.transformers.RasterSourceToRasterRefs.bandNames
import org.locationtech.rasterframes.model.TileDimensions
import org.locationtech.rasterframes.tiles.ProjectedRasterTile

/**
  * Constructs a Spark Relation over one or more RasterSource paths.
  * @param sqlContext Query context
  * @param discretePaths list of URIs to fetch rastefrom.
  * @param bandIndexes band indexes to fetch
  * @param subtileDims how big to tile/subdivide rasters info
  */
case class RasterSourceRelation(sqlContext: SQLContext, discretePaths: Seq[String],
  pathTable: Option[RasterSourceTable], bandIndexes: Seq[Int], subtileDims: Option[TileDimensions])
  extends BaseRelation with TableScan {

  lazy val inputColNames = pathTable
    .map(_.columnNames)
    .getOrElse(Seq(TILE_COLUMN.columnName))

  def pathColNames = inputColNames
    .map(_ + "_path")

  def srcColNames = inputColNames
    .map(_ + "_src")

  def refColNames = srcColNames
    .flatMap(bandNames(_, bandIndexes))
    .map(_ + "_ref")

  def tileColNames = inputColNames
    .flatMap(bandNames(_, bandIndexes))

  override def schema: StructType = {
    val tileSchema = schemaOf[ProjectedRasterTile]
    val paths = for {
      pathCol <- pathColNames
    } yield StructField(pathCol, StringType, false)
    val tiles = for {
      tileColName <- tileColNames
    } yield StructField(tileColName, tileSchema, true)

    StructType(paths ++ tiles)
  }

  override def buildScan(): RDD[Row] = {
    import sqlContext.implicits._

    // The general transformaion is:
    // input -> path -> src -> ref -> tile
    // Each step is broken down for readability
    val inputs: DataFrame = pathTable match {
      case Some(spec) => sqlContext.table(spec.tableName)
      case _ => discretePaths.toDF(inputColNames.head)
    }

    // Basically renames the input columns to have the '_path' suffix
    val pathsAliasing = for {
      (input, path) <- inputColNames.zip(pathColNames)
    } yield col(input).as(path)

    // Wraps paths in a RasterSource
    val srcs = for {
      (pathColName, srcColName) <- pathColNames.zip(srcColNames)
    } yield URIToRasterSource(col(pathColName)) as srcColName

    // Expand RasterSource into multiple columns per band, and multiple rows per tile
    // There's some unintentional fragililty here in that the structure of the expression
    // is expected to line up with our column structure here.
    val refs = RasterSourceToRasterRefs(subtileDims, bandIndexes, srcs: _*) as refColNames

    // RasterSourceToRasterRef is a generator, which means you have to do the Tile conversion
    // in a separate select statement (Query planner doesn't know how many columns ahead of time).
    val refsToTiles = for {
      (refColName, tileColName) <- refColNames.zip(tileColNames)
    } yield RasterRefToTile(col(refColName)) as tileColName

    // Add path columns
    val withPaths = inputs
      .select(pathsAliasing: _*)

    // Path columns have to be manually pulled along through each step. Resolve columns once
    // and reused with each select.
    val paths = pathColNames.map(withPaths.apply)

    val df = withPaths
      .select(paths :+ refs: _*)
      .select(paths ++ refsToTiles: _*)

    df.rdd
  }
}
