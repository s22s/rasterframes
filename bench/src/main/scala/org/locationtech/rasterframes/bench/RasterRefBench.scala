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

package org.locationtech.rasterframes.bench

import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql._
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.expressions.transformers.{RasterRefToTile, RasterSourceToRasterRefs}
import org.locationtech.rasterframes.model.TileDimensions
import org.locationtech.rasterframes.ref.RasterSource
import org.openjdk.jmh.annotations._

@BenchmarkMode(Array(Mode.AverageTime))
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
class RasterRefBench  extends SparkEnv with LazyLogging {
  import spark.implicits._

  var expandedDF: DataFrame = _
  var singleDF: DataFrame = _

  @Setup(Level.Trial)
  def setupData(): Unit = {
    val r1 = RasterSource(remoteCOGSingleband1)
    val r2 = RasterSource(remoteCOGSingleband2)

    singleDF = Seq((r1, r2)).toDF("B1", "B2")
      .select(RasterRefToTile(RasterSourceToRasterRefs(Some(TileDimensions(r1.dimensions)), Seq(0), $"B1", $"B2")))

    expandedDF = Seq((r1, r2)).toDF("B1", "B2")
      .select(RasterRefToTile(RasterSourceToRasterRefs($"B1", $"B2")))
  }

  @Benchmark
  def computeDifferenceExpanded() = {
    expandedDF
      .select(rf_normalized_difference($"B1", $"B2"))
      .cache()
      .count()
  }

  @Benchmark
  def computeDifferenceSingle() = {
    singleDF
      .select(rf_normalized_difference($"B1", $"B2"))
      .cache()
      .count()
  }

  @Benchmark
  def computeStatsSingle() = {
    singleDF.select(rf_agg_stats($"B1")).collect()
  }

  @Benchmark
  def computeStatsExpanded() = {
    expandedDF.select(rf_agg_stats($"B1")).collect()
  }

  @Benchmark
  def computeDifferenceStats() = {
    singleDF.select(rf_agg_stats(rf_normalized_difference($"B1", $"B2"))).collect()
  }

}