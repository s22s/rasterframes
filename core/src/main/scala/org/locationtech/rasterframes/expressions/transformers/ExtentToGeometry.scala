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

import org.locationtech.rasterframes.encoders.CatalystSerializer._
import org.locationtech.rasterframes.expressions.{DynamicExtractors, row}
import org.locationtech.jts.geom.{Envelope, Geometry}
import geotrellis.vector.Extent
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.jts.JTSTypes
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Column, TypedColumn}
import org.locationtech.geomesa.spark.jts.encoders.SpatialEncoders

/**
 * Catalyst Expression for converting a bounding box structure into a JTS Geometry type.
 *
 * @since 8/24/18
 */
case class ExtentToGeometry(child: Expression) extends UnaryExpression with CodegenFallback {
    override def nodeName: String = "st_geometry"

  override def dataType: DataType = JTSTypes.GeometryTypeInstance

  override def checkInputDataTypes(): TypeCheckResult = {
    if (!DynamicExtractors.extentExtractor.isDefinedAt(child.dataType)) {
      TypeCheckFailure(
        s"Expected bounding box of form '${schemaOf[Envelope]}' or '${schemaOf[Extent]}' " +
          s"but received '${child.dataType.simpleString}'."
      )
    }
    else TypeCheckSuccess
  }

  override protected def nullSafeEval(input: Any): Any = {
    val r = row(input)
    val extent = DynamicExtractors.extentExtractor(child.dataType)(r)
    val geom = extent.toPolygon()
    JTSTypes.GeometryTypeInstance.serialize(geom)
  }
}

object ExtentToGeometry extends SpatialEncoders {
  def apply(bounds: Column): TypedColumn[Any, Geometry] =
    new Column(new ExtentToGeometry(bounds.expr)).as[Geometry]
}
