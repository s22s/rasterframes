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

package org.locationtech.rasterframes.encoders

import geotrellis.proj4._
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, encoderFor}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.{Dataset, Encoder, Encoders}
import org.locationtech.rasterframes.{TestData, TestEnvironment}


class DevSpec extends TestEnvironment {
  import TestData._

  describe("home rolled encoders") {
    import spark.implicits._
    it("matches spark serializers") {
      val home = Person.encoder
      val mySerializerDescription = home.treeString
      info(mySerializerDescription )

      val baked = implicitly[Encoder[Person]].asInstanceOf[ExpressionEncoder[Person]].objSerializer
      val sparkSerializerDescription = baked.treeString
      info(sparkSerializerDescription )

      mySerializerDescription shouldBe sparkSerializerDescription
    }

    it ("Round-trip Person through DataFrame") {
      implicit def en: Encoder[Person] = {
        new ExpressionEncoder[Person](Person.encoder, Person.decoder, typeToClassTag[Person])
      }
      val ds = Seq(Person("Bob", 11), Person("Eugene", 10)).toDS

      ds.printSchema()
      ds.show()

      val df = ds.toDF()

      val me = df.as[Person].first()
      info(me.toString)
    }

    it ("Round-trip School through DataFrame") {
      implicit def en: Encoder[School] = {
        new ExpressionEncoder[School](School.encoder, School.decoder, typeToClassTag[School])
      }

      val teacher = Person("Sid", 0)
      val student = Person("Eugene", 10)
      val school = School(teacher, student)
      val ds = Seq(school).toDS

      ds.printSchema()
      ds.show()

      val df = ds.toDF()

      val it = df.as[School].first()
      info(it.toString)
    }
  }

  describe("scalar tile operations") {
    import org.locationtech.rasterframes.tiles.ProjectedRasterTile
    import spark.implicits._

    ignore("=== DeSer for Time ===") {
      import org.apache.spark.sql.catalyst.ScalaReflection.deserializerForType

      import scala.reflect.runtime.universe._

      val de = deserializerForType(typeOf[java.sql.Timestamp])

      info(de.numberedTreeString)
    }


    ignore("=== DeSer for Person ===") {
      import org.apache.spark.sql.catalyst.ScalaReflection.deserializerForType

      import scala.reflect.runtime.universe._

      val de = deserializerForType(typeOf[Person])

      info(de.numberedTreeString)
    }

    it("Person Frame"){
      val ds = Seq((Person("Bob", 11), Person("Eugene", 10))).toDS
      ds.printSchema()
      ds.show()
    }

    ignore("does it really serialize CRS?") {
      val first = CRSEncoder()

      info("Before: \n" + first.deserializer.treeString)

      val en = first.resolveAndBind()
      info("After: \n" + en.deserializer.treeString)

      val ir = en.createSerializer().apply(LatLng)
      val out = en.createDeserializer().apply(ir)
      out shouldBe LatLng
    }

    it("person") {
      val data = List(Person("Eugene", 39), Person("Bob", 45))
      val ds = data.toDS
      ds.printSchema()
      ds.show()
      val df = ds.toDF()

      val me = df.as[Person].first()
      info(me.toString)
    }

    it("person encoder"){
      val en = ExpressionEncoder[Person]
      info(en.objSerializer.treeString)
      info(en.deserializer.treeString)
    }

    it("prt encoder"){
      val en = ProjectedRasterTile.prtEncoder
      info(en.objSerializer.treeString)
      info(en.deserializer.treeString)
    }

    it("should round trip Tuple2[Int, ProjectedRasterTile]") {
      implicit val enc = Encoders.tuple(
        implicitly[Encoder[Int]],
        ProjectedRasterTile.prtEncoder
      )
      val data = Seq((1, one), (2, two))
      val ds = data.toDS
      val df = ds.toDF()

      val indexedTile = df.as[(Int, ProjectedRasterTile)].first()
      indexedTile should be (data.head)
    }

    it("should round trip Tuple1[ProjectedRasterTile]") {
      implicit val enc = ExpressionEncoder.tuple(
        ProjectedRasterTile.prtEncoder
      )
      val data = Seq(Tuple1(one), Tuple1(two))
      val ds = data.toDS
      val df = ds.toDF()

      val tupledTile = df.as[Tuple1[ProjectedRasterTile]].first()
      tupledTile should be (data.head)
    }

    it("should round trip Option[ProjectedRasterTile]") {
      //import StandardEncoders.optionPrtEncoder
      val data = Seq(Option(one), Option(two))
      val ds = data.toDS
      ds.printSchema()
      ds.show()
      val df = ds.toDF()

      val tile = df.as[Option[ProjectedRasterTile]].first()
      info(tile.toString)
    }

    it("should round trip ProjectedRasterTile") {
      val data = Seq(one, two)
      val ds = data.toDS
      ds.printSchema()
      ds.show()
      val df = ds.toDF()

      val tile = df.as[ProjectedRasterTile].first()
      info(tile.toString)
    }

    it("one") {
      type T = ProjectedRasterTile
      val data = Seq(one)
      val in: Encoder[T] = implicitly[Encoder[T]]
      val enc = encoderFor[T](in)
      val toRow = enc.createSerializer()
      val attributes = enc.schema.map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())
      val encoded = data.map(d => toRow(d).copy())
      val plan = new LocalRelation(attributes, encoded)
      val dataset = new Dataset(spark, plan, implicitly[Encoder[T]])
      dataset.printSchema()
      dataset.show()
      val df = dataset.toDF("one")
      df.printSchema

      //      val df2 = localSeqToDatasetHolder(Seq(one)).toDF("one")
    }

    it("two") {
      type T = (ProjectedRasterTile, ProjectedRasterTile)
      val data = Seq((one, one))
      val in: Encoder[T] = implicitly[Encoder[T]]
      val enc = encoderFor[T](in)
      val toRow = enc.createSerializer()
      val attributes = enc.schema.map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())
      val encoded = data.map(d => toRow(d).copy())
      val plan = new LocalRelation(attributes, encoded)
      val dataset = new Dataset(spark, plan, implicitly[Encoder[T]])
      dataset.printSchema()
      dataset.show()
      val df = dataset.toDF("one", "two")
      df.printSchema()

      //      val df1 = localSeqToDatasetHolder(Seq((one, one))).toDF("one","other")
    }

    it("should rf_local_add") {
      val df = Seq(two).toDF("one")
      val tile = df.as[ProjectedRasterTile].first()
      info(tile.toString)
      df.printSchema()
      df.show()

    }

    it("should handle two") {
      val df = Seq((one, two)).toDF("one", "two")
      df.printSchema()
      df.show()
      val tile = df.select("one").as[ProjectedRasterTile].first()
      info(tile.toString)
    }
  }

}
