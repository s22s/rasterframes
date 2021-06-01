package org.locationtech.rasterframes.encoders

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.objects.{UnwrapOption, WrapOption}
import org.apache.spark.sql.types.ObjectType
import org.locationtech.rasterframes.encoders.CatalystSerializerEncoder.CatSerializeToRow

import scala.reflect.classTag
import scala.reflect.runtime.universe.TypeTag

object CatSerOptionEncoder {
  def wrapOption[T: TypeTag](child: CatSerializeToRow[T]) =  {
    WrapOption(child, ScalaReflection.dataTypeFor[T])
  }

  def unwrapOption[T: TypeTag](child: Expression) = {
    UnwrapOption(ScalaReflection.dataTypeFor[T], child)
  }

  def apply[T: TypeTag: CatalystSerializer]: ExpressionEncoder[Option[T]] = {
    val enc = CatalystSerializerEncoder[T]()
    val tType = ObjectType(runtimeClass[T])
    val serializer =
      UnwrapOption(
        tType,
        enc.serializer.head
      )
    //val schema = schemaOf[CellType]
//    val inputObject = BoundReference(0, tType, nullable = false)
//    val inputRow = GetColumnByOrdinal(0, tType)

    val deserializer = WrapOption(
      enc.deserializer, tType
    )

    ExpressionEncoder[Option[T]](
      serializer,
      deserializer,
      classTag[Option[T]]
    )
  }
}
