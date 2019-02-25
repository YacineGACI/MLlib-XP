package fr.insa.distml.transformer

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

import scala.collection.immutable._

class EpilepticSeizureRecognitionTransformer(override val uid: String) extends Transformer {

  def this() = this(Identifiable.randomUID("EpilepticSeizureRecognitionTransformer"))

  override def transform(dataset: Dataset[_]): DataFrame = {

    val assembler = new VectorAssembler()
      .setInputCols((for (i <- List.range(1, 179)) yield s"X$i").toArray)
      .setOutputCol("features")

    assembler.transform(dataset.withColumnRenamed("y", "label"))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    StructType(schema.fields :+ StructField("features", VectorType) :+ StructField("label", IntegerType))
  }
}
