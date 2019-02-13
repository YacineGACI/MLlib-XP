package fr.insa.distml.transformer

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType

class ColumnRenamer(override val uid: String) extends Transformer with HasInputCol with HasOutputCol with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("columnRenamer"))

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transformSchema(schema: StructType): StructType = {
    val inputColName = $(inputCol)
    val outputColName = $(outputCol)
    val inputFields = schema.fields

    require(!inputFields.exists(_.name == outputColName), s"Output column $outputColName already exists.")

    val outputField = inputFields
      .find(field => field.name == inputColName)
      .map(field => field.copy(name = outputColName))
      .getOrElse(throw new IllegalArgumentException(s"Input column $inputColName does not exists."))

    val outputFields = inputFields.filter(field => field.name != inputColName) :+ outputField

    StructType(outputFields)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val inputColName = $(inputCol)
    val outputColName = $(outputCol)

    dataset.withColumnRenamed(inputColName, outputColName)
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}

object ColumnRenamer extends DefaultParamsReadable[ColumnRenamer] {

  override def load(path: String): ColumnRenamer = super.load(path)
}
