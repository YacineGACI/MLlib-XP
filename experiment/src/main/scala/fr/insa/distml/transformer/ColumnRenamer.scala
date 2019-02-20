package fr.insa.distml.transformer

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.{StructField, StructType}

class ColumnRenamer(override val uid: String) extends Transformer {

  var _existingName: String = _
  var _newName:      String = _

  def getExistingName: String = _existingName

  def setExistingName(existingName: String): this.type = {
    _existingName = existingName
    this
  }

  def getNewName: String = _newName

  def setNewName(newName: String): this.type = {
    _newName = newName
    this
  }

  def this() = this(Identifiable.randomUID("ColumnRenamer"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    dataset.withColumnRenamed(_existingName, _newName)
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    val existing = schema.fields.find(_.name == _existingName).getOrElse(throw new NoSuchElementException)
    StructType(schema.fields :+ StructField(_newName, existing.dataType, existing.nullable, existing.metadata))
  }
}
