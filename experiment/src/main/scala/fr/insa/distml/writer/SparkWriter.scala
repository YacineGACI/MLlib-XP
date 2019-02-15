package fr.insa.distml.writer

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.immutable._

class SparkWriter extends Writer {

  private var _location: String = _
  private var _format:   String = _
  private var _coalesce: Option[Int]      = None
  private var _options:  Map[String, Any] = Map.empty

  def setLocation(location: String): this.type = {
    _location = location
    this
  }

  def getLocation: String = _location

  def setFormat(format: String): this.type = {
    _format = format
    this
  }

  def getFormat: String = _format

  def setOptions(options: Map[String, Any]): this.type = {
    _options = options
    this
  }

  def getOptions: Map[String, Any] = _options

  def setCoalesce(coalesce: Int): this.type = {
    _coalesce = Some(coalesce)
    this
  }

  def getCoalesce: Option[Int] = _coalesce

  override def write(dataframe: DataFrame)(implicit spark: SparkSession): Unit = {

    val df = _coalesce.map(dataframe.coalesce).getOrElse(dataframe)

    val writer = df.write.format(_format)

    for((key, value) <- _options) {
      writer.option(key, value.toString)
    }

    writer.save(_location)
  }
}
