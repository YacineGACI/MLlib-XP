package fr.insa.distml.reader

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.immutable._

class SparkReader extends Reader {

  private var _location: String = _
  private var _format:   String = _
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

  override def read()(implicit spark: SparkSession): DataFrame = {

    val reader = spark.read.format(_format)

    for((key, value) <- _options)
      reader.option(key, value.toString)

    reader.load(_location)
  }
}
