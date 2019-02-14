package fr.insa.distml.reader
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, SparkSession}

class EpilepticSeizureRecognitionReader extends Reader {

  private var _location: String = _

  def setLocation(location: String): this.type = {
    _location = location
    this
  }

  def getLocation: String = _location

  override def read()(implicit spark: SparkSession): DataFrame = {

    val raw = spark.read
      .format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(_location)

    val assembler = new VectorAssembler()
      .setInputCols((for(i <- List.range(1, 179)) yield s"X$i").toArray)
      .setOutputCol("features")

    val data = assembler.transform(raw.withColumnRenamed("y", "label"))

    data
  }
}
