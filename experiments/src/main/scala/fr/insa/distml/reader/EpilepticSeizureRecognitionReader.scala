package fr.insa.distml.reader

import org.apache.spark.sql.{DataFrame, SparkSession}

class EpilepticSeizureRecognitionReader(location: String) extends Reader {

  override def read()(implicit spark: SparkSession): DataFrame = {

  }
}
