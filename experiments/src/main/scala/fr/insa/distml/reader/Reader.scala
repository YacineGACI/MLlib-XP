package fr.insa.distml.reader

import org.apache.spark.sql.{DataFrame, SparkSession}

trait Reader {
  def read()(implicit spark: SparkSession): DataFrame
}
