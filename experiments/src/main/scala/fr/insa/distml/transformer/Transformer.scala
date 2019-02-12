package fr.insa.distml.transformer

import org.apache.spark.sql.{DataFrame, SparkSession}

trait Transformer {
  def transform(data: DataFrame)(implicit spark: SparkSession): DataFrame
}
