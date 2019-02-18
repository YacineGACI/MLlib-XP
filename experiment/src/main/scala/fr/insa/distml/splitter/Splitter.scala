package fr.insa.distml.splitter

import org.apache.spark.sql.{DataFrame, SparkSession}

trait Splitter {
  def split(dataframe: DataFrame)(implicit spark: SparkSession): Array[DataFrame]
}
