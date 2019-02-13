package fr.insa.distml.metrics

import org.apache.spark.sql.{DataFrame, SparkSession}

trait Metrics {
  def createDF()(implicit spark: SparkSession): DataFrame
}

abstract class ApplicationMetrics extends Metrics {
  def trainingTime: Long
}
