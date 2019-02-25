package fr.insa.distml.metrics

import ch.cern.sparkmeasure.StageMetrics

import org.apache.spark.sql.{DataFrame, SparkSession}

class StageMetricsCollector(spark: SparkSession) extends StageMetrics(spark) with MetricsCollector {
  override def collect(): DataFrame = {
    super.createStageMetricsDF()
  }
}
