package fr.insa.distml.metrics

import ch.cern.sparkmeasure.TaskMetrics
import org.apache.spark.sql.{DataFrame, SparkSession}

class TaskMetricsCollector(spark: SparkSession) extends TaskMetrics(spark) with MetricsCollector {
  override def collect(): DataFrame = {
    super.createTaskMetricsDF()
  }
}
