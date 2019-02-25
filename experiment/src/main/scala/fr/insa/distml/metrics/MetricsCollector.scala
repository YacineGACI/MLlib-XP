package fr.insa.distml.metrics

import java.util.NoSuchElementException

import org.apache.spark.sql.{DataFrame, SparkSession}

trait MetricsCollector {
  def begin(): Long

  def end(): Long

  def collect(): DataFrame
}

object MetricsCollectors {
  def fromLevel(level: String)(implicit spark: SparkSession): MetricsCollector = {
    level match {
      case "task"  => new  TaskMetricsCollector(spark)
      case "stage" => new StageMetricsCollector(spark)
      case _       => throw new NoSuchElementException
    }
  }
}
