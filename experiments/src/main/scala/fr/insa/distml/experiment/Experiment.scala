package fr.insa.distml.experiment

import fr.insa.distml.metrics.Metrics
import org.apache.spark.sql.{DataFrame, SparkSession}

trait Experiment {
  def execute()(implicit spark: SparkSession): Metrics
}
