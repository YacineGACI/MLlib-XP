package fr.insa.distml.experiment

import java.util.NoSuchElementException
import org.apache.spark.sql.{DataFrame, SparkSession}

trait Experiment {
  def execute()(implicit spark: SparkSession): Metrics
}

object Experiment {

  def from(name: String, dataset: String): Experiment = {
    name match {
      case "epileptic-seizure-recognition" => new EpilepticSeizureRecognition(dataset)
      case _                               => throw new NoSuchElementException
    }
  }
}

trait Metrics {
  def createDF()(implicit spark: SparkSession): DataFrame
}
