package fr.insa.distml.experiments

import java.util.NoSuchElementException
import org.apache.spark.sql.SparkSession

trait Experiment {
  def execute(config: Configuration)(implicit spark: SparkSession): Metrics
}

object Experiment {

  def from(name: String): Experiment = {
    name match {
      case "epileptic-seizure-recognition" => EpilepticSeizureRecognition
      case _                               => throw new NoSuchElementException
    }
  }
}
