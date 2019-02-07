package fr.insa.distml.experiments

import java.util.NoSuchElementException
import org.apache.spark.sql.SparkSession

trait Experiment {
  def execute(spark: SparkSession, config: Configuration): Metrics
}

object Experiments {

  def apply(experiment: String): Experiment = {
    experiment match {
      case "epileptic-seizure-recognition" => new EpilepticSeizureRecognition
      case _                               => throw new NoSuchElementException
    }
  }
}

