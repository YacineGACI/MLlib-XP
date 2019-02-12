package fr.insa.distml

import fr.insa.distml.experiment.Experiment
import org.apache.spark.sql.{DataFrame, SparkSession}
import scopt.OptionParser
import ch.cern.sparkmeasure.StageMetrics


object Main {

  def save(df: DataFrame, file: String): Unit = {
    df.coalesce(1).write.format("json").save(file)
  }

  def startExperiment(params: Params): Unit = {

    val (local, name, dataset, metrics) = (params.local, params.experiment, params.dataset, params.metrics)

    val builder = SparkSession.builder

    if(local) builder.master("local[*]")

    implicit val spark: SparkSession = builder.getOrCreate()

    val experiment = Experiment.from(name, dataset)

    val stageMetrics = StageMetrics(spark)

    stageMetrics.begin()

    val appMetrics = experiment.execute()

    stageMetrics.end()

    save(stageMetrics.createStageMetricsDF(), s"$metrics/spark-metrics.json")
    save(stageMetrics.createAccumulablesDF(), s"$metrics/spark-metrics-accumulable.json")
    save(  appMetrics.createDF(),             s"$metrics/app-metrics.json")

    spark.stop()
  }

  def main(args: Array[String]): Unit = {

    val parser = new OptionParser[Params]("DistML") {
      opt[String]("config")
        .required()
        .valueName("<file>")
        .action((value, params) => params.copy(config = value))
        .text("path to the config file")

      opt[Unit]("local")
        .optional()
        .action((_, params) => params.copy(local = true))
        .text("to start a local spark cluster")
    }

    parser.parse(args, Params()) match {
      case Some(params) => startExperiment(params)
      case None         => Unit
    }
  }

  case class Params(
    config: String  = ".",
     local: Boolean = false
  )
}
