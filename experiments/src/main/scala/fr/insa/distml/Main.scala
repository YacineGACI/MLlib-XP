package fr.insa.distml

import fr.insa.distml.experiments.Experiment
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
      opt[String]("dataset")
        .required()
        .valueName("<file>")
        .action((value, params) => params.copy(dataset = value))
        .text("path to the dataset file")

      opt[String]("experiment")
        .required()
        .valueName("<name>")
        .action((value, params) => params.copy(experiment = value))
        .text("name of the experiment to launch")

      opt[String]("metrics")
        .optional()
        .valueName("<dir>")
        .action((value, params) => params.copy(metrics = value.replaceAll("/$", "")))
        .text("path to the metrics directory")

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
       dataset: String  = ".",
         local: Boolean = false,
    experiment: String  = "",
       metrics: String  = ""
  )
}
