package fr.insa.distml

import fr.insa.distml.experiments.{Configuration, Experiments}
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

object Main {

  def startExperiment(params: Params): Unit = {

    val builder = SparkSession.builder

    if (params.local) {
      builder.master("local[*]")
    }

    val spark = builder.getOrCreate()

    val experiment = Experiments(params.experiment)

    val metrics = experiment.execute(spark, Configuration(params.dataset))

    println(metrics)

    spark.stop()
  }

  def main(args: Array[String]): Unit = {

    val parser = new OptionParser[Params](this.getClass.getSimpleName) {
      opt[String]("dataset")
        .required()
        .valueName("<file>")
        .action((value, params) => params.copy(dataset = value))
        .text("path to the dataset")

      opt[String]("experiment")
        .required()
        .valueName("<name>")
        .action((value, params) => params.copy(experiment = value))
        .text("which experiment to launch")

      opt[Unit]("local")
        .optional()
        .action((_, params) => params.copy(local = true))
        .text("start a local spark cluster")
    }

    parser.parse(args, Params()) match {
      case Some(params) => startExperiment(params)
      case None         => Unit
    }
  }

  case class Params(
       dataset: String  = ".",
         local: Boolean = false,
    experiment: String  = ""
  )
}
