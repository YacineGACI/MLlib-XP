package fr.insa.distml

import scopt.OptionParser
import scala.io.Source
import io.circe.yaml.{parser => YamlParser}
import io.circe._
import cats.syntax.either._
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._


object Main {

  def performExperiments(conf: Conf): Unit = {
    println(conf)
  }

  def main(args: Array[String]): Unit = {

    implicit val customConfig: Configuration = Configuration.default.withDefaults

    val parser = new OptionParser[Params]("DistML") {
      opt[String]("config")
        .required()
        .valueName("<file>")
        .action((value, params) => params.copy(config = value))
        .text("path to the config file")
    }

    parser.parse(args, Params()) match {
      case None => Unit
      case Some(params) => {

        val json = YamlParser
          .parse(
            Source
              .fromFile(params.config)
              .bufferedReader()
          )

        val conf = json
          .leftMap(err => err: Error)
          .flatMap(_.as[Conf])
          .valueOr(throw _)

        performExperiments(conf)
      }
    }
  }

  case class Params(config: String  = ".")

  case class Conf(experiments: ExperimentsConf)

  case class ExperimentsConf(
        spark: Map[String, String] = Map.empty,
      metrics:       MetricsConf = MetricsConf(),
      results:       ResultsConf = ResultsConf(),
    execution:     ExecutionConf = ExecutionConf(),
        split:  Option[SplitConf],
         load: List[    LoadConf] = List.empty,
        train: List[   TrainConf] = List.empty,
     evaluate: List[EvaluateConf] = List.empty
  )

  case class MetricsConf(applicative: AppMetricsConf = AppMetricsConf(), spark: SparkMetricsConf = SparkMetricsConf())
  case class   AppMetricsConf(enable: Boolean = true)
  case class SparkMetricsConf(enable: Boolean = true, select: String = "*")

  case class ResultsConf(location: String = "./results/", format: String = "csv")

  case class ExecutionConf(runs: Int = 1)

  case class     LoadConf(name: String, dtypes: List[String], transformers: List[TransformerConf] = List.empty, reader: ReaderConf)
  case class    TrainConf(name: String, dtype: String,           estimator:        EstimatorConf)
  case class EvaluateConf(name: String, dtype: String,           evaluator:        EvaluatorConf)

  case class ParametersConf(
       stable: Map[String,      Json]  = Map.empty,
     variable: Map[String, List[Json]] = Map.empty
   )

  case class       SplitConf(                   parameters: ParametersConf = ParametersConf())

  case class TransformerConf(classname: String, parameters: ParametersConf = ParametersConf())
  case class   EstimatorConf(classname: String, parameters: ParametersConf = ParametersConf())
  case class   EvaluatorConf(classname: String, parameters: ParametersConf = ParametersConf())

  case class      ReaderConf(classname: Option[String] = None, options: Map[String, Json] = Map.empty, format: Option[String] = None, location: String)
}
