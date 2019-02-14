package fr.insa.distml

import scopt.OptionParser

import scala.io.Source
import io.circe.yaml.{parser => YamlParser}
import io.circe._
import cats.syntax.either._
import ch.cern.sparkmeasure.StageMetrics
import fr.insa.distml.experiment.Experiment
import fr.insa.distml.reader.Reader
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.immutable.Map


object Main {

  def save(df: DataFrame, file: String, format: String): Unit = {
    df.coalesce(1).write.format(format).save(file)
  }

  def generateParameterizedInstance[T](classname: String, parameter: Map[String, Object]): T = {
    val o = Class.forName(classname).newInstance()
    for((name, value) <- parameter) {
      o.getClass.getMethod("setMaxDepth").invoke(5)
    }
  }

  def generateParameters(grid: Map[String, List[Object]]): Seq[Map[String, Object]] = {

    if(grid.isEmpty) {
      return Seq.empty
    }

    val (name, values) = grid.head

    val untreated = grid.filterKeys(key => name != key)

    val parameters = generateParameters(untreated)

    parameters.flatMap(parameter => for(value <- values) yield parameter + (name -> value))
  }

  def generateExperiments(datasets: List[DatasetConf], algorithms: List[AlgorithmConf], split: SplitConf): Seq[Experiment] = {

    for(dataset <- datasets; algorithm <- algorithms if dataset.dtypes.contains(algorithm.dtype)) {

      val    readerParameters = generateParameters(     dataset.reader.parameters)
      val estimatorParameters = generateParameters(algorithm.estimator.parameters)
      val evaluatorParameters = generateParameters(algorithm.evaluator.parameters)

      for(readerParameter <- readerParameters; estimatorParameter <- estimatorParameters; evaluatorParameter <- evaluatorParameters) {

        val reader    = Class.forName(     dataset.reader.classname).newInstance().asInstanceOf[Reader]
        val estimator = Class.forName(algorithm.estimator.classname).newInstance().asInstanceOf[Estimator]
        val evaluator = Class.forName(algorithm.evaluator.classname).newInstance().asInstanceOf[Evaluator]


      }
    }
  }

  def performExperiments(conf: ExperimentsConf): Unit = {

    val experiments = generateExperiments(conf.datasets, conf.algorithms, conf.split)

    val location = conf.metrics.location
    val format   = conf.metrics.format

    for(experiment <- experiments) {

      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()

      implicit val spark: SparkSession = SparkSession.builder().getOrCreate()

      val sparkMetrics = StageMetrics(spark)

      sparkMetrics.begin()

      val appMetrics = experiment.execute()

      sparkMetrics.end()

      save(sparkMetrics.createStageMetricsDF(), s"$location/spark-metrics", format)
      save(  appMetrics.createAppMetricsDF(),   s"$location/app-metrics",   format)

      spark.stop()
    }
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

        performExperiments(conf.experiments)
      }
    }
  }

  case class Params(config: String  = ".")

  case class Conf(experiments: ExperimentsConf)

  case class ExperimentsConf(
       metrics:        MetricsConf  =   MetricsConf(),
     execution:      ExecutionConf  = ExecutionConf(),
         split:          SplitConf  =     SplitConf(),
      datasets:   List[DatasetConf],
    algorithms: List[AlgorithmConf]
  )

  case class MetricsConf(
    location: String = "./results/",
      format: String = "csv",
    applicative:   AppMetricsConf =   AppMetricsConf(),
          spark: SparkMetricsConf = SparkMetricsConf()
  )
  case class   AppMetricsConf(enable: Boolean = true)
  case class SparkMetricsConf(enable: Boolean = true, select: String = "*")

  case class ExecutionConf(runs: Int = 1)

  case class   DatasetConf(dtypes: List[String],   reader:    ReaderConf)
  case class AlgorithmConf(dtype:       String, estimator: EstimatorConf, evaluator: EvaluatorConf)

  case class EstimatorConf(classname: String, parameters: Map[String, List[Json]] = Map.empty)
  case class EvaluatorConf(classname: String, parameters: Map[String, List[Json]] = Map.empty)
  case class    ReaderConf(classname: String, parameters: Map[String, List[Json]] = Map.empty)

  case class SplitConf(ratio: Double = 0.8)
}
