package fr.insa.distml

import fr.insa.distml.experiment._
import scopt.OptionParser
import play.api.libs.json._
import play.api.libs.functional.syntax._
import scala.reflect.io.Path
import scala.collection.immutable._


object Main {

  def main(args: Array[String]): Unit = {

    val parser = new OptionParser[Arguments]("DistML") {
      opt[String]("config")
        .required()
        .valueName("<file|json>")
        .action((value, parameters) => parameters.copy(config = value))
        .text("The configuration in json or the path to the configuration file")
    }

    parser.parse(args, Arguments("")) match {
      case None             => Unit
      case Some(parameters) =>
        val json = Path(parameters.config)
          .ifFile(file => Json.parse(file.inputStream()))
          .getOrElse(Json.parse(parameters.config))

        json.validate[ExperimentConfiguration] match {
          case JsSuccess(config, _) => Experiment.startExperiment(config)
          case JsError(errors)      => throw new IllegalArgumentException(errors.toString())
        }
    }
  }

  case class Arguments(config: String)

  implicit val anyRead: Reads[Any] = new Reads[Any] {

    def toAny(json: JsValue): JsResult[Any] = {
      json match {
        case JsNumber(value) => JsSuccess(
          if(value.isValidInt) value.toIntExact
          else                 value.doubleValue
        )
        case JsBoolean(bool) => JsSuccess(bool)
        case JsString(value) => JsSuccess(value)
        case _               => JsError("Type must be Int|Double|Boolean|String")
      }
    }

    override def reads(json: JsValue): JsResult[Any] = {
      json match {
        case JsArray(values)  =>
          val (errors, any) = values.map(toAny).toArray.partition(_.isError)
          errors.headOption match {
            case Some(err) => err
            case None      => JsSuccess(any.map(_.get))
          }
        case JsObject(values) =>
          val (errors, any) = values.mapValues(toAny).toMap.partition(_._2.isError)
          errors.headOption match {
            case Some(err) => err._2
            case None      => JsSuccess(any.mapValues(_.get))
          }
        case value            => toAny(value)
      }
    }
  }

  implicit val classConfigurationReads: Reads[ClassConfiguration] = (
    (JsPath \ "classname" ).read[String] and
    (JsPath \ "parameters").read[Map[String, Any]] and
    (JsPath \ "name").readNullable[String]
  )(ClassConfiguration.apply _)

  implicit val datasetConfigurationReads: Reads[DatasetConfiguration] = (
    (JsPath \ "reader"      ).read[ClassConfiguration]      and
    (JsPath \ "transformers").read[Seq[ClassConfiguration]] and
    (JsPath \ "splitter"    ).readNullable[ClassConfiguration]
  )(DatasetConfiguration.apply _)

  implicit val algorithmConfigurationReads: Reads[AlgorithmConfiguration] = (
    (JsPath \ "estimator" ).read[ClassConfiguration] and
    (JsPath \ "evaluators").read[Seq[ClassConfiguration]]
  )(AlgorithmConfiguration.apply _)

  implicit val metricConfigurationReads: Reads[MetricConfiguration] =
    (JsPath \ "writer").read[ClassConfiguration]
      .map(MetricConfiguration)

  implicit val metricsConfigurationReads: Reads[MetricsConfiguration] = (
    (JsPath \ "applicative").readNullable[MetricConfiguration] and
    (JsPath \ "spark"      ).readNullable[MetricConfiguration]
  )(MetricsConfiguration.apply _)

  implicit val experimentConfigurationReads: Reads[ExperimentConfiguration] = (
    (JsPath \ "metrics"  ).read[MetricsConfiguration] and
    (JsPath \ "dataset"  ).read[DatasetConfiguration] and
    (JsPath \ "algorithm").read[AlgorithmConfiguration]
  )(ExperimentConfiguration.apply _)
}
