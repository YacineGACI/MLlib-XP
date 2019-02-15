package fr.insa.distml

import fr.insa.distml.experiment._
import scopt.OptionParser
import play.api.libs.json._
import play.api.libs.functional.syntax._
import scala.reflect.io.Path
import scala.collection.immutable._


object Main {

  def main(args: Array[String]): Unit = {

    val parser = new OptionParser[Parameters]("DistML") {
      opt[String]("config")
        .required()
        .valueName("<file|json>")
        .action((value, parameters) => parameters.copy(config = value))
        .text("the configuration or the path to the configuration file formatted in json")
    }

    parser.parse(args, Parameters("")) match {
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

  case class Parameters(config: String)

  implicit val anyRead: Reads[Any] = new Reads[Any] {

    def toAny(json: JsValue): JsResult[Any] = {
      json match {
        case JsBoolean(bool) => JsSuccess(bool)
        case JsNumber(value) => JsSuccess(if(value.isValidInt) value.toIntExact else value.doubleValue)
        case JsString(value) => JsSuccess(value)
        case _               => JsError("Can't convert to type Any")
      }
    }

    override def reads(json: JsValue): JsResult[Any] = {

      json match {

        case JsArray(values)  =>
          val (allErrors, anyValues) = values.map(toAny).toList.partition(_.isError)
          if (allErrors.isEmpty) {
            JsSuccess(anyValues.map(_.get))
          } else {
            allErrors.headOption match {
              case Some(errors) => errors
              case None         => throw new NoSuchElementException
            }
          }

        case JsObject(values) =>
          val (allErrors, anyValues) = values.mapValues(toAny).partition(_._2.isError)
          if (allErrors.isEmpty) {
            JsSuccess(anyValues.mapValues(_.get).toMap)
          } else {
            allErrors.headOption match {
              case Some(errors) => errors._2
              case None         => throw new NoSuchElementException
            }
          }

        case value            => toAny(value)
      }
    }
  }

  implicit val classConfigurationReads: Reads[ClassConfiguration] = (
    (JsPath \ "classname" ).read[String] and
    (JsPath \ "parameters").read[Map[String, Any]]
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

  implicit val metricsConfigurationReads: Reads[MetricsConfiguration] = (
    (JsPath \ "writer"     ).read[ClassConfiguration] and
    (JsPath \ "applicative").read[Boolean]            and
    (JsPath \ "spark"      ).read[Boolean]
  )(MetricsConfiguration.apply _)

  implicit val experimentConfigurationReads: Reads[ExperimentConfiguration] = (
    (JsPath \ "metrics"  ).read[MetricsConfiguration] and
    (JsPath \ "dataset"  ).read[DatasetConfiguration] and
    (JsPath \ "algorithm").read[AlgorithmConfiguration]
  )(ExperimentConfiguration.apply _)
}
