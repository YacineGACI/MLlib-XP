package fr.insa.distml.experiment

import play.api.libs.json._
import play.api.libs.functional.syntax._
import scala.collection.immutable._
import fr.insa.distml.utils._

object ExperimentConfigReads {

  /*
  * Reader for mapping Json into type Any.
  * */
  implicit val anyReads: Reads[Any] = new Reads[Any] {

    private def toAny(json: JsValue): JsResult[Any] = {
      json match {
        case JsNumber(value) =>
          if (value.isValidInt) JsSuccess(value.toIntExact)
          else                  JsSuccess(value.doubleValue)
        case JsBoolean(bool) => JsSuccess(bool)
        case JsString(value) => JsSuccess(value)
        case _               => JsError("Type must be Int | Double | Boolean | String")
      }
    }

    override def reads(json: JsValue): JsResult[Any] = {
      json match {
        // Json arrays are converted into type Array[Int] | Array[Double] | Array[Boolean] | Array[String]
        case JsArray(values)  =>
          val (errors, any) = values.map(toAny).toArray.partition(_.isError)
          errors.headOption match {
            case Some(err) => err
            case None      =>
              val array = any.map(_.get)
              array.headOption match {
                case Some(_: java.lang.Integer) => JsSuccess(castArray[Int    ](array))
                case Some(_: java.lang.Double ) => JsSuccess(castArray[Double ](array))
                case Some(_: java.lang.Boolean) => JsSuccess(castArray[Boolean](array))
                case Some(_: java.lang.String ) => JsSuccess(castArray[String ](array))
                case Some(_)                    => JsError("Type must be Int | Double | Boolean | String")
                case None                       => JsError("Array must not be empty to infer type")
              }
          }
        // Json objects are converted into type Map[String, _]
        case JsObject(values) =>
          val (errors, any) = values.mapValues(toAny).toMap.partition(_._2.isError) // toMap is used to convert from mutable.Map to immutable.Map
          errors.headOption match {
            case Some(err) => err._2
            case None      =>
              val dict = any.mapValues(_.get)
              JsSuccess(dict)
          }
        // Json values are converted into type Int | Double | Boolean | String
        case value            => toAny(value)
      }
    }
  }

  /*
  * Readers for mapping Json into type ExperimentConfig.
  * */
  implicit val      classConfigReads: Reads[ClassConfig     ] = (
    (JsPath \ "classname"     ).read[String]               and
    (JsPath \ "parameters"    ).read[Map[String, Any]]     and
    (JsPath \ "name"          ).readNullable[String]
  )(ClassConfig)

  implicit val     metricConfigReads: Reads[MetricConfig    ] = (
    (JsPath \ "writer"        ).read[ClassConfig]          and
    (JsPath \ "measurer"      ).readNullable[String]
  )(MetricConfig)

  implicit val    metricsConfigReads: Reads[MetricsConfig   ] = (
    (JsPath \ "applicative"   ).readNullable[MetricConfig] and
    (JsPath \ "spark"         ).readNullable[MetricConfig]
  )(MetricsConfig)

  implicit val  executionConfigReads: Reads[ExecutionConfig ] = (
    (JsPath \ "storage"       ).read[String]               and
    (JsPath \ "lazily"        ).read[Boolean]
  )(ExecutionConfig)

  implicit val   workflowConfigReads: Reads[WorkflowConfig  ] = (
    (JsPath \ "reader"        ).read[      ClassConfig]    and
    (JsPath \ "transformers"  ).read[Array[ClassConfig]]   and
    (JsPath \ "splitter"      ).readNullable[ClassConfig]  and
    (JsPath \ "preprocessors" ).read[Array[ClassConfig]]   and
    (JsPath \ "estimator"     ).read[      ClassConfig]    and
    (JsPath \ "postprocessors").read[Array[ClassConfig]]   and
    (JsPath \ "evaluators"    ).read[Array[ClassConfig]]
  )(WorkflowConfig)

  implicit val experimentConfigReads: Reads[ExperimentConfig] = (
    (JsPath \ "spark"         ).read[Map[String, String]]  and
    (JsPath \ "execution"     ).read[ExecutionConfig]      and
    (JsPath \ "metrics"       ).read[  MetricsConfig]      and
    (JsPath \ "workflow"      ).read[ WorkflowConfig]
  )(ExperimentConfig)
}
