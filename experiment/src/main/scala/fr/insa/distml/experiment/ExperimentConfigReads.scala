package fr.insa.distml.experiment

import fr.insa.distml.reader.Reader
import fr.insa.distml.writer.Writer
import fr.insa.distml.splitter.Splitter
import fr.insa.distml.utils._

import play.api.libs.json._
import play.api.libs.functional.syntax._

import org.apache.spark.SparkConf
import org.apache.spark.ml._
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.storage.StorageLevel

import scala.collection.immutable._
import scala.reflect.ClassTag

object ExperimentConfigReads {

  /*
  * Create a new parameterized instance from a class configuration.
  * */
  private def fromClassConfig[T: ClassTag](config: ClassConfig): T = {
    newParameterizedInstance[T](config.classname, config.parameters)
  }

  /*
  * Create a new reader by applying a function to an existing reader.
  * */
  private def mapReads[C, T](f: C => T)(implicit rds: Reads[C]): Reads[T] = {
    new Reads[T] {
      override def reads(json: JsValue): JsResult[T] = {
        json.validate[C](rds) match {
          case JsSuccess(value, _) => JsSuccess(f(value))
          case JsError(errors)     => JsError(errors)
        }
      }
    }
  }

  /*
  * Reader for mapping Json into type Any in order to configure a parameterized instance.
  * */
  implicit val anyReads: Reads[Any] = new Reads[Any] {
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
          val (errors, any) = values.mapValues(toAny).toMap.partition(_._2.isError)
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
  }

  /*
  * Readers for mapping Json into type ExperimentConfig.
  * */
  implicit val            classConfigReads: Reads[ClassConfig       ] = (
    (JsPath \ "classname"     ).read[String]                     and
    (JsPath \ "parameters"    ).read[Map[String, Any]]
  )(ClassConfig)

  implicit val    sparkConfReads: Reads[SparkConf   ] = mapReads[Map[String, String], SparkConf   ](x => new SparkConf().setAll(x))
  implicit val storageLevelReads: Reads[StorageLevel] = mapReads[             String, StorageLevel](x => cast[StorageLevel](StorageLevel.getClass.getMethod(x).invoke(StorageLevel)))
  implicit val     pipelineReads: Reads[Pipeline    ] = mapReads[ Array[ClassConfig], Pipeline    ](x => new Pipeline().setStages(x.map(fromClassConfig[PipelineStage])))
  implicit val       readerReads: Reads[Reader      ] = mapReads[        ClassConfig, Reader      ](fromClassConfig[Reader   ])
  implicit val       writerReads: Reads[Writer      ] = mapReads[        ClassConfig, Writer      ](fromClassConfig[Writer   ])
  implicit val     splitterReads: Reads[Splitter    ] = mapReads[        ClassConfig, Splitter    ](fromClassConfig[Splitter ])
  implicit val    evaluatorReads: Reads[Evaluator   ] = mapReads[        ClassConfig, Evaluator   ](fromClassConfig[Evaluator])

  implicit val     sparkMetricsConfigReads: Reads[SparkMetricsConfig] = (
    (JsPath \ "level"         ).read[String]                     and
    (JsPath \ "writer"        ).read[Writer]
  )(SparkMetricsConfig)

  implicit val     appliMetricsConfigReads: Reads[AppliMetricsConfig] =
    (JsPath \ "writer"        ).read[Writer]
      .map(AppliMetricsConfig)

  implicit val          metricsConfigReads: Reads[MetricsConfig     ] = (
    (JsPath \ "applicative"   ).readNullable[AppliMetricsConfig] and
    (JsPath \ "spark"         ).readNullable[SparkMetricsConfig]
  )(MetricsConfig)

  implicit val        executionConfigReads: Reads[ExecutionConfig   ] = (
    (JsPath \ "storage"       ).read[StorageLevel]               and
    (JsPath \ "lazily"        ).read[Boolean     ]
  )(ExecutionConfig)

  implicit val         workflowConfigReads: Reads[WorkflowConfig    ] = (
    (JsPath \ "reader"        ).read[Reader  ]                   and
    (JsPath \ "transformers"  ).read[Pipeline]                   and
    (JsPath \ "splitter"      ).readNullable[Splitter]           and
    (JsPath \ "preprocessors" ).read[Pipeline]                   and
    (JsPath \ "estimators"    ).read[Pipeline]                   and
    (JsPath \ "postprocessors").read[Pipeline]                   and
    (JsPath \ "evaluators"    ).read[Map[String, Evaluator]]
  )(WorkflowConfig)

  implicit val       experimentConfigReads: Reads[ExperimentConfig  ] = (
    (JsPath \ "spark"         ).read[    SparkConf  ]            and
    (JsPath \ "execution"     ).read[ExecutionConfig]            and
    (JsPath \ "metrics"       ).read[  MetricsConfig]            and
    (JsPath \ "workflow"      ).read[ WorkflowConfig]
  )(ExperimentConfig)
}
