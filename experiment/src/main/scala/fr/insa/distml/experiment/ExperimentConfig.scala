package fr.insa.distml.experiment

import fr.insa.distml.reader.Reader
import fr.insa.distml.splitter.Splitter
import fr.insa.distml.writer.Writer

import org.apache.spark.SparkConf
import org.apache.spark.ml._
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.storage.StorageLevel

import scala.collection.immutable._
import scala.language.existentials

case class ExperimentConfig(
  sparkConf:     SparkConf,
  execution: ExecutionConfig,
    metrics:   MetricsConfig,
   workflow:  WorkflowConfig
)

case class WorkflowConfig(
          reader: Reader,
    transformers: Pipeline,
        splitter: Option[Splitter],
   preprocessors: Pipeline,
      estimators: Pipeline,
  postprocessors: Pipeline,
      evaluators: Map[String, Evaluator]
)

case class MetricsConfig(
  appli: Option[AppliMetricsConfig],
  spark: Option[SparkMetricsConfig]
)

case class SparkMetricsConfig(
   level: String,
  writer: Writer
)

case class AppliMetricsConfig(
  writer: Writer
)

case class ClassConfig(
   classname: String,
  parameters: Map[String, Any]
)

case class ExecutionConfig(
  storage: StorageLevel,
   lazily: Boolean
)
