package fr.insa.distml.experiment

import scala.collection.immutable._

case class ExperimentConfig(
      spark: Map[String, String],
  execution: ExecutionConfig,
    metrics:   MetricsConfig,
   workflow:  WorkflowConfig
)

case class WorkflowConfig(
          reader:        ClassConfig,
    transformers:  Array[ClassConfig],
        splitter: Option[ClassConfig],
   preprocessors:  Array[ClassConfig],
       estimator:        ClassConfig,
  postprocessors:  Array[ClassConfig],
      evaluators:  Array[ClassConfig]
)

case class MetricsConfig(
  appli: Option[MetricConfig],
  spark: Option[MetricConfig]
)

case class MetricConfig(
    writer: ClassConfig,
  measurer: Option[String]
)

case class ClassConfig(
   classname: String,
  parameters: Map[String, Any],
        name: Option[String]
)

case class ExecutionConfig(
  storage: String,
   lazily: Boolean
)
