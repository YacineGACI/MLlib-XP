package fr.insa.distml.experiment

import fr.insa.distml.metrics.MetricsCollectors
import fr.insa.distml.reader.Reader
import fr.insa.distml.splitter.Splitter
import fr.insa.distml.utils._

import org.apache.spark.ml._
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkConf

import scala.collection.immutable._
import scala.language.existentials

class Experiment(config: ExperimentConfig) {

  val                reader: Reader                     = config.workflow.reader
  val              splitter: Option[Splitter]           = config.workflow.splitter
  val          transformers: Pipeline                   = config.workflow.transformers
  val         preprocessors: Pipeline                   = config.workflow.preprocessors
  val        postprocessors: Pipeline                   = config.workflow.postprocessors
  val            estimators: Pipeline                   = config.workflow.estimators
  val            evaluators: Map[String, Evaluator]     = config.workflow.evaluators

  val                lazily: Boolean                    = config.execution.lazily
  val               storage: StorageLevel               = config.execution.storage

  val    sparkMetricsConfig: Option[SparkMetricsConfig] = config.metrics.spark
  val    appliMetricsConfig: Option[AppliMetricsConfig] = config.metrics.appli

  val             sparkConf: SparkConf                  = config.sparkConf

  def perform(): Unit = {
    withSparkSession(sparkConf, implicit spark => execute)
  }

  private def execute()(implicit spark: SparkSession): Unit = {
    // Reading raw dataset.
    val raw = reader.read()

    // Apply transformation.
    val transformation = transformers.fit(raw)
    val data = transformation.transform(raw)

    // Perform train/test split if enabled.
    val Array(trainData, testData) = splitter.map(_.split(data)).getOrElse(Array(data, data))

    // Apply preprocessing.
    val preprocessing = preprocessors.fit(trainData)
    val Array(train, test) = Array(trainData, testData).map(preprocessing.transform)

    // Persist DataFrame to avoid computation when fitting the model if enabled.
    if(!lazily)
      Array(train, test).foreach(_.persist(storage).count())

    // Start collect of Spark metrics if enabled.
    val collector = sparkMetricsConfig.map(_.level).map(MetricsCollectors.fromLevel)

    ifDefined(collector)(_.begin())

    // Fit on train data and transform test data
    val (      model,       fitTime) = time { estimators.fit(train) }
    val (predictions, transformTime) = time {
      val predictions = model.transform(test)
      // Force DataFrame computation when collecting metrics if enabled
      if(!lazily)
        predictions.count()
      predictions
    }

    // Stop collect of Spark metrics if enabled.
    ifDefined(collector)(_.end())

    // Apply postprocessing.
    val postprocessing = postprocessors.fit(predictions)
    val results = postprocessing.transform(predictions)

    // Persist DataFrame before looping on each evaluator.
    results.persist(storage)

    // Collect applicative metrics if enabled.
    import spark.implicits._
    ifDefined(sparkMetricsConfig.map(_.writer), collector)((writer, collector) => writer.write(collector.collect()))
    ifDefined(appliMetricsConfig.map(_.writer))(writer => {
      val evaluMetrics = evaluators.mapValues(_.evaluate(results))
      val appliMetrics = Map[String, Double](
        "fitTime" -> fitTime, "transformTime" -> transformTime,
        "trainCount" -> train.count(), "testCount" -> test.count(),
        "resultsFirstRowSize" -> results.first().size)
      writer.write((evaluMetrics.toSeq ++ appliMetrics.toSeq).toDF("name", "value"))
    })
  }
}

