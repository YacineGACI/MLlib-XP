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

  private val             reader: Reader                     = config.workflow.reader
  private val           splitter: Option[Splitter]           = config.workflow.splitter
  private val       transformers: Pipeline                   = config.workflow.transformers
  private val      preprocessors: Pipeline                   = config.workflow.preprocessors
  private val     postprocessors: Pipeline                   = config.workflow.postprocessors
  private val         estimators: Pipeline                   = config.workflow.estimators
  private val         evaluators: Map[String, Evaluator]     = config.workflow.evaluators

  private val             lazily: Boolean                    = config.execution.lazily
  private val            storage: StorageLevel               = config.execution.storage

  private val sparkMetricsConfig: Option[SparkMetricsConfig] = config.metrics.spark
  private val appliMetricsConfig: Option[AppliMetricsConfig] = config.metrics.appli

  private val          sparkConf: SparkConf                  = config.sparkConf

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
    if (!lazily)
      Array(train, test).foreach(_.persist(storage).count())

    val collector = sparkMetricsConfig.map(_.level).map(MetricsCollectors.fromLevel)

    // Start collect of Spark metrics if enabled.
    ifDefined(collector)(_.begin())

    // Fit on train data and transform test data
    val (      model,       fitTime) = time { estimators.fit(train) }
    val (predictions, transformTime) = time {
      val predictions = model.transform(test)
      // Force DataFrame computation when collecting metrics if enabled
      if (!lazily)
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

    // Save metrics if enabled.
    import spark.implicits._
    ifDefined(sparkMetricsConfig.map(_.writer), collector)((writer, collector) => writer.write(collector.collect()))
    ifDefined(appliMetricsConfig.map(_.writer))(writer => {
      val evaluMetrics = evaluators.mapValues(_.evaluate(results))
      val appliMetrics = Map[String, Double](
                    "fitTime" ->       fitTime,
              "transformTime" -> transformTime,
                 "trainCount" -> train.count(),
                  "testCount" ->  test.count(),
        "resultsFirstRowSize" -> results.first().size
      )
      writer.write((evaluMetrics ++ appliMetrics).toSeq.toDF("name", "value"))
    })
  }
}

