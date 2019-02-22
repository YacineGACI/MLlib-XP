package fr.insa.distml.experiment

import ch.cern.sparkmeasure.TaskMetrics

import fr.insa.distml.reader.Reader
import fr.insa.distml.splitter.Splitter
import fr.insa.distml.writer.Writer

import org.apache.spark.ml._
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkConf

import scala.collection.immutable._
import scala.reflect.ClassTag
import scala.language.existentials

import fr.insa.distml.experiment.types._
import fr.insa.distml.utils._

object Experiment {

  /*
  * Create a new parameterized instance from a class configuration.
  * */
  private def fromClassConfig[T: ClassTag](config: ClassConfig): T = {
    newParameterizedInstance[T](config.classname, config.parameters)
  }

  /*
  * Create a new experiment based on the specified configuration for this run.
  * */
  def from(conf: ExperimentConfig): Experiment = {

    // Create beans to be used in the experiment.
    val Array(appliWriter, sparkWriter) =
      Array(conf.metrics.appli, conf.metrics.spark)
        .map(_.map(_.writer).map(fromClassConfig[Writer]))

    val Array(transformers, preprocessors, postprocessors) =
      Array(conf.workflow.transformers, conf.workflow.preprocessors, conf.workflow.postprocessors)
        .map(_.map(fromClassConfig[PipelineStage]))
        .map(stages => new Pipeline().setStages(stages))

    val reader     = fromClassConfig[Reader](conf.workflow.reader)
    val estimator  = fromClassConfig[AnyEstimator](conf.workflow.estimator)

    val splitter   = conf.workflow.splitter.map(fromClassConfig[Splitter])
    val evaluators = conf.workflow.evaluators.map(fromClassConfig[Evaluator])
    val names      = conf.workflow.evaluators.map(_.name.getOrElse(throw new IllegalArgumentException("Missing name for evaluator")))

    val storage    = cast[StorageLevel](StorageLevel.getClass.getMethod(conf.execution.storage).invoke(StorageLevel))

    val sparkConf  = new SparkConf().setAll(conf.spark)

    // Inject beans and create a new experiment.
    new Experiment(storage, conf.execution.lazily, sparkConf, appliWriter, sparkWriter,
      reader, transformers, splitter, preprocessors, estimator, postprocessors, names.zip(evaluators))
  }
}

class Experiment(
  storage: StorageLevel, lazily: Boolean, sparkConf: SparkConf, appliWriter: Option[Writer], sparkWriter: Option[Writer],
  reader: Reader, transformers: Pipeline, splitter: Option[Splitter], preprocessors: Pipeline,
  estimator: AnyEstimator, postprocessors: Pipeline, evaluators: Array[(String, Evaluator)]
) {

  def perform(): Unit = {
    withNewSparkSession(sparkConf, implicit session => execute)
  }

  private def execute()(implicit session: SparkSession): Unit = {
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
    val sparkMetrics = sparkWriter.map(_ => TaskMetrics(session))
    sparkMetrics.foreach(_.begin())

    // Fit on train data and transform test data
    val (      model,       fitTime) = time { estimator.fit(train) }
    val (predictions, transformTime) = time {
      val predictions = model.transform(test)
      // Force DataFrame computation when collecting metrics if enabled
      if(!lazily)
        predictions.count()
      predictions
    }

    // Stop collect of Spark metrics if enabled.
    sparkMetrics.foreach(_.end())

    // Apply postprocessing.
    val postprocessing = postprocessors.fit(predictions)
    val results = postprocessing.transform(predictions)

    // Persist DataFrame before looping on each evaluator.
    results.persist(storage)

    // Collect applicative metrics if enabled.
    val appliMetrics = appliWriter.map(_ =>
      (for ((name, evaluator) <- evaluators)
        yield               name -> evaluator.evaluate(results)).toMap[String, AnyVal]
        + (            "fitTime" ->              fitTime)
        + (      "transformTime" ->        transformTime)
        + (         "trainCount" ->        train.count())
        + ("resultsFirstRowSize" -> results.first().size)
        + (          "testCount" ->         test.count())
    )

    // Save metrics.
    import session.implicits._
    sparkMetrics.foreach(metrics => sparkWriter.foreach(_.write(metrics.createTaskMetricsDF())))
    appliMetrics.foreach(metrics => appliWriter.foreach(_.write(metrics.toSeq.toDF("name", "value"))))
  }
}

