package fr.insa.distml.experiment

import ch.cern.sparkmeasure.TaskMetrics
import fr.insa.distml.reader.Reader
import fr.insa.distml.writer.Writer
import org.apache.spark.ml.{Estimator, Pipeline, PipelineStage, Transformer}
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.sql.SparkSession

import scala.collection.immutable._

object Experiment {

  @SuppressWarnings(Array("AsInstanceOf"))
  private def newParameterizedInstance[C](classname: String, parameters: Map[String, Any]): C = {

    val obj = Class.forName(classname).newInstance().asInstanceOf[C]

    for((key, value) <- parameters) {

      val valueClass = value match {
        case _: java.lang.Integer => classOf[Int]
        case _: java.lang.Double  => classOf[Double]
        case _: java.lang.String  => classOf[String]
        case _: Seq[_]            => classOf[Seq[_]]
        case _: Map[_, _]         => classOf[Map[_, _]]
        case _                    => value.getClass
      }

      val method = obj.getClass.getMethod("set" + key.capitalize, valueClass)

      method.invoke(obj, value.asInstanceOf[AnyRef])
    }

    obj
  }

  private def fromClassConfiguration[C](config: ClassConfiguration): C = {
    newParameterizedInstance[C](config.classname, config.parameters)
  }

  private def time[R](block: => R): (R, Long) = {
    val start = System.currentTimeMillis()
    val result = block
    val end = System.currentTimeMillis()
    (result, end - start)
  }

  @SuppressWarnings(Array("AsInstanceOf"))
  def startExperiment(conf: ExperimentConfiguration): Unit = {

    val writer       = fromClassConfiguration[Writer](conf.metrics.writer)
    val reader       = fromClassConfiguration[Reader](conf.dataset.reader)
    val estimator    = fromClassConfiguration[Estimator[_]](conf.algorithm.estimator)
    val transformers = conf.dataset.transformers.map(cls =>  fromClassConfiguration[Transformer](cls))
    val evaluators   = conf.algorithm.evaluators.map(cls => (fromClassConfiguration[Evaluator](cls), cls.parameters("metricName").asInstanceOf[String]))

    val preprocessing = new Pipeline().setStages(transformers.toArray[PipelineStage])
    val training      = new Pipeline().setStages(Array[PipelineStage](estimator))

    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()

    implicit val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()

    val sparkMetrics =
      if(conf.metrics.spark)
        Some(TaskMetrics(spark))
      else None

    sparkMetrics.foreach(_.begin())

    val raw = reader.read()
    val preprocessor = preprocessing.fit(raw)
    val data = preprocessor.transform(raw)

    val (model, fitTime) = time {
      training.fit(data)
    }

    val (results, transformTime) = time {
      model.transform(data)
    }

    sparkMetrics.foreach(_.end())

    val appliMetrics =
      if(conf.metrics.applicative)
        Some(
          (for((evaluator, metricName) <- evaluators)
            yield   metricName -> evaluator.evaluate(results)).toMap
            + (      "fitTime" ->       fitTime.toDouble)
            + ("transformTime" -> transformTime.toDouble)
        )
      else None

    import spark.implicits._

    sparkMetrics.foreach(metrics => writer.write(metrics.createTaskMetricsDF()))
    //appliMetrics.foreach(metrics => writer.write(metrics.toSeq.toDF("metricName", "value")))

    spark.stop()
  }
}

case class ExperimentConfiguration(  metrics: MetricsConfiguration, dataset: DatasetConfiguration, algorithm: AlgorithmConfiguration)
case class    MetricsConfiguration(   writer:   ClassConfiguration, applicative: Boolean, spark: Boolean)
case class    DatasetConfiguration(   reader:   ClassConfiguration, transformers: Seq[ClassConfiguration], splitter: Option[ClassConfiguration])
case class  AlgorithmConfiguration(estimator:   ClassConfiguration,   evaluators: Seq[ClassConfiguration])
case class      ClassConfiguration(classname: String, parameters: Map[String, Any])
