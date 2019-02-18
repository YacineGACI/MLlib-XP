package fr.insa.distml.experiment

import ch.cern.sparkmeasure.TaskMetrics
import fr.insa.distml.reader.Reader
import fr.insa.distml.splitter.Splitter
import fr.insa.distml.writer.Writer
import org.apache.spark.ml.{Estimator, Pipeline, PipelineStage, Transformer}
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.sql.SparkSession
import scala.collection.immutable._
import scala.reflect.ClassTag

object Experiment {

  private def asInstanceOfOption[T: ClassTag](o: Any): Option[T] = {
    Some(o) collect { case m: T => m }
  }

  private def castArray[T: ClassTag](array: Array[_]): Array[T] = {
    array
      .map(asInstanceOfOption[T])
      .map(_.getOrElse(throw new IllegalArgumentException("Content must be of the same type in Array")))
  }

  private def newParameterizedInstance[C: ClassTag](classname: String, parameters: Map[String, Any]): C = {

    val obj =
      asInstanceOfOption[C](Class.forName(classname).getConstructor().newInstance())
        .getOrElse(throw new IllegalArgumentException("Invalid class type"))

    for((key, value) <- parameters) {

      val (param: AnyRef, cls: Class[Any]) = value match {
        case o: java.lang.Integer => (o, classOf[Int    ])
        case o: java.lang.Double  => (o, classOf[Double ])
        case o: java.lang.Boolean => (o, classOf[Boolean])
        case o: java.lang.String  => (o, classOf[String ])
        case o: Array[_]          =>
          o.headOption match {
            case Some(_: java.lang.Integer) => (castArray[Int    ](o), classOf[Array[Int    ]])
            case Some(_: java.lang.Double)  => (castArray[Double ](o), classOf[Array[Double ]])
            case Some(_: java.lang.Boolean) => (castArray[Boolean](o), classOf[Array[Boolean]])
            case Some(_: java.lang.String)  => (castArray[String ](o), classOf[Array[String ]])
            case Some(_) => throw new IllegalArgumentException("Type must be Int|Double|Boolean|String in Array")
            case None    => throw new IllegalArgumentException("Array must not be empty to deduce the content type")
          }
        case o: Map[_, _]         => (o, classOf[Map[_, _]])
        case _                    => throw new IllegalArgumentException("Type must be Int|Double|Boolean|String|Array|Map")
      }

      val method = obj.getClass.getMethod("set" + key.capitalize, cls)

      method.invoke(obj, param)
    }

    obj
  }

  private def fromClassConfiguration[C: ClassTag](config: ClassConfiguration): C = {
    newParameterizedInstance[C](config.classname, config.parameters)
  }

  private def time[R](block: => R): (R, Long) = {
    val start = System.currentTimeMillis()
    val result = block
    val end = System.currentTimeMillis()
    (result, end - start)
  }

  def startExperiment(conf: ExperimentConfiguration): Unit = {

    val appliWriter  = conf.metrics.appli.map(_.writer).map(fromClassConfiguration[Writer])
    val sparkWriter  = conf.metrics.spark.map(_.writer).map(fromClassConfiguration[Writer])
    val reader       = fromClassConfiguration[Reader](conf.dataset.reader)
    val estimator    = fromClassConfiguration[Estimator[_]](conf.algorithm.estimator)
    val splitter     = conf.dataset.splitter.map(fromClassConfiguration[Splitter])
    val transformers = conf.dataset.transformers.map(cls => fromClassConfiguration[Transformer](cls))
    val evaluators   = conf.algorithm.evaluators.map(cls => (
      fromClassConfiguration[Evaluator](cls),
      cls.name.getOrElse(throw new IllegalArgumentException("Missing name for evaluator"))
    ))

    val preprocessing = new Pipeline().setStages(transformers.toArray[PipelineStage])
    val learning      = new Pipeline().setStages(Array[PipelineStage](estimator))

    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()

    implicit val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()

    val sparkMetrics = sparkWriter.map(_ => TaskMetrics(spark))

    sparkMetrics.foreach(_.begin())

    val raw = reader.read()
    val preprocessor = preprocessing.fit(raw)
    val data = preprocessor.transform(raw)

    val Array(train, test) =
      splitter
        .map(_.split(data))
        .getOrElse(Array(data, data))

    val (model, fitTime) = time {
      learning.fit(train)
    }

    val (predictions, transformTime) = time {
      model.transform(test)
    }

    sparkMetrics.foreach(_.end())

    val appliMetrics = appliWriter.map(_ =>
      (for((evaluator, name) <- evaluators)
        yield         name -> evaluator.evaluate(predictions)).toMap
        + (      "fitTime" ->       fitTime.toDouble)
        + ("transformTime" -> transformTime.toDouble)
    )

    import spark.implicits._

    sparkMetrics.foreach(metrics => sparkWriter.foreach(_.write(metrics.createTaskMetricsDF())))
    appliMetrics.foreach(metrics => appliWriter.foreach(_.write(metrics.toSeq.toDF("name", "value"))))

    spark.stop()
  }
}

case class ExperimentConfiguration(  metrics: MetricsConfiguration, dataset: DatasetConfiguration, algorithm: AlgorithmConfiguration)
case class    MetricsConfiguration(    appli: Option[MetricConfiguration], spark: Option[MetricConfiguration])
case class     MetricConfiguration(   writer:   ClassConfiguration)
case class    DatasetConfiguration(   reader:   ClassConfiguration, transformers: Seq[ClassConfiguration], splitter: Option[ClassConfiguration])
case class  AlgorithmConfiguration(estimator:   ClassConfiguration,   evaluators: Seq[ClassConfiguration])
case class      ClassConfiguration(classname: String, parameters: Map[String, Any], name: Option[String])
