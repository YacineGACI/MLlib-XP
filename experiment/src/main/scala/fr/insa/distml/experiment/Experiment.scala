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

  /*
  * Similar to asInstanceOf, but return an Option when the casting failed.
  * */
  private def asInstanceOfOption[T: ClassTag](o: Any): Option[T] = Some(o) collect { case m: T => m }

  /*
  * Create a new parameterized instance of a class by calling its setter methods after instantiation.
  * We do not support setter methods with more than one argument since we use a Map[String, Any] instead of a Map[String, Array[Any]].
  * We do not support setter methods which take a boxed AnyVal (java.lang.Integer).
  * */
  private def newParameterizedInstance[C: ClassTag](classname: String, parameters: Map[String, Any]): C = {

    // Create new instance
    val obj = asInstanceOfOption[C](
      Class.forName(classname).getConstructor().newInstance()
    ).getOrElse(throw new IllegalArgumentException("Invalid class type"))

    for((key, value) <- parameters) {

      // Un-boxing Java Object to Scala AnyVal
      val cls = value match {
        case _: java.lang.Integer => classOf[Int]
        case _: java.lang.Double  => classOf[Double]
        case _: java.lang.Boolean => classOf[Boolean]
        case _: Map[_, _]         => classOf[Map[_, _]] // To interpret a Map$Map1 type as a Map type
        case o                    => o.getClass
      }

      // Invoke setter method to configure the instance
      val method = obj.getClass.getMethod("set" + key.capitalize, Array[Class[_]](cls):_*)
      method.invoke(obj, Array[AnyRef](asInstanceOfOption[AnyRef](value).getOrElse(throw new RuntimeException)):_*) // Auto-boxing AnyVal to AnyRef (Object)
    }

    obj
  }

  /*
  * Cast an array of type Array[_] to an array of type Array[T] along with its contents.
  * */
  private def castArray[T: ClassTag](array: Array[_]): Array[T] = {
    array.map(asInstanceOfOption[T]).map(_.getOrElse(throw new IllegalArgumentException("Failed to cast array content")))
  }

  /*
  * Cast a parameter of type Any to a compatible Java type for setter methods.
  * This method is primarily used to cast Object[] arrays to int[] arrays (or the corresponding type of the first element, thus array must not be empty).
  * This is due to the fact that Json arrays can contain anything when parsing.
  * */
  private def castParameter(parameter: Any): Any = {
    parameter match {
      case o: Array[_] =>
        o.headOption match {
          case Some(_: java.lang.Integer) => castArray[Int    ](o)
          case Some(_: java.lang.Double)  => castArray[Double ](o)
          case Some(_: java.lang.Boolean) => castArray[Boolean](o)
          case Some(_: java.lang.String)  => castArray[String](o)
          case Some(_)                    => o
          case None                       => throw new IllegalArgumentException("Array must not be empty to infer type")
        }
      case o           => o
    }
  }

  /*
  * Create a new parameterized instance from a class configuration.
  * */
  private def fromClassConfiguration[C: ClassTag](config: ClassConfiguration): C = {
    newParameterizedInstance[C](config.classname, config.parameters.mapValues(castParameter))
  }

  /*
  * Surround a call-by-name block of code with a timer.
  * */
  private def time[R](block: => R): (R, Long) = {
    val start  = System.currentTimeMillis()
    val result = block
    val end    = System.currentTimeMillis()
    (result, end - start)
  }

  /*
  * Perform a new experiment based on the specified configuration for this run.
  * */
  def perform(conf: ExperimentConfiguration): Unit = {

    // Create beans to be used in the experiment
    val appliWriter  = conf.metrics.appli.map(_.writer).map(fromClassConfiguration[Writer])
    val sparkWriter  = conf.metrics.spark.map(_.writer).map(fromClassConfiguration[Writer])

    val reader       = fromClassConfiguration[Reader](conf.dataset.reader)
    val transformers = conf.dataset.transformers.map(fromClassConfiguration[Transformer])
    val splitter     = conf.dataset.splitter.map(fromClassConfiguration[Splitter])

    val estimator    = fromClassConfiguration[Estimator[_]](conf.algorithm.estimator)
    val evaluators   = conf.algorithm.evaluators.map(cls => (fromClassConfiguration[Evaluator](cls), cls.name.getOrElse(throw new IllegalArgumentException("Missing name for evaluator"))))

    // Create pipelines
    val preprocessing = new Pipeline().setStages(transformers.toArray[PipelineStage])
    val learning      = new Pipeline().setStages(Array[PipelineStage](estimator))

    // Create a fresh Spark session
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    implicit val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()

    // Start collect of Spark metrics if enabled
    val sparkMetrics = sparkWriter.map(_ => TaskMetrics(spark))
    sparkMetrics.foreach(_.begin())

    // Reading raw dataset and apply preprocessing pipeline on it
    val raw          = reader.read()
    val preprocessor = preprocessing.fit(raw)
    val data         = preprocessor.transform(raw)

    // Perform train/test split if enabled
    val Array(train, test) = splitter.map(_.split(data)).getOrElse(Array(data, data))

    // Fit on train data and transform test data
    val (      model,       fitTime) = time { learning.fit(train)   }
    val (predictions, transformTime) = time { model.transform(test) } // TODO: See if DataFrame are lazily evaluated

    // Stop collect of Spark metrics if enabled
    sparkMetrics.foreach(_.end())

    // Collect applicative metrics if enabled
    val appliMetrics = appliWriter.map(_ =>
      (for((evaluator, name) <- evaluators)
        yield         name -> evaluator.evaluate(predictions)).toMap
        + (      "fitTime" ->       fitTime.toDouble)
        + ("transformTime" -> transformTime.toDouble)
    )

    // Save metrics
    import spark.implicits._
    sparkMetrics.foreach(metrics => sparkWriter.foreach(_.write(metrics.createTaskMetricsDF())))
    appliMetrics.foreach(metrics => appliWriter.foreach(_.write(metrics.toSeq.toDF("name", "value"))))

    // Stop Spark session
    spark.stop()
  }
}

/*
* Configuration DTO.
* */
case class ExperimentConfiguration(  metrics: MetricsConfiguration, dataset: DatasetConfiguration, algorithm: AlgorithmConfiguration)
case class    MetricsConfiguration(    appli: Option[MetricConfiguration], spark: Option[MetricConfiguration])
case class     MetricConfiguration(   writer:   ClassConfiguration)
case class    DatasetConfiguration(   reader:   ClassConfiguration, transformers: Seq[ClassConfiguration], splitter: Option[ClassConfiguration])
case class  AlgorithmConfiguration(estimator:   ClassConfiguration,   evaluators: Seq[ClassConfiguration])
case class      ClassConfiguration(classname: String, parameters: Map[String, Any], name: Option[String])
