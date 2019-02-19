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
import scala.util.{Try, Success, Failure}

object Experiment {

  /*
  * Similar to asInstanceOf, but return an Option when the casting failed.
  * */
  private def asInstanceOfOption[T: ClassTag](o: Any): Option[T] = Some(o) collect { case m: T => m }

  /*
  * Cast an array of type Array[_] to an array of type Array[T] along with its contents.
  * */
  private def castArray[T: ClassTag](array: Array[_]): Array[T] = {
    array.map(asInstanceOfOption[T]).map(_.getOrElse(throw new IllegalArgumentException("Failed to cast Array content")))
  }

  /*
  * Create a new parameterized instance of a class by calling its setter methods after instantiation.
  * */
  private def newParameterizedInstance[C: ClassTag](classname: String, parameters: Map[String, Any]): C = {

    // Create new instance
    val obj = asInstanceOfOption[C](Class.forName(classname)
      .getConstructor().newInstance())
      .getOrElse(throw new IllegalArgumentException("Invalid class type"))

    parameters.get("coalesce").map(_.getClass).foreach(println)
    println(parameters)

    for((key, value) <- parameters) {

      // Conversion from Scala types to Java ones to find and match the corresponding setter method
      val (param: AnyRef, cls: Class[Any]) = value match {

        case o: java.lang.Integer => (o, classOf[Int    ])
        case o: java.lang.Double  => (o, classOf[Double ])
        case o: java.lang.Boolean => (o, classOf[Boolean])
        case o: java.lang.String  => (o, classOf[String ])
        case o: Array[_]          =>

          /*
          * We try to convert arrays of type Array[_] into arrays of type Array[T] with T being the type of the first element.
          * If it failed, we fallback on the default Array[_] type.
          * This step is necessary because the type Array[_] in Scala is translated into List[Object] in Java, but the type Array[Int] is translated into int[].
          * */

          def convert[T: ClassTag](obj: Array[_]): (Any, Class[_]) = {
            Try(castArray[T](obj)) match {
              case Success(array) =>
                val a = (array, classOf[Array[T]])
                val d = array.getClass
                a
              case Failure(_)     => (obj,   classOf[Array[_]])
            }
          }

          o.headOption match {
            case Some(_: java.lang.Integer) => convert[Int    ](o)
            case Some(_: java.lang.Double)  => convert[Double ](o)
            case Some(_: java.lang.Boolean) => convert[Boolean](o)
            case Some(_: java.lang.String)  => convert[String ](o)
            case _                => (o, classOf[Array[_]])
          }

        case o: Map[_, _]         => (o, classOf[Map[_, _]])
        case o                    => (o, o.getClass)
      }

      // Invoke setter methods to configure the instance
      obj.getClass.getMethod("set" + key.capitalize, cls).invoke(obj, param)
    }

    obj
  }

  /*
  * Create a new parameterized instance from a class configuration.
  * */
  private def fromClassConfiguration[C: ClassTag](config: ClassConfiguration): C = {
    newParameterizedInstance[C](config.classname, config.parameters)
  }

  /*
  * Surround a call-by-name block of code with a timer.
  * */
  private def time[R](block: => R): (R, Long) = {
    val start = System.currentTimeMillis()
    val result = block
    val end = System.currentTimeMillis()
    (result, end - start)
  }

  /*
  * Start a new experiment based on the specified configuration for this run.
  * */
  def startExperiment(conf: ExperimentConfiguration): Unit = {

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
    implicit val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()

    // Start collect of Spark metrics if enabled
    val sparkMetrics = sparkWriter.map(_ => TaskMetrics(spark))
    sparkMetrics.foreach(_.begin())

    // Reading raw dataset and apply preprocessing pipeline on it
    val raw = reader.read()
    val preprocessor = preprocessing.fit(raw)
    val data = preprocessor.transform(raw)

    // Perform train/test split if enabled
    val Array(train, test) = splitter.map(_.split(data)).getOrElse(Array(data, data))

    // Fit on train data and transform test data
    val (model, fitTime) = time { learning.fit(train) }
    val (predictions, transformTime) = time { model.transform(test) }

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
