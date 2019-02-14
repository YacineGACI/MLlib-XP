package fr.insa.distml.experiment

import fr.insa.distml.metrics.{ApplicationMetrics, Metrics}
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, SparkSession}

class EpilepticSeizureRecognition(dataset: String) extends Experiment {

  def execute()(implicit spark: SparkSession): Metrics = {

    val raw = spark.read
      .format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(dataset)

    val assembler = new VectorAssembler()
      .setInputCols((for (i <- List.range(1, 179)) yield s"X$i").toArray)
      .setOutputCol("features")

    val data = assembler.transform(raw.withColumnRenamed("y", "label"))

    val clf = new DecisionTreeClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxDepth(30)

    val Array(train, test) = data.randomSplit(Array(0.8, 0.2))

    val model = clf.fit(train)

    val predictions = model.transform(test)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("f1")

    val f1 = evaluator.evaluate(predictions)

    EpilepticSeizureRecognitionMetrics(trainingTime = 0, f1 = f1)
  }

  def performExperiments(conf: Conf): Unit = {

    for(dataset <- conf.experiments.datasets; algorithm <- conf.experiments.algorithms if dataset.dtypes.contains(algorithm.dtype)) {

      val reader    = Class.forName(     dataset.reader.classname).newInstance().asInstanceOf[Reader]
      val estimator = Class.forName(algorithm.estimator.classname).newInstance().asInstanceOf[Estimator]

      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()

      implicit val spark: SparkSession = SparkSession.builder().getOrCreate()

      val stageMetrics = StageMetrics(spark)

      stageMetrics.begin()

      val data = reader.read()

      val Array(train, test) = algorithm.dtype match {
        case "classification" => data.randomSplit(Array(conf.experiments.split.ratio, 1 - conf.experiments.split.ratio))
        case _                => Array(data, data)
      }

      val model = estimator.fit(train)

      val predictions = model.transform(test)

      stageMetrics.end()

      val evaluator = Class.forName(algorithm.evaluator.classname).newInstance().asInstanceOf[Evaluator]
      evaluator.getClass.getMethod("setFeaturesCol").invoke("features")
      evaluator.getClass.getMethod("setPredictionCol").invoke("prediction")
      evaluator.getClass.getMethod("setLabelCol").invoke("label")

      val appMetrics = evaluator.evaluate(predictions)

      val format = conf.experiments.metrics.format
      val location = conf.experiments.metrics.location

      save(stageMetrics.createStageMetricsDF(), s"$location/spark-metrics", format)
      save(  appMetrics.createAppMetricsDF(),   s"$location/app-metrics",   format)

      spark.stop()
    }
  }
}



case class EpilepticSeizureRecognitionMetrics(trainingTime: Long, f1: Double) extends ApplicationMetrics {

  override def createDF()(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    Seq(this).toDF
  }
}
