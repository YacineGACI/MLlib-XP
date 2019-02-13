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
}

case class EpilepticSeizureRecognitionMetrics(trainingTime: Long, f1: Double) extends ApplicationMetrics {

  override def createDF()(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    Seq(this).toDF
  }
}
