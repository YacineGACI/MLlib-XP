package fr.insa.distml.splitter

import org.apache.spark.sql.{DataFrame, SparkSession}

class RandomSplitter extends Splitter {

  private var _weights:  Array[Double] = _

  def setWeights(weights: Array[Double]): this.type = {
    _weights = weights
    this
  }

  def getWeights: Array[Double] = _weights

  def split(dataframe: DataFrame)(implicit spark: SparkSession): Array[DataFrame] = {
    dataframe.randomSplit(_weights)
  }
}
