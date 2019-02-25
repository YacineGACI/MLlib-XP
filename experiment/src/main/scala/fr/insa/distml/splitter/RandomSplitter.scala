package fr.insa.distml.splitter

import org.apache.spark.sql.{DataFrame, SparkSession}

class RandomSplitter extends Splitter {

  private var _weights: Array[Double] = _
  private var _seed:    Option[Long]  = None

  def setWeights(weights: Array[Double]): this.type = {
    _weights = weights
    this
  }

  def getWeights: Array[Double] = _weights

  def setSeed(seed: Long): this.type = {
    _seed = Some(seed)
    this
  }

  def getSeed: Option[Long] = _seed

  def split(dataframe: DataFrame)(implicit spark: SparkSession): Array[DataFrame] = {
    _seed match {
      case Some(seed) => dataframe.randomSplit(_weights, seed)
      case None       => dataframe.randomSplit(_weights)
    }
  }
}
