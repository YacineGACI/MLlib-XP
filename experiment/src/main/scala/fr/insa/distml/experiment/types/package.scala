package fr.insa.distml.experiment

import org.apache.spark.ml.{Estimator, Model}
import scala.language.existentials

package object types {
  type AnyModel     =     Model[M] forSome {type M <: Model[M]}
  type AnyEstimator = Estimator[M] forSome {type M <: AnyModel}
}
