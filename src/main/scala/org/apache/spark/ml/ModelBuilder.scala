package org.apache.spark.ml

import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.regression.GeneralizedLinearRegressionModel

/**
 * Helper for creating MLlib models which have private constructors.
 */
object ModelBuilder {

  def newLogisticRegressionModel(
      coefficients: Vector,
      intercept: Double): LogisticRegressionModel = {
    new LogisticRegressionModel("lr", coefficients, intercept)
  }

  def newGLR(
      coefficients: Vector,
      intercept: Double): GeneralizedLinearRegressionModel =
    new GeneralizedLinearRegressionModel("glr-uid", coefficients, intercept)
}