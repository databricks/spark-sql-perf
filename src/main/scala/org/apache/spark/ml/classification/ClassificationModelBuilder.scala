package org.apache.spark.ml.classification

import org.apache.spark.ml.linalg.{Matrix, Vector}


object ClassificationModelBuilder {

  def newLinearSVCModel(
      coefficients: Vector,
      intercept: Double): LinearSVCModel = {
    new LinearSVCModel("linearSVC", coefficients, intercept)
  }
}
