package org.apache.spark.ml.classification

import org.apache.spark.ml.linalg.{Matrix, Vector}

/** Object for building Naive Bayes classifiers. */
object NaiveBayesBuilder {
  def newNaiveBayesModel(pi: Vector, theta: Matrix): NaiveBayesModel = {
    val model = new NaiveBayesModel("naivebayes-uid", pi, theta)
    // TODO(@sid.murching/reviewers) This functionality could be
    // provided in ModelBuilder.scala; however, it seems we need to set the modelType param
    // (a private[classification] member) explicitly. Otherwise, when the NaiveBayesModel
    // constructed here is used, the default parameter setting for modelType
    // is not detected & an exception is thrown.
    model.set(model.modelType, "multinomial")
  }
}
