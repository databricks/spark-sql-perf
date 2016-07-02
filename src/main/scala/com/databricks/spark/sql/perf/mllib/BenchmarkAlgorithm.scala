package com.databricks.spark.sql.perf.mllib

import com.typesafe.scalalogging.slf4j.Logging

import org.apache.spark.ml.{Estimator, Transformer}
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

/**
 * The description of a benchmark for an ML algorithm. It follows a simple, standard proceduce:
 *  - generate some test and training data
 *  - generate a model against the training data
 *  - score the model against the training data
 *  - score the model against the test data
 *
 * You should not assume that your implementation can carry state around. If some state is needed,
 * consider adding it to the context.
 *
 * It is assumed that the implementation is going to be an object.
 */
trait BenchmarkAlgorithm extends Logging {

  def trainingDataSet(ctx: MLBenchContext): DataFrame

  def testDataSet(ctx: MLBenchContext): DataFrame

  /**
   * Create an [[Estimator]] with params set from the given [[MLBenchContext]].
   */
  def getEstimator(ctx: MLBenchContext): Estimator[_]

  /**
   * The unnormalized score of the training procedure on a dataset. The normalization is
   * performed by the caller.
   */
  @throws[Exception]("if scoring fails")
  def score(
      ctx: MLBenchContext,
      testSet: DataFrame,
      model: Transformer): Double = -1.0 // Not putting NaN because it is not valid JSON.

  def name: String = {
    this.getClass.getCanonicalName.replace("$", "")
  }
}

/**
 * Uses an evaluator to perform the scoring.
 */
trait ScoringWithEvaluator {
  self: BenchmarkAlgorithm =>

  protected def evaluator(ctx: MLBenchContext): Evaluator

  final override def score(
      ctx: MLBenchContext,
      testSet: DataFrame,
      model: Transformer): Double = {
    val eval = model.transform(testSet)
    evaluator(ctx).evaluate(eval)
  }
}

/**
 * Builds the training set for an initial dataset and an initial model. Useful for validating a
 * trained model against a given model.
 */
trait TrainingSetFromTransformer {
  self: BenchmarkAlgorithm =>

  protected def initialData(ctx: MLBenchContext): DataFrame

  protected def trueModel(ctx: MLBenchContext): Transformer

  final override def trainingDataSet(ctx: MLBenchContext): DataFrame = {
    val initial = initialData(ctx)
    val model = trueModel(ctx)
    model.transform(initial).select(col("features"), col("prediction").as("label"))
  }
}

/**
 * The test data is the same as the training data.
 */
trait TestFromTraining {
  self: BenchmarkAlgorithm =>

  final override def testDataSet(ctx: MLBenchContext): DataFrame = {
    // Copy the context with a new seed.
    val ctx2 = ctx.params.randomSeed match {
      case Some(x) =>
        // Also set the number of examples to the number of test examples.
        assert(ctx.params.numTestExamples.nonEmpty, "You must specify test examples")
        val p = ctx.params.copy(randomSeed = Some(x + 1), numExamples = ctx.params.numTestExamples)
        ctx.copy(params = p)
      case None =>
        // Making a full copy to reset the internal seed.
        ctx.copy()
    }
    self.trainingDataSet(ctx2)
  }
}

