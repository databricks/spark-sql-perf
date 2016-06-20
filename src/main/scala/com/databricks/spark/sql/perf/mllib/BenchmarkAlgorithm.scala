package com.databricks.spark.sql.perf.mllib

import com.typesafe.scalalogging.slf4j.Logging

import org.apache.spark.ml.Transformer
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

  // This is internal to the implementation of the benchmark.
  type Model <: Transformer

  def trainingDataSet(ctx: ClassificationContext): DataFrame

  def testDataSet(ctx: ClassificationContext): DataFrame

  @throws[Exception]("if training fails")
  def train(ctx: ClassificationContext,
            trainingSet: DataFrame): Model

  @throws[Exception]("if scoring fails")
  def score(ctx: ClassificationContext,
            testSet: DataFrame, model: Model): Double
}

/**
 * Uses an evaluator to perform the scoring.
 */
trait ScoringWithEvaluator {
  self: BenchmarkAlgorithm =>

  protected def evaluator(ctx: ClassificationContext): Evaluator

  final override def score(
      ctx: ClassificationContext,
      testSet: DataFrame,
      model: Model): Double = {
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

  protected def initialData(ctx: ClassificationContext): DataFrame

  protected def initialModel(ctx: ClassificationContext): Model

  final override def trainingDataSet(ctx: ClassificationContext): DataFrame = {
    val initial = initialData(ctx)
    val model = initialModel(ctx)
    model.transform(initial).select(col("features"), col("prediction").as("label"))
  }
}

/**
 * The test data is the same as the training data.
 */
trait TestFromTraining {
  self: BenchmarkAlgorithm =>

  final override def testDataSet(ctx: ClassificationContext): DataFrame = {
    // Copy the context with a new seed.
    val ctx2 = ctx.commonParams.randomSeed match {
      case Some(x) =>
        val p = ctx.commonParams.copy(randomSeed = Some(x + 1))
        ctx.copy(commonParams = p)
      case None =>
        // Making a full copy to reset the internal seed.
        ctx.copy()
    }
    self.trainingDataSet(ctx2)
  }
}

