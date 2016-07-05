package com.databricks.spark.sql.perf.mllib.regression

import org.apache.spark.ml
import org.apache.spark.ml.evaluation.{Evaluator, RegressionEvaluator}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.{Estimator, ModelBuilder, Transformer}

import com.databricks.spark.sql.perf.mllib.OptionImplicits._
import com.databricks.spark.sql.perf.mllib._
import com.databricks.spark.sql.perf.mllib.data.DataGenerator


object LinearRegression extends BenchmarkAlgorithm with TestFromTraining with
  TrainingSetFromTransformer with ScoringWithEvaluator {

  override protected def initialData(ctx: MLBenchContext) = {
    import ctx.params._
    DataGenerator.generateContinuousFeatures(
      ctx.sqlContext,
      numExamples,
      ctx.seed(),
      numPartitions,
      numFeatures)
  }

  override protected def trueModel(ctx: MLBenchContext): Transformer = {
    val rng = ctx.newGenerator()
    val coefficients =
      Vectors.dense(Array.fill[Double](ctx.params.numFeatures)(2 * rng.nextDouble() - 1))
    // Small intercept to prevent some skew in the data.
    val intercept = 0.01 * (2 * rng.nextDouble - 1)
    ModelBuilder.newLinearRegressionModel(coefficients, intercept)
  }

  override def getEstimator(ctx: MLBenchContext): Estimator[_] = {
    import ctx.params._
    new ml.regression.LinearRegression()
      .setSolver("l-bfgs")
      .setRegParam(regParam)
      .setMaxIter(maxIter)
      .setTol(tol)
  }

  override protected def evaluator(ctx: MLBenchContext): Evaluator =
    new RegressionEvaluator()
}
