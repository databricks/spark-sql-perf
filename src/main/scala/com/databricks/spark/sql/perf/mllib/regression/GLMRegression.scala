package com.databricks.spark.sql.perf.mllib.regression

import org.apache.spark.ml.evaluation.{Evaluator, RegressionEvaluator}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.GeneralizedLinearRegression
import org.apache.spark.ml.{Estimator, ModelBuilder, Transformer}

import com.databricks.spark.sql.perf.mllib.OptionImplicits._
import com.databricks.spark.sql.perf.mllib._
import com.databricks.spark.sql.perf.mllib.data.DataGenerator


object GLMRegression extends BenchmarkAlgorithm with TestFromTraining with
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
    import ctx.params._
    val rng = ctx.newGenerator()
    val coefficients =
      Vectors.dense(Array.fill[Double](ctx.params.numFeatures)(2 * rng.nextDouble() - 1))
    // Small intercept to prevent some skew in the data.
    val intercept = 0.01 * (2 * rng.nextDouble - 1)
    val m = ModelBuilder.newGLR(coefficients, intercept)
    m.set(m.link, link.get)
    m.set(m.family, family.get)
    m
  }

  override def getEstimator(ctx: MLBenchContext): Estimator[_] = {
    import ctx.params._
    new GeneralizedLinearRegression()
      .setLink(link)
      .setFamily(family)
      .setRegParam(regParam)
      .setMaxIter(maxIter)
      .setTol(tol)
  }

  override protected def evaluator(ctx: MLBenchContext): Evaluator =
    new RegressionEvaluator()
}
