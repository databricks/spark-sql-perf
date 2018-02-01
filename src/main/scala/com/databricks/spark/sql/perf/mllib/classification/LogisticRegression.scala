package com.databricks.spark.sql.perf.mllib.classification

import com.databricks.spark.sql.perf.mllib.OptionImplicits._
import com.databricks.spark.sql.perf.mllib._
import com.databricks.spark.sql.perf.mllib.data.DataGenerator
import org.apache.spark.ml.evaluation.{Evaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.{Estimator, ModelBuilderSSP, PipelineStage, Transformer}
import org.apache.spark.ml
import org.apache.spark.ml.linalg.Vectors


object LogisticRegression extends BenchmarkAlgorithm
  with TestFromTraining with TrainingSetFromTransformer with ScoringWithEvaluator {

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
    ModelBuilderSSP.newLogisticRegressionModel(coefficients, intercept)
  }

  override def getPipelineStage(ctx: MLBenchContext): PipelineStage = {
    import ctx.params._
    new ml.classification.LogisticRegression()
      .setTol(tol)
      .setMaxIter(maxIter)
      .setRegParam(regParam)
  }

  override protected def evaluator(ctx: MLBenchContext): Evaluator =
    new MulticlassClassificationEvaluator()
}

