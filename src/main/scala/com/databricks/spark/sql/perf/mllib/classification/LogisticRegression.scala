package com.databricks.spark.sql.perf.mllib.classification

import com.databricks.spark.sql.perf.mllib.OptionImplicits._
import com.databricks.spark.sql.perf.mllib._
import com.databricks.spark.sql.perf.mllib.data.DataGenerator

import org.apache.spark.ml.{Transformer, ModelBuilder}
import org.apache.spark.ml
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.DataFrame

object LogisticRegression extends BenchmarkAlgorithm
  with TestFromTraining with TrainingSetFromTransformer with ScoringWithEvaluator {

  override protected def initialData(ctx: MLBenchContext) = {
    import ctx.extraParams._
    DataGenerator.generateFeatures(
      ctx.sqlContext,
      numExamples,
      ctx.seed(),
      numPartitions,
      numFeatures)
  }

  override protected def trueModel(ctx: MLBenchContext): Transformer = {
    val rng = ctx.newGenerator()
    val coefficients =
      Vectors.dense(Array.fill[Double](ctx.extraParams.numFeatures)(2 * rng.nextDouble() - 1))
    // Small intercept to prevent some skew in the data.
    val intercept = 0.01 * (2 * rng.nextDouble - 1)
    ModelBuilder.newLogisticRegressionModel(coefficients, intercept)
  }

  override def train(ctx: MLBenchContext,
            trainingSet: DataFrame): Transformer = {
    logger.info(s"$this: train: trainingSet=${trainingSet.schema}")
    import ctx.extraParams._
    val lr = new ml.classification.LogisticRegression()
      .setTol(tol)
      .setRegParam(regParam)
    lr.fit(trainingSet)
  }
}

