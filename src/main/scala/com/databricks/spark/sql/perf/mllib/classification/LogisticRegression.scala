package com.databricks.spark.sql.perf.mllib.classification

import com.databricks.spark.sql.perf.mllib.OptionImplicits._
import com.databricks.spark.sql.perf.mllib._
import com.databricks.spark.sql.perf.mllib.data.DataGenerator

import org.apache.spark.ml.ModelBuilder
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame}

object LogisticRegression extends ClassificationPipelineDescription
  with TestFromTraining with TrainingSetFromTransformer with ScoringWithEvaluator {

  override type Model = LogisticRegressionModel

  def evaluator(ctx: ClassificationContext) = new MulticlassClassificationEvaluator()

  def initialData(ctx: ClassificationContext) = {
    import ctx.commonParams._
    DataGenerator.generateFeatures(
      ctx.sqlContext,
      numExamples,
      ctx.seed(),
      numPartitions,
      numFeatures)
  }

  def initialModel(ctx: ClassificationContext): Model = {
    val rng = ctx.newGenerator()
    val coefficients =
      Vectors.dense(Array.fill[Double](ctx.commonParams.numFeatures)(2 * rng.nextDouble() - 1))
    val intercept = 2 * rng.nextDouble - 1
    ModelBuilder.newLogisticRegressionModel(coefficients, intercept)
  }

  def train(ctx: ClassificationContext,
            trainingSet: DataFrame): Model = {
    logger.info(s"$this: train: trainingSet=${trainingSet.schema}")
    import ctx.extraParams._
    val lr = new LogisticRegression()
      .setTol(tol)
      .setRegParam(regParam)
    lr.fit(trainingSet)
  }
}

