package com.databricks.spark.sql.perf.mllib

import com.typesafe.scalalogging.slf4j.Logging

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

/**
 * The description of a benchmark for doing classification using the pipeline API.
 */
trait ClassificationPipelineDescription extends Logging {

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

trait ScoringWithEvaluator {
  self: ClassificationPipelineDescription =>

  protected def evaluator(ctx: ClassificationContext): Evaluator

  final override def score(
      ctx: ClassificationContext,
      testSet: DataFrame,
      model: Model): Double = {
    val eval = model.transform(testSet)
    evaluator(ctx).evaluate(eval)
  }
}

trait TrainingSetFromTransformer {
  self: ClassificationPipelineDescription =>

  protected def initialData(ctx: ClassificationContext): DataFrame

  protected def initialModel(ctx: ClassificationContext): Model

  final override def trainingDataSet(ctx: ClassificationContext): DataFrame = {
    val initial = initialData(ctx)
    val model = initialModel(ctx)
    model.transform(initial).select(col("features"), col("prediction").as("label"))
  }
}

trait TestFromTraining {
  self: ClassificationPipelineDescription =>

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

