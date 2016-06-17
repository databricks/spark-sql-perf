package com.databricks.spark.sql.perf.mllib.classification

import com.databricks.spark.sql.perf.ExtraMLTestParameters
import com.databricks.spark.sql.perf.mllib.data.DataGenerator
import com.databricks.spark.sql.perf.mllib.{ClassificationContext, ClassificationPipelineDescription}
import org.apache.spark.ml.{ModelBuilder, Transformer}
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator, Evaluator}
import org.apache.spark.ml.classification.ClassificationModel
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.ml.classification.LogisticRegression
import com.databricks.spark.sql.perf.mllib.OptionImplicits._


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
    model.transform(initial)
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

object LogisticRegressionBenchmark
  extends ClassificationPipelineDescription {

  override type Model = LogisticRegressionModel

  def trainingDataSet(ctx: ClassificationContext): DataFrame = {
    DataGenerator.generateBinaryLabeledPoints(
      ctx.sqlContext,
      ctx.commonParams.numExamples.get,
      ctx.commonParams)
  }

  def testDataSet(ctx: ClassificationContext): DataFrame = {
    DataGenerator.generateBinaryLabeledPoints(
      ctx.sqlContext,
      ctx.commonParams.numTestExamples.get,
      ctx.commonParams)
  }

  def train(ctx: ClassificationContext,
            trainingSet: DataFrame): Model = {
    import ctx.extraParams._
    val lr = new LogisticRegression()
        .setTol(tol)
        .setRegParam(regParam)
    lr.fit(trainingSet)
  }

  def score(ctx: ClassificationContext,
            testSet: DataFrame,
            model: Model): Double = {
    val eval = model.evaluate(testSet)
    Utils.accuracy(eval.predictions)
  }
}

object LogisticRegressionBenchmark2 extends ClassificationPipelineDescription
  with TestFromTraining with TrainingSetFromTransformer with ScoringWithEvaluator {

  override type Model = LogisticRegressionModel

  def evaluator(ctx: ClassificationContext) = new MulticlassClassificationEvaluator()

  def initialData(ctx: ClassificationContext) = {
    import ctx.commonParams._
    DataGenerator.generateFeatures(
      ctx.sqlContext, numExamples, ctx.seed(), numPartitions, numFeatures)
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
    import ctx.extraParams._
    val lr = new LogisticRegression()
      .setTol(tol)
      .setRegParam(regParam)
    lr.fit(trainingSet)
  }
}


object Utils {
  def accuracy(set: DataFrame): Double = {
    val session = set.sqlContext.sparkSession
    import session.implicits._
    val counts = set.map { case Row(pred: Double, label: Double) =>
      if (pred == label) 1.0 else 0.0
    } .rdd.sum()
    100.0 * counts / set.count()
  }
}
