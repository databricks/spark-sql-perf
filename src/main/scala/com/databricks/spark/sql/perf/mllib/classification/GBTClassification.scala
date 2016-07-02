package com.databricks.spark.sql.perf.mllib.classification

import org.apache.spark.ml.{Estimator, ModelBuilder, Transformer}
import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.ml.evaluation.{Evaluator, MulticlassClassificationEvaluator}

import com.databricks.spark.sql.perf.mllib._
import com.databricks.spark.sql.perf.mllib.OptionImplicits._
import com.databricks.spark.sql.perf.mllib.data.DataGenerator


object GBTClassification extends BenchmarkAlgorithm
  with TestFromTraining with TrainingSetFromTransformer with ScoringWithEvaluator {

  override protected def initialData(ctx: MLBenchContext) = {
    import ctx.params._
    DataGenerator.generateMixedFeatures(ctx.sqlContext, numExamples, ctx.seed(), numPartitions,
      TreeOrForestClassification.getFeatureArity(ctx))
  }

  override protected def trueModel(ctx: MLBenchContext): Transformer = {
    // We add +1 to the depth to make it more likely that many iterations of boosting are needed
    // to model the true tree.
    ModelBuilder.newDecisionTreeClassificationModel(ctx.params.depth + 1, ctx.params.numClasses,
      TreeOrForestClassification.getFeatureArity(ctx), ctx.seed())
  }

  override def getEstimator(ctx: MLBenchContext): Estimator[_] = {
    import ctx.params._
    // TODO: subsamplingRate, featureSubsetStrategy
    // TODO: cacheNodeIds, checkpoint?
    new GBTClassifier()
      .setMaxDepth(depth)
      .setMaxIter(maxIter)
      .setSeed(ctx.seed())
  }

  override protected def evaluator(ctx: MLBenchContext): Evaluator =
    new MulticlassClassificationEvaluator()
}
