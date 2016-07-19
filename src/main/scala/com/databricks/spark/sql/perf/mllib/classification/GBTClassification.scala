package com.databricks.spark.sql.perf.mllib.classification

import org.apache.spark.ml.{Estimator, ModelBuilder, Transformer, TreeUtils}
import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.ml.evaluation.{Evaluator, MulticlassClassificationEvaluator}
import org.apache.spark.sql._

import com.databricks.spark.sql.perf.mllib._
import com.databricks.spark.sql.perf.mllib.OptionImplicits._
import com.databricks.spark.sql.perf.mllib.data.DataGenerator


object GBTClassification extends BenchmarkAlgorithm
  with TestFromTraining with TrainingSetFromTransformer with ScoringWithEvaluator {

  import TreeOrForestClassification.getFeatureArity

  override protected def initialData(ctx: MLBenchContext) = {
    import ctx.params._
    val featureArity: Array[Int] = getFeatureArity(ctx)
    val data: DataFrame = DataGenerator.generateMixedFeatures(ctx.sqlContext, numExamples,
      ctx.seed(), numPartitions, featureArity)
    TreeUtils.setMetadata(data, "features", featureArity)
  }

  override protected def trueModel(ctx: MLBenchContext): Transformer = {
    import ctx.params._
    // We add +1 to the depth to make it more likely that many iterations of boosting are needed
    // to model the true tree.
    ModelBuilder.newDecisionTreeClassificationModel(depth + 1, numClasses, getFeatureArity(ctx),
      ctx.seed())
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
