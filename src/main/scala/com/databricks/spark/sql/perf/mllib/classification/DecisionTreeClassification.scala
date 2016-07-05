package com.databricks.spark.sql.perf.mllib.classification

import org.apache.spark.ml.{Estimator, ModelBuilder, Transformer}
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.{Evaluator, MulticlassClassificationEvaluator}

import com.databricks.spark.sql.perf.mllib.OptionImplicits._
import com.databricks.spark.sql.perf.mllib._
import com.databricks.spark.sql.perf.mllib.data.DataGenerator


abstract class TreeOrForestClassification extends BenchmarkAlgorithm
  with TestFromTraining with TrainingSetFromTransformer with ScoringWithEvaluator {

  override protected def initialData(ctx: MLBenchContext) = {
    import ctx.params._
    DataGenerator.generateMixedFeatures(ctx.sqlContext, numExamples, ctx.seed(), numPartitions,
      TreeOrForestClassification.getFeatureArity(ctx))
  }

  override protected def trueModel(ctx: MLBenchContext): Transformer = {
    ModelBuilder.newDecisionTreeClassificationModel(ctx.params.depth, ctx.params.numClasses,
      TreeOrForestClassification.getFeatureArity(ctx), ctx.seed())
  }

  override protected def evaluator(ctx: MLBenchContext): Evaluator =
    new MulticlassClassificationEvaluator()
}

object DecisionTreeClassification extends TreeOrForestClassification {

  override def getEstimator(ctx: MLBenchContext): Estimator[_] = {
    import ctx.params._
    new DecisionTreeClassifier()
      .setMaxDepth(depth)
      .setSeed(ctx.seed())
  }
}

object TreeOrForestClassification {

  /**
   * Get feature arity for tree and tree ensemble tests.
   * Currently, this is hard-coded as:
   *  - 1/2 binary features
   *  - 1/2 high-arity (20-category) features
   *  - 1/2 continuous features
   * @return  Array of length numFeatures, where 0 indicates continuous feature and
   *          value > 0 indicates a categorical feature of that arity.
   */
  def getFeatureArity(ctx: MLBenchContext): Array[Int] = {
    val numFeatures = ctx.params.numFeatures
    val fourthFeatures = numFeatures / 4
    Array.fill[Int](fourthFeatures)(2) ++ // low-arity categorical
      Array.fill[Int](fourthFeatures)(20) ++ // high-arity categorical
      Array.fill[Int](numFeatures - 2 * fourthFeatures)(0) // continuous
  }
}
