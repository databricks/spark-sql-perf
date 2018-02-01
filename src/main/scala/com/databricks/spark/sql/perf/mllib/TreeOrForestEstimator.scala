package com.databricks.spark.sql.perf.mllib

import org.apache.spark.ml.{ModelBuilderSSP, Transformer, TreeUtils}
import org.apache.spark.ml.evaluation.{Evaluator, MulticlassClassificationEvaluator,
  RegressionEvaluator}
import org.apache.spark.sql.DataFrame

import com.databricks.spark.sql.perf.mllib.OptionImplicits._
import com.databricks.spark.sql.perf.mllib.data.DataGenerator

/** Base trait for BenchmarkAlgorithm objects testing a tree or forest estimator */
private[mllib] trait TreeOrForestEstimator
  extends TestFromTraining with TrainingSetFromTransformer with ScoringWithEvaluator {
  self: BenchmarkAlgorithm =>

  override protected def initialData(ctx: MLBenchContext) = {
    import ctx.params._
    val featureArity: Array[Int] = TreeOrForestEstimator.getFeatureArity(ctx)
    val data: DataFrame = DataGenerator.generateMixedFeatures(ctx.sqlContext, numExamples,
      ctx.seed(), numPartitions, featureArity)
    TreeUtils.setMetadata(data, "features", featureArity)
  }
}

/** Base trait for BenchmarkAlgorithm objects testing a tree or forest classifier */
private[mllib] trait TreeOrForestClassifier extends TreeOrForestEstimator {
  self: BenchmarkAlgorithm =>

  override protected def evaluator(ctx: MLBenchContext): Evaluator = {
    new MulticlassClassificationEvaluator()
  }

  override protected def trueModel(ctx: MLBenchContext): Transformer = {
    ModelBuilderSSP.newDecisionTreeClassificationModel(ctx.params.depth, ctx.params.numClasses,
      TreeOrForestEstimator.getFeatureArity(ctx), ctx.seed())
  }
}

/** Base trait for BenchmarkAlgorithm objects testing a tree or forest regressor */
private[mllib] trait TreeOrForestRegressor extends TreeOrForestEstimator {
  self: BenchmarkAlgorithm =>

  override protected def evaluator(ctx: MLBenchContext): Evaluator = {
    new RegressionEvaluator()
  }

  override protected def trueModel(ctx: MLBenchContext): Transformer = {
    ModelBuilderSSP.newDecisionTreeRegressionModel(ctx.params.depth,
      TreeOrForestEstimator.getFeatureArity(ctx), ctx.seed())
  }

}

private[mllib] object TreeOrForestEstimator {

  /**
   * Get feature arity for tree and tree ensemble tests.
   * Currently, this is hard-coded as:
   * - 1/4 binary features
   * - 1/4 high-arity (20-category) features
   * - 1/2 continuous features
   *
   * @return Array of length numFeatures, where 0 indicates continuous feature and
   *         value > 0 indicates a categorical feature of that arity.
   */
  def getFeatureArity(ctx: MLBenchContext): Array[Int] = {
    val numFeatures = ctx.params.numFeatures
    val fourthFeatures = numFeatures / 4
    Array.fill[Int](fourthFeatures)(2) ++ // low-arity categorical
      Array.fill[Int](fourthFeatures)(20) ++ // high-arity categorical
      Array.fill[Int](numFeatures - 2 * fourthFeatures)(0) // continuous
  }
}

