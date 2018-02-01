package com.databricks.spark.sql.perf.mllib.classification

import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.ml.{ModelBuilderSSP, PipelineStage, Transformer}

import com.databricks.spark.sql.perf.mllib.OptionImplicits._
import com.databricks.spark.sql.perf.mllib._

object GBTClassification extends BenchmarkAlgorithm with TreeOrForestClassifier {

  import TreeOrForestEstimator.getFeatureArity

  override protected def trueModel(ctx: MLBenchContext): Transformer = {
    import ctx.params._
    // We add +1 to the depth to make it more likely that many iterations of boosting are needed
    // to model the true tree.
    ModelBuilderSSP.newDecisionTreeClassificationModel(depth + 1, numClasses, getFeatureArity(ctx),
      ctx.seed())
  }

  override def getPipelineStage(ctx: MLBenchContext): PipelineStage = {
    import ctx.params._
    // TODO: subsamplingRate, featureSubsetStrategy
    // TODO: cacheNodeIds, checkpoint?
    new GBTClassifier()
      .setMaxDepth(depth)
      .setMaxIter(maxIter)
      .setSeed(ctx.seed())
  }

}
