package com.databricks.spark.sql.perf.mllib.classification

import org.apache.spark.ml.Estimator
import org.apache.spark.ml.classification.RandomForestClassifier

import com.databricks.spark.sql.perf.mllib._
import com.databricks.spark.sql.perf.mllib.OptionImplicits._


object RandomForestClassification extends TreeOrForestClassification {

  override def getEstimator(ctx: MLBenchContext): Estimator[_] = {
    import ctx.params._
    // TODO: subsamplingRate, featureSubsetStrategy
    // TODO: cacheNodeIds, checkpoint?
    new RandomForestClassifier()
      .setMaxDepth(depth)
      .setNumTrees(maxIter)
      .setSeed(ctx.seed())
  }
}
