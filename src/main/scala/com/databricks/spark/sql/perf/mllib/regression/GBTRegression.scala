package com.databricks.spark.sql.perf.mllib.regression

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.regression.GBTRegressor

import com.databricks.spark.sql.perf.mllib.OptionImplicits._
import com.databricks.spark.sql.perf.mllib.{BenchmarkAlgorithm, MLBenchContext,
  TreeOrForestRegressor}

object GBTRegression extends BenchmarkAlgorithm with TreeOrForestRegressor {
  override def getPipelineStage(ctx: MLBenchContext): PipelineStage = {
    import ctx.params._
    new GBTRegressor()
      .setMaxDepth(depth)
      .setMaxIter(maxIter)
      .setSeed(ctx.seed())
  }
}