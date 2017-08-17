package com.databricks.spark.sql.perf.mllib.regression

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.regression.RandomForestRegressor

import com.databricks.spark.sql.perf.mllib.OptionImplicits._
import com.databricks.spark.sql.perf.mllib.{BenchmarkAlgorithm, MLBenchContext,
  TreeOrForestRegressor}

object RandomForestRegression extends BenchmarkAlgorithm with TreeOrForestRegressor {
  override def getPipelineStage(ctx: MLBenchContext): PipelineStage = {
    import ctx.params._
    new RandomForestRegressor()
      .setMaxDepth(depth)
      .setNumTrees(maxIter)
      .setSeed(ctx.seed())
  }
}
