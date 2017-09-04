package com.databricks.spark.sql.perf.mllib.regression

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.regression.DecisionTreeRegressor

import com.databricks.spark.sql.perf.mllib.OptionImplicits._
import com.databricks.spark.sql.perf.mllib._


object DecisionTreeRegression extends BenchmarkAlgorithm with TreeOrForestRegressor {

  override def getPipelineStage(ctx: MLBenchContext): PipelineStage = {
    import ctx.params._
    new DecisionTreeRegressor()
      .setMaxDepth(depth)
      .setSeed(ctx.seed())
  }
}
