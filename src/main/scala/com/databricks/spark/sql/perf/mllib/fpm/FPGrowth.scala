package com.databricks.spark.sql.perf.mllib.fpm

import org.apache.spark.ml
import org.apache.spark.ml.PipelineStage
import com.databricks.spark.sql.perf.mllib.OptionImplicits._
import com.databricks.spark.sql.perf.mllib._
import com.databricks.spark.sql.perf.mllib.data.DataGenerator
import org.apache.spark.sql.DataFrame

/** Object containing methods used in performance tests for FPGrowth */
object FPGrowth extends BenchmarkAlgorithm with TestFromTraining {

  def trainingDataSet(ctx: MLBenchContext): DataFrame = {
    import ctx.params._
    val rng = ctx.newGenerator()

    // These two param will move to MLParam later.
    val numItems = 1000
    val averageSizeOfItemSet = 4

    DataGenerator.generateFrequentItemSet(
      ctx.sqlContext,
      numExamples,
      ctx.seed(),
      numPartitions,
      numItems,
      averageSizeOfItemSet)
  }

  override def getPipelineStage(ctx: MLBenchContext): PipelineStage = {
    import ctx.params._

    new ml.fpm.FPGrowth()
      .setItemsCol("items")
  }
}
