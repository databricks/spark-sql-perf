package com.databricks.spark.sql.perf.mllib.fpm

import org.apache.spark.ml
import org.apache.spark.ml.PipelineStage
import org.apache.spark.sql.DataFrame

import com.databricks.spark.sql.perf.mllib._
import com.databricks.spark.sql.perf.mllib.OptionImplicits._
import com.databricks.spark.sql.perf.mllib.data.DataGenerator


/** Object containing methods used in performance tests for FPGrowth */
object FPGrowth extends BenchmarkAlgorithm with TestFromTraining {

  def trainingDataSet(ctx: MLBenchContext): DataFrame = {
    import ctx.params._

    DataGenerator.generateItemSet(
      ctx.sqlContext,
      numExamples,
      ctx.seed(),
      numPartitions,
      numItems,
      itemSetSize)
  }

  override def getPipelineStage(ctx: MLBenchContext): PipelineStage = {
    new ml.fpm.FPGrowth()
      .setItemsCol("items")
  }
}
