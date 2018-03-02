package com.databricks.spark.sql.perf.mllib.feature

import org.apache.spark.ml
import org.apache.spark.ml.PipelineStage
import org.apache.spark.sql._

import com.databricks.spark.sql.perf.mllib.OptionImplicits._
import com.databricks.spark.sql.perf.mllib.data.DataGenerator
import com.databricks.spark.sql.perf.mllib.{BenchmarkAlgorithm, MLBenchContext, TestFromTraining}

/** Object for testing MinHashLSH performance */
object MinHashLSH extends BenchmarkAlgorithm with TestFromTraining {

  override def trainingDataSet(ctx: MLBenchContext): DataFrame = {
    import ctx.params._

    val df = DataGenerator.generateMixedFeatures(
      ctx.sqlContext,
      numExamples,
      ctx.seed(),
      numPartitions,
      Array.fill(numFeatures)(2)
    )
    df
  }

  override def getPipelineStage(ctx: MLBenchContext): PipelineStage = {
    import ctx.params._

    new ml.feature.MinHashLSH()
      .setInputCol("features")
      .setNumHashTables(numHashTables)
  }

}
