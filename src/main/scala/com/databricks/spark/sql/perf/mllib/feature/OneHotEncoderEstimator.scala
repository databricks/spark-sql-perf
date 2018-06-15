package com.databricks.spark.sql.perf.mllib.feature

import org.apache.spark.ml
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.PipelineStage
import org.apache.spark.sql._

import com.databricks.spark.sql.perf.mllib.OptionImplicits._
import com.databricks.spark.sql.perf.mllib.data.DataGenerator
import com.databricks.spark.sql.perf.mllib.{BenchmarkAlgorithm, MLBenchContext, TestFromTraining}

/** Object for testing OneHotEncoderEstimator performance */
object OneHotEncoderEstimator extends BenchmarkAlgorithm with TestFromTraining with UnaryTransformer {

  override def trainingDataSet(ctx: MLBenchContext): DataFrame = {
    import ctx.params._
    import ctx.sqlContext.implicits._

    DataGenerator.generateMixedFeatures(
      ctx.sqlContext,
      numExamples,
      ctx.seed(),
      numPartitions,
      Array.fill(1)(featureArity.get)
    ).rdd.map { case Row(vec: Vector) =>
      vec(0) // extract the single generated double value for each row
    }.toDF(inputCol)
  }

  override def getPipelineStage(ctx: MLBenchContext): PipelineStage = {
    new ml.feature.OneHotEncoderEstimator()
      .setInputCols(Array(inputCol))
  }
}
