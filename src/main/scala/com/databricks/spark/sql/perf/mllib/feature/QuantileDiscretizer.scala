package com.databricks.spark.sql.perf.mllib.feature

import org.apache.spark.ml
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.PipelineStage
import org.apache.spark.sql._

import com.databricks.spark.sql.perf.mllib.OptionImplicits._
import com.databricks.spark.sql.perf.mllib.data.DataGenerator
import com.databricks.spark.sql.perf.mllib.{BenchmarkAlgorithm, MLBenchContext, TestFromTraining}

/** Object for testing QuantileDiscretizer performance */
object QuantileDiscretizer extends BenchmarkAlgorithm with TestFromTraining with UnaryTransformer {

  override def trainingDataSet(ctx: MLBenchContext): DataFrame = {
    import ctx.params._
    import ctx.sqlContext.implicits._

    DataGenerator.generateContinuousFeatures(
      ctx.sqlContext,
      numExamples,
      ctx.seed(),
      numPartitions,
      1
    ).rdd.map { case Row(vec: Vector) =>
      vec(0) // extract the single generated double value for each row
    }.toDF(inputCol)
  }

  override def getPipelineStage(ctx: MLBenchContext): PipelineStage = {
    import ctx.params._
    new ml.feature.QuantileDiscretizer()
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
      .setNumBuckets(bucketizerNumBuckets)
      .setRelativeError(relativeError)
  }
}
