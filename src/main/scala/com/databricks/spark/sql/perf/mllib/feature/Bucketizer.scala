package com.databricks.spark.sql.perf.mllib.feature

import scala.util.Random

import org.apache.spark.ml
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.PipelineStage
import org.apache.spark.sql._

import com.databricks.spark.sql.perf.mllib.OptionImplicits._
import com.databricks.spark.sql.perf.mllib.data.DataGenerator
import com.databricks.spark.sql.perf.mllib.{BenchmarkAlgorithm, MLBenchContext, TestFromTraining}

/** Object for testing Bucketizer performance */
object Bucketizer extends BenchmarkAlgorithm with TestFromTraining with UnaryTransformer {

  override def trainingDataSet(ctx: MLBenchContext): DataFrame = {
    import ctx.params._
    import ctx.sqlContext.implicits._
    val rng = ctx.newGenerator()
    // For a bucketizer, training data consists of a single column of random doubles
    DataGenerator.generateContinuousFeatures(ctx.sqlContext,
      numExamples, ctx.seed(), numPartitions, numFeatures = 1).rdd.map { case Row(vec: Vector) =>
        vec(0) // extract the single generated double value for each row
    }.toDF(inputCol)
  }

  override def getPipelineStage(ctx: MLBenchContext): PipelineStage = {
    import ctx.params._
    val rng = ctx.newGenerator()
    // Generate an array of (finite) splitting points in [-1, 1) for the Bucketizer
    val splitPoints = 0.until(bucketizerNumBuckets - 1).map { _ =>
      2 * rng.nextDouble() - 1
    }.sorted.toArray
    // Final array of splits contains +/- infinity
    val splits = Array(Double.NegativeInfinity) ++ splitPoints ++ Array(Double.PositiveInfinity)
    new ml.feature.Bucketizer()
      .setSplits(splits)
      .setInputCol(inputCol)
  }

}
