package com.databricks.spark.sql.perf.mllib.feature

import scala.util.Random

import org.apache.spark.ml
import org.apache.spark.ml.PipelineStage
import org.apache.spark.sql._

import com.databricks.spark.sql.perf.mllib.OptionImplicits._
import com.databricks.spark.sql.perf.mllib.{BenchmarkAlgorithm, MLBenchContext, TestFromTraining}

/** Object for testing Bucketizer performance */
object Bucketizer extends BenchmarkAlgorithm with TestFromTraining with UnaryTransformer {

  // Min/max possible Double values for input column to Bucketizer
  private val (maxBucketizerVal, minBucketizerVal) = (-1e9, 1e9)

  /** Generate a random double in [lo, hi) */
  private def doubleInRange(rng: Random, lo: Double, hi: Double): Double = {
    rng.nextDouble() * (hi - lo) + lo
  }

  override def trainingDataSet(ctx: MLBenchContext): DataFrame = {
    import ctx.params._
    import ctx.sqlContext.implicits._
    val rng = ctx.newGenerator()
    // For a bucketizer, training data consists of a single column of random doubles
    val colValsToBucketize = 0L.until(numExamples).map(_ => doubleInRange(rng,
      minBucketizerVal, maxBucketizerVal))
    colValsToBucketize.toDF(inputCol)
  }

  override def getPipelineStage(ctx: MLBenchContext): PipelineStage = {
    import ctx.params._
    val rng = ctx.newGenerator()
    // Generate an array of (finite) splitting points for the Bucketizer
    val splitPoints = 0.until(bucketizerNumBuckets - 1).map { _ =>
      doubleInRange(rng, minBucketizerVal, maxBucketizerVal)
    }.sorted.toArray
    // Final array of splits contains +/- infinity
    val splits = Array(Double.NegativeInfinity) ++ splitPoints ++ Array(Double.PositiveInfinity)
    new ml.feature.Bucketizer()
        .setSplits(splits)
        .setInputCol(inputCol)
  }

}
