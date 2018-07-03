package com.databricks.spark.sql.perf.mllib.feature

import org.apache.spark.ml
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.PipelineStage
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import com.databricks.spark.sql.perf.mllib.OptionImplicits._
import com.databricks.spark.sql.perf.mllib.data.DataGenerator
import com.databricks.spark.sql.perf.mllib.{BenchmarkAlgorithm, MLBenchContext, TestFromTraining}

/** Object for testing VectorAssembler performance */
object VectorAssembler extends BenchmarkAlgorithm with TestFromTraining {

  private def getInputCols(numInputCols: Int): Array[String] = {
    Array.tabulate(numInputCols)(i => s"c${i}")
  }

  override def trainingDataSet(ctx: MLBenchContext): DataFrame = {
    import ctx.params._

    require(numInputCols.get <= numFeatures.get,
      s"numInputCols (${numInputCols}) cannot be greater than numFeatures (${numFeatures}).")

    val df = DataGenerator.generateContinuousFeatures(
      ctx.sqlContext,
      numExamples,
      ctx.seed(),
      numPartitions,
      numFeatures)

    val slice = udf { (v: Vector, numSlices: Int) =>
      val data = v.toArray
      val n = data.length.toLong
      (0 until numSlices).map { i =>
        val start = ((i * n) / numSlices).toInt
        val end = ((i + 1) * n / numSlices).toInt
        Vectors.dense(data.slice(start, end))
      }
    }

    val inputCols = getInputCols(numInputCols.get)
    df.select(slice(col("features"), lit(numInputCols.get)).as("slices"))
      .select((0 until numInputCols.get).map(i => col("slices")(i).as(inputCols(i))): _*)
  }

  override def getPipelineStage(ctx: MLBenchContext): PipelineStage = {
    import ctx.params._
    val inputCols = getInputCols(numInputCols.get)
    new ml.feature.VectorAssembler()
      .setInputCols(inputCols)
  }
}
