package com.databricks.spark.sql.perf.mllib.feature

import org.apache.spark.ml
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.PipelineStage
import org.apache.spark.sql._
import com.databricks.spark.sql.perf.mllib.OptionImplicits._
import com.databricks.spark.sql.perf.mllib.data.DataGenerator
import com.databricks.spark.sql.perf.mllib.{BenchmarkAlgorithm, MLBenchContext, TestFromTraining}

/** Object for testing VectorSlicer performance */
object VectorSlicer extends BenchmarkAlgorithm with TestFromTraining {

  override def trainingDataSet(ctx: MLBenchContext): DataFrame = {
    import ctx.params._

    DataGenerator.generateMixedFeatures(
      ctx.sqlContext,
      numExamples,
      ctx.seed(),
      numPartitions,
      Array.fill(numFeatures)(20)
    )
  }

  override def getPipelineStage(ctx: MLBenchContext): PipelineStage = {
    import ctx.params._

    val indices = (0 until numFeatures by 2).toArray

    new ml.feature.VectorSlicer()
      .setInputCol("features")
      .setIndices(indices)
  }
}
