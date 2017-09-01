package com.databricks.spark.sql.perf.mllib.clustering

import org.apache.spark.ml
import org.apache.spark.ml.PipelineStage
import org.apache.spark.sql.DataFrame

import com.databricks.spark.sql.perf.mllib.OptionImplicits._
import com.databricks.spark.sql.perf.mllib.{BenchmarkAlgorithm, MLBenchContext, TestFromTraining}
import com.databricks.spark.sql.perf.mllib.data.DataGenerator

object GaussianMixture extends BenchmarkAlgorithm with TestFromTraining {

  override def trainingDataSet(ctx: MLBenchContext): DataFrame = {
    import ctx.params._
    DataGenerator.generateGaussianMixtureData(ctx.sqlContext, numCenters = k,
      numExamples = numExamples, seed = ctx.seed(), numPartitions = numPartitions,
      numFeatures = numFeatures)
  }

  override def getPipelineStage(ctx: MLBenchContext): PipelineStage = {
    import ctx.params._
    new ml.clustering.GaussianMixture()
      .setK(k)
      .setSeed(randomSeed.toLong)
      .setMaxIter(maxIter)
      .setTol(tol)
  }

  // TODO(?) add a scoring method here.
}
