package com.databricks.spark.sql.perf.mllib.clustering

import org.apache.spark.ml
import org.apache.spark.ml.Estimator
import org.apache.spark.sql._

import com.databricks.spark.sql.perf.mllib.OptionImplicits._
import com.databricks.spark.sql.perf.mllib.data.DataGenerator
import com.databricks.spark.sql.perf.mllib.{BenchmarkAlgorithm, MLBenchContext, TestFromTraining}


object KMeans extends BenchmarkAlgorithm with TestFromTraining {

  override def trainingDataSet(ctx: MLBenchContext): DataFrame = {
    import ctx.params._
    DataGenerator.generateGaussianMixtureData(ctx.sqlContext, k, numExamples, ctx.seed(),
      numPartitions, numFeatures)
  }

  override def getEstimator(ctx: MLBenchContext): Estimator[_] = {
    import ctx.params._
    new ml.clustering.KMeans()
      .setK(k)
      .setSeed(randomSeed.toLong)
      .setMaxIter(maxIter)
  }

  // TODO(?) add a scoring method here.
}
