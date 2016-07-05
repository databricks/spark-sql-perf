package com.databricks.spark.sql.perf.mllib.recommendation

import org.apache.spark.ml
import org.apache.spark.ml.evaluation.{RegressionEvaluator, Evaluator}
import org.apache.spark.ml.{Transformer, Estimator}
import org.apache.spark.sql._

import com.databricks.spark.sql.perf.mllib.OptionImplicits._
import com.databricks.spark.sql.perf.mllib.data.DataGenerator
import com.databricks.spark.sql.perf.mllib.{ScoringWithEvaluator, BenchmarkAlgorithm, MLBenchContext}

object ALS extends BenchmarkAlgorithm with ScoringWithEvaluator {

  override def trainingDataSet(ctx: MLBenchContext): DataFrame = {
    import ctx.params._
    DataGenerator.generateRatings(
      ctx.sqlContext,
      numUsers,
      numItems,
      numExamples,
      numTestExamples,
      implicitPrefs = false,
      numPartitions,
      ctx.seed())._1
  }

  override def testDataSet(ctx: MLBenchContext): DataFrame = {
    import ctx.params._
    DataGenerator.generateRatings(
      ctx.sqlContext,
      numUsers,
      numItems,
      numExamples,
      numTestExamples,
      implicitPrefs = false,
      numPartitions,
      ctx.seed())._2
  }

  override def getEstimator(ctx: MLBenchContext): Estimator[_] = {
    import ctx.params._
    new ml.recommendation.ALS()
      .setSeed(ctx.seed())
      .setRegParam(regParam)
      .setNumBlocks(numPartitions)
      .setRank(rank)
      .setMaxIter(maxIter)
  }

  override protected def evaluator(ctx: MLBenchContext): Evaluator = {
    new RegressionEvaluator().setLabelCol("rating")
  }
}
