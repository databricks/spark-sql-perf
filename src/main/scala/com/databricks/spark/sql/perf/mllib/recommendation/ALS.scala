package com.databricks.spark.sql.perf.mllib.recommendation

import org.apache.spark.ml
import org.apache.spark.ml.evaluation.{Evaluator, RegressionEvaluator}
import org.apache.spark.ml.PipelineStage
import org.apache.spark.sql._

import com.databricks.spark.sql.perf.mllib.OptionImplicits._
import com.databricks.spark.sql.perf.mllib.data.DataGenerator
import com.databricks.spark.sql.perf.mllib.{BenchmarkAlgorithm, MLBenchContext, ScoringWithEvaluator}

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

  override def getPipelineStage(ctx: MLBenchContext): PipelineStage = {
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
