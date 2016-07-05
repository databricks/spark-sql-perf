package com.databricks.spark.sql.perf.mllib.recommendation

import org.apache.spark.ml
import org.apache.spark.ml.{Transformer, Estimator}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import com.databricks.spark.sql.perf.mllib.OptionImplicits._
import com.databricks.spark.sql.perf.mllib.data.DataGenerator
import com.databricks.spark.sql.perf.mllib.{BenchmarkAlgorithm, MLBenchContext}

object ALS extends BenchmarkAlgorithm {

  override def trainingDataSet(ctx: MLBenchContext): DataFrame = {
    import ctx.params._
    DataGenerator.generateRatings(
      ctx.sqlContext,
      numUsers,
      numItems,
      numExamples,
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

  override def score(
      ctx: MLBenchContext,
      testSet: DataFrame,
      model: Transformer): Double = {
    val out = model.transform(testSet)
    val prediction = out("predictionCol")
    val rating = out("rating")
    val squares = (prediction - rating) * (prediction - rating)
    val Array(Row(rmse: Double)) = out.select(avg(squares)).collect()
    math.sqrt(rmse)
  }
}
