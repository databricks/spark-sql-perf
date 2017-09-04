package com.databricks.spark.sql.perf.mllib.feature

import scala.util.Random

import org.apache.spark.ml
import org.apache.spark.ml.PipelineStage
import org.apache.spark.sql._
import org.apache.spark.sql.functions.split

import com.databricks.spark.sql.perf.mllib.OptionImplicits._
import com.databricks.spark.sql.perf.mllib.data.DataGenerator
import com.databricks.spark.sql.perf.mllib.{BenchmarkAlgorithm, MLBenchContext, TestFromTraining}

object HashingTF extends BenchmarkAlgorithm with TestFromTraining with UnaryTransformer {

  // Sample a random sentence of length up to maxLen from the provided array of words
  private def randomSentence(rng: Random, maxLen: Int, dictionary: Array[String]): Array[String] = {
    val length = rng.nextInt(maxLen - 1) + 1
    val dictLength = dictionary.length
    Array.tabulate[String](length)(_ => dictionary(rng.nextInt(dictLength)))
  }

  override def trainingDataSet(ctx: MLBenchContext): DataFrame = {
    import ctx.params._
    // To test HashingTF, we generate arrays of (on average) docLength strings, where
    // each string is selected from a pool of vocabSize strings
    // The expected # of occurrences of each word in our vocabulary is
    // (docLength * numExamples) / vocabSize
    val df = DataGenerator.generateDoc(ctx.sqlContext, numExamples = numExamples, seed = ctx.seed(),
      numPartitions = numPartitions, vocabSize = vocabSize, avgDocLength = docLength,
      dataColName = inputCol)
    df.withColumn(inputCol, split(df(inputCol), " "))
  }

  override def getPipelineStage(ctx: MLBenchContext): PipelineStage = {
    import ctx.params._
    new ml.feature.HashingTF()
      .setInputCol(inputCol)
      .setNumFeatures(numFeatures)
  }

}
