package com.databricks.spark.sql.perf.mllib.feature

import scala.util.Random

import org.apache.spark.ml
import org.apache.spark.ml.{PipelineStage, Transformer}
import org.apache.spark.ml.feature.Word2VecModel
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, split}

import com.databricks.spark.sql.perf.mllib.{BenchmarkAlgorithm, MLBenchContext, TestFromTraining}
import com.databricks.spark.sql.perf.mllib.OptionImplicits._
import com.databricks.spark.sql.perf.mllib.data.DataGenerator

/** Object for testing Word2Vec performance */
object Word2Vec extends BenchmarkAlgorithm with TestFromTraining {

  override def trainingDataSet(ctx: MLBenchContext): DataFrame = {
    import ctx.params._

    val df = DataGenerator.generateDoc(
      ctx.sqlContext,
      numExamples,
      ctx.seed(),
      numPartitions,
      vocabSize,
      docLength,
      "text"
    )
    df.select(split(col("text"), " ").as("text"))
  }

  override def getPipelineStage(ctx: MLBenchContext): PipelineStage = {
    new ml.feature.Word2Vec().setInputCol("text")
  }

  override def testAdditionalMethods(
      ctx: MLBenchContext,
      model: Transformer): Map[String, () => _] = {
    import ctx.params._

    val rng = new Random(ctx.seed())
    val word2vecModel = model.asInstanceOf[Word2VecModel]
    val testWord = Vectors.dense(Array.fill(word2vecModel.getVectorSize)(rng.nextGaussian()))

    Map("findSynonyms" -> (() => {
      word2vecModel.findSynonyms(testWord, numSynonymsToFind)
    }))
  }

}
