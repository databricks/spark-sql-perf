package com.databricks.spark.sql.perf.mllib.feature

import org.apache.spark.ml
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.PipelineStage
import org.apache.spark.sql._

import com.databricks.spark.sql.perf.mllib.OptionImplicits._
import com.databricks.spark.sql.perf.mllib.data.DataGenerator
import com.databricks.spark.sql.perf.mllib.{BenchmarkAlgorithm, MLBenchContext, TestFromTraining}

/** Object for testing Tokenizer performance */
object Tokenizer extends BenchmarkAlgorithm with TestFromTraining with UnaryTransformer {

  override def trainingDataSet(ctx: MLBenchContext): DataFrame = {
    import ctx.params._
    import ctx.sqlContext.implicits._

    DataGenerator.generateDoc(
      ctx.sqlContext,
      numExamples,
      ctx.seed(),
      numPartitions,
      vocabSize,
      docLength,
      inputCol)
  }

  override def getPipelineStage(ctx: MLBenchContext): PipelineStage = {
    new ml.feature.Tokenizer()
      .setInputCol(inputCol)
  }
}
