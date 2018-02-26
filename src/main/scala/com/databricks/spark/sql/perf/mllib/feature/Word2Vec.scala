package com.databricks.spark.sql.perf.mllib.feature

import org.apache.spark.ml
import org.apache.spark.ml.PipelineStage
import org.apache.spark.sql.DataFrame

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
    df
  }

  override def getPipelineStage(ctx: MLBenchContext): PipelineStage = {
    new ml.feature.Word2Vec().setInputCol("text")
  }

}
