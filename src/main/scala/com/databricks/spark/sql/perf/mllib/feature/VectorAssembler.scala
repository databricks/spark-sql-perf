package com.databricks.spark.sql.perf.mllib.feature

import org.apache.spark.ml
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.PipelineStage
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import com.databricks.spark.sql.perf.mllib.OptionImplicits._
import com.databricks.spark.sql.perf.mllib.data.DataGenerator
import com.databricks.spark.sql.perf.mllib.{BenchmarkAlgorithm, MLBenchContext, TestFromTraining}

/** Object for testing VectorAssembler performance */
object VectorAssembler extends BenchmarkAlgorithm with TestFromTraining {

  override def trainingDataSet(ctx: MLBenchContext): DataFrame = {
    import ctx.params._
    import ctx.sqlContext.implicits._

    var df = DataGenerator.generateContinuousFeatures(
      ctx.sqlContext,
      numExamples,
      ctx.seed(),
      numPartitions,
      numFeatures * numInputCols
    )
    val sliceVec = udf { (v: Vector, from: Int, until: Int) =>
      Vectors.dense(v.toArray.slice(from, until))
    }
    for (i <- (1 to numInputCols.get)) {
      val colName = s"inputCol${i.toString}"
      val fromIndex = (i - 1) * numFeatures
      val untilIndex = i * numFeatures
      df = df.withColumn(colName, sliceVec(col("features"), lit(fromIndex), lit(untilIndex)))
    }
    df.drop(col("features"))
  }

  override def getPipelineStage(ctx: MLBenchContext): PipelineStage = {
    import ctx.params._

    val inputCols = (1 to numInputCols.get)
      .map(i => s"inputCol${i.toString}").toArray

    new ml.feature.VectorAssembler()
      .setInputCols(inputCols)
  }
}
