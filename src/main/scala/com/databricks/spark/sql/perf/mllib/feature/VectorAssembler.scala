package com.databricks.spark.sql.perf.mllib.feature

import org.apache.spark.ml
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.PipelineStage
import org.apache.spark.sql._
import com.databricks.spark.sql.perf.mllib.OptionImplicits._
import com.databricks.spark.sql.perf.mllib.data.DataGenerator
import com.databricks.spark.sql.perf.mllib.{BenchmarkAlgorithm, MLBenchContext, TestFromTraining}
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.sql.types.{StructField, StructType}

/** Object for testing VectorAssembler performance */
object VectorAssembler extends BenchmarkAlgorithm with TestFromTraining {

  override def trainingDataSet(ctx: MLBenchContext): DataFrame = {
    import ctx.params._
    import ctx.sqlContext.implicits._

    val localNumInputCols = numInputCols.get
    val localNumFeatures = numFeatures.get
    val rdd = DataGenerator.generateContinuousFeatures(
      ctx.sqlContext,
      numExamples,
      ctx.seed(),
      numPartitions,
      numFeatures * numInputCols
    ).rdd.map { case Row(v: Vector) =>
      val data = v.toArray
      Row.fromSeq(
        (0 until localNumInputCols).map(i => {
          Vectors.dense(data.slice(localNumFeatures * i, localNumFeatures * (i + 1)))
        })
      )
    }
    val vecUDT = new VectorUDT
    val schema = StructType(
      (1 to numInputCols).map { i =>
        val colName = s"inputCol${i.toString}"
        StructField(colName, vecUDT, false)
      }
    )
    val df = ctx.sqlContext.createDataFrame(rdd, schema)
    df
  }

  override def getPipelineStage(ctx: MLBenchContext): PipelineStage = {
    import ctx.params._

    val inputCols = (1 to numInputCols.get)
      .map(i => s"inputCol${i.toString}").toArray

    new ml.feature.VectorAssembler()
      .setInputCols(inputCols)
  }
}
