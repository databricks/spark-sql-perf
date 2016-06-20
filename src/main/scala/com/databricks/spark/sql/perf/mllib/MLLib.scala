package com.databricks.spark.sql.perf.mllib

import java.util.Random

import com.databricks.spark.sql.perf._
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.classification.{ClassificationModel, RandomForestClassificationModel}
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

class MLLib(@transient sqlContext: SQLContext)
  extends Benchmark(sqlContext) with Serializable {

  def this() = this(SQLContext.getOrCreate(SparkContext.getOrCreate()))
}

object MLLib {
  def runDefault(runConfig: RunConfig): MLLib = {
    val ml = new MLLib()
    val benchmarks = MLBenchmarks.benchmarkObjects
    ml.runExperiment(
      executionsToRun = benchmarks,
      resultLocation = "/test/results")
    ml
  }
}
