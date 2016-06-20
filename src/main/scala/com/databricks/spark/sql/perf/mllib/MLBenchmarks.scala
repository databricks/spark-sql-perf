package com.databricks.spark.sql.perf.mllib

import com.databricks.spark.sql.perf.mllib.classification.LogisticRegression
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import com.databricks.spark.sql.perf.{ExtraMLTestParameters, MLTestParameters}
import OptionImplicits._

case class MLBenchmark(
    benchmark: BenchmarkAlgorithm,
    common: MLTestParameters,
    extra: ExtraMLTestParameters)

// Example on how to create benchmarks using the API.
object MLBenchmarks {
  // The list of standard benchmarks that we are going to run for ML.
  val benchmarks: Seq[MLBenchmark] = List(
      MLBenchmark(
        LogisticRegression,
        MLTestParameters(
          numFeatures = 10,
          numExamples = 10,
          numTestExamples = 10,
          numPartitions = 3),
        ExtraMLTestParameters(
          regParam = 1,
          tol = 0.2)
      )
  )

  val context = SparkContext.getOrCreate()
  val sqlContext: SQLContext = SQLContext.getOrCreate(context)

  def benchmarkObjects: Seq[MLClassificationBenchmarkable] = benchmarks.map { mlb =>
    new MLClassificationBenchmarkable(mlb.extra, mlb.common, mlb.benchmark, sqlContext)
  }

}
