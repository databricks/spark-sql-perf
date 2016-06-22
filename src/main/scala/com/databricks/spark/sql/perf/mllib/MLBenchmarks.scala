package com.databricks.spark.sql.perf.mllib

import com.databricks.spark.sql.perf.mllib.classification.LogisticRegression
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import com.databricks.spark.sql.perf.{MLParams}
import OptionImplicits._

case class MLTest(
                   benchmark: BenchmarkAlgorithm,
                   params: MLParams)

// Example on how to create benchmarks using the API.
object MLBenchmarks {
  // The list of standard benchmarks that we are going to run for ML.
  val benchmarks: Seq[MLTest] = List(
      MLTest(
        LogisticRegression,
        MLParams(
          numFeatures = 10,
          numExamples = 10,
          numTestExamples = 10,
          numPartitions = 3,
          regParam = 1,
          tol = 0.2)
      )
  )

  val context = SparkContext.getOrCreate()
  val sqlContext: SQLContext = SQLContext.getOrCreate(context)

  def benchmarkObjects: Seq[MLTransformerBenchmarkable] = benchmarks.map { mlb =>
    new MLTransformerBenchmarkable(mlb.params, mlb.benchmark, sqlContext)
  }

}
