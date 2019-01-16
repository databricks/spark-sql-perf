package com.databricks.spark.sql.perf.mllib

import com.databricks.spark.sql.perf.mllib.classification.LogisticRegression
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext,SparkSession}

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
        new MLParams(
          numFeatures = 10,
          numExamples = 10,
          numTestExamples = 10,
          numPartitions = 3,
          regParam = 1,
          tol = 0.2)
      )
  )

  val sparkSession = SparkSession.builder.getOrCreate()
  val sqlContext: SQLContext = sparkSession.sqlContext
  val context = sqlContext.sparkContext

  def benchmarkObjects: Seq[MLPipelineStageBenchmarkable] = benchmarks.map { mlb =>
    new MLPipelineStageBenchmarkable(mlb.params, mlb.benchmark, sqlContext)
  }

}
