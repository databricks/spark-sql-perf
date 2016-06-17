package com.databricks.spark.sql.perf.mllib

import com.databricks.spark.sql.perf.mllib.classification.{LogisticRegressionBenchmark2, LogisticRegressionBenchmark}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import com.databricks.spark.sql.perf.{ExtraMLTestParameters, MLTestParameters}
import OptionImplicits._

case class MLBenchmark(
    benchmark: ClassificationPipelineDescription,
    common: MLTestParameters,
    extra: ExtraMLTestParameters)

object MLBenchmarkRegister {
  var tests: Map[String, ClassificationPipelineDescription] = Map.empty

  def register[Model](c: ClassificationPipelineDescription): Unit = {
    val n = c.getClass.getCanonicalName
    tests += n -> c.asInstanceOf[ClassificationPipelineDescription]
  }
}

object MLBenchmarks {
  // The list of standard benchmarks that we are going to run for ML.
  val benchmarks: Seq[MLBenchmark] = List(
//    MLBenchmark(
//      LogisticRegressionBenchmark,
//      MLTestParameters(
//        numFeatures = 100,
//        numExamples = 10,
//        numTestExamples = 100),
//      ExtraMLTestParameters(
//        regParam = 1,
//        tol = 0.1)
//    ),
      MLBenchmark(
        LogisticRegressionBenchmark2,
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
