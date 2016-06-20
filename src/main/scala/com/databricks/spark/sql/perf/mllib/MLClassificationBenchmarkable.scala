package com.databricks.spark.sql.perf.mllib

import com.databricks.spark.sql.perf._
import org.apache.spark.sql._

import scala.collection.mutable.ArrayBuffer


class MLClassificationBenchmarkable(
                                     extraParam: ExtraMLTestParameters,
                                     commonParam: MLTestParameters,
                                     test: ClassificationPipelineDescription,
                                     sqlContext: SQLContext)
  extends Benchmarkable with Serializable {

  private var testData: DataFrame = null
  private var trainingData: DataFrame = null
  val param = ClassificationContext(commonParam, extraParam, sqlContext)

  override val name = test.getClass.getCanonicalName

  override val executionMode: ExecutionMode = ExecutionMode.SparkPerfResults

  override def beforeBenchmark(): Unit = {
    println(s"$this beforeBenchmark")
    try {
      // TODO(?) cache + prewarm the datasets
      testData = test.testDataSet(param)
      trainingData = test.trainingDataSet(param)

    } catch {
      case e: Throwable =>
        println(s"$this error in beforeBenchmark: ${e.getStackTraceString}")
        throw e
    }
  }

  override def doBenchmark(
                            includeBreakdown: Boolean,
                            description: String,
                            messages: ArrayBuffer[String]): BenchmarkResult = {
    println(s"entering doBenchmark")
    try {
      val (trainingTime, model) = measureTime(test.train(param, trainingData))
      println(s"model: $model")
      val (_, scoreTraining) = measureTime {
        test.score(param, trainingData, model)
      }
      println(s"scoreTraining: $scoreTraining")
      val (scoreTestTime, scoreTest) = measureTime {
        test.score(param, testData, model)
      }


      val ml = MLResult(
        testParameters = Some(commonParam),
        extraTestParameters = Some(extraParam),
        trainingTime = Some(trainingTime.toMillis),
        trainingMetric = Some(scoreTraining),
        testTime = Some(scoreTestTime.toMillis),
        testMetric = Some(scoreTest))

      BenchmarkResult(
        name = name,
        mode = executionMode.toString,
        parameters = Map.empty,
        executionTime = Some(trainingTime.toMillis),
        ml = Some(ml))
    } catch {
      case e: Exception =>
        BenchmarkResult(
          name = name,
          mode = executionMode.toString,
          parameters = Map.empty,
          failure = Some(Failure(e.getClass.getSimpleName,
            e.getMessage + ":\n" + e.getStackTraceString)))
    }
  }
}

