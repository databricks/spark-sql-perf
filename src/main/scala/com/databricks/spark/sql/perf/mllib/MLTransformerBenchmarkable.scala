package com.databricks.spark.sql.perf.mllib

import com.databricks.spark.sql.perf._
import com.typesafe.scalalogging.slf4j.{LazyLogging => Logging}

import org.apache.spark.sql._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.ml.Transformer

class MLTransformerBenchmarkable(
    params: MLParams,
    test: BenchmarkAlgorithm,
    sqlContext: SQLContext)
  extends Benchmarkable with Serializable with Logging {

  import MLTransformerBenchmarkable._

  private var testData: DataFrame = null
  private var trainingData: DataFrame = null
  private var testDataCount: Option[Long] = None
  private val param = MLBenchContext(params, sqlContext)

  override val name = test.name

  override protected val executionMode: ExecutionMode = ExecutionMode.SparkPerfResults

  override protected def beforeBenchmark(): Unit = {
    logger.info(s"$this beforeBenchmark")
    try {
      testData = test.testDataSet(param)
      testData.cache()
      testDataCount = Some(testData.count())
      trainingData = test.trainingDataSet(param)
      trainingData.cache()
      trainingData.count()
    } catch {
      case e: Throwable =>
        println(s"$this error in beforeBenchmark: ${e.getStackTraceString}")
        throw e
    }
  }

  override protected def doBenchmark(
    includeBreakdown: Boolean,
    description: String,
    messages: ArrayBuffer[String]): BenchmarkResult = {
    try {
      val (trainingTime, model: Transformer) = measureTime {
        logger.info(s"$this: train: trainingSet=${trainingData.schema}")
        val estimator = test.getEstimator(param)
        estimator.fit(trainingData)
      }
      logger.info(s"model: $model")
      val (_, scoreTraining) = measureTime {
        test.score(param, trainingData, model)
      }
      val (scoreTestTime, scoreTest) = measureTime {
        test.score(param, testData, model)
      }


      val ml = MLResult(
        trainingTime = Some(trainingTime.toMillis),
        trainingMetric = Some(scoreTraining),
        testTime = Some(scoreTestTime.toMillis),
        testMetric = Some(scoreTest / testDataCount.get))

      BenchmarkResult(
        name = name,
        mode = executionMode.toString,
        parameters = Map.empty,
        executionTime = Some(trainingTime.toMillis),
        mlParams = Some(params),
        mlResult = Some(ml))
    } catch {
      case e: Exception =>
        BenchmarkResult(
          name = name,
          mode = executionMode.toString,
          parameters = Map.empty,
          mlParams = Some(params),
          failure = Some(Failure(e.getClass.getSimpleName,
            e.getMessage + ":\n" + e.getStackTraceString)))
    } finally {
      Option(testData).map(_.unpersist())
      Option(trainingData).map(_.unpersist())
    }
  }

  def prettyPrint: String = {
    val paramString = pprint(params).mkString("\n")
    s"$test\n$paramString"
  }


}

object MLTransformerBenchmarkable {
  private def pprint(p: AnyRef): Seq[String] = {
    val m = getCCParams(p)
    m.flatMap {
      case (key, Some(value: Any)) => Some(s"  $key=$value")
      case _ => None
    } .toSeq
  }

  // From http://stackoverflow.com/questions/1226555/case-class-to-map-in-scala
  private def getCCParams(cc: AnyRef) =
    (Map[String, Any]() /: cc.getClass.getDeclaredFields) {(a, f) =>
      f.setAccessible(true)
      a + (f.getName -> f.get(cc))
    }
}

