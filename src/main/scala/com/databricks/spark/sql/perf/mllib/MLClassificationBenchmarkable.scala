package com.databricks.spark.sql.perf.mllib

import com.databricks.spark.sql.perf._
import com.typesafe.scalalogging.slf4j.Logging
import org.apache.spark.sql._

import scala.collection.mutable.ArrayBuffer

// TODO(tjh) rename, this is not only about classification
class MLClassificationBenchmarkable(
    extraParam: ExtraMLTestParameters,
    commonParam: MLTestParameters,
    test: BenchmarkAlgorithm,
    sqlContext: SQLContext)
  extends Benchmarkable with Serializable with Logging {

  import MLClassificationBenchmarkable._

  private var testData: DataFrame = null
  private var trainingData: DataFrame = null
  val param = ClassificationContext(commonParam, extraParam, sqlContext)

  override val name = test.getClass.getCanonicalName

  override val executionMode: ExecutionMode = ExecutionMode.SparkPerfResults

  override def beforeBenchmark(): Unit = {
    logger.info(s"$this beforeBenchmark")
    try {
      testData = test.testDataSet(param)
      testData.cache()
      testData.count()
      trainingData = test.trainingDataSet(param)
      trainingData.cache()
      trainingData.count()
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
    try {
      val (trainingTime, model) = measureTime(test.train(param, trainingData))
      logger.info(s"model: $model")
      val (_, scoreTraining) = measureTime {
        test.score(param, trainingData, model)
      }
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
    } finally {
      Option(testData).map(_.unpersist())
      Option(trainingData).map(_.unpersist())
    }
  }

  def prettyPrint: String = {
    val params = (pprint(commonParam) ++ pprint(extraParam)).mkString("\n")
    s"$test\n$params"
  }


}

object MLClassificationBenchmarkable {
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

