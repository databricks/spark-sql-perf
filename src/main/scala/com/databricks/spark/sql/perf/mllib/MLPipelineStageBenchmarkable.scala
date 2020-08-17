package com.databricks.spark.sql.perf.mllib

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.apache.spark.ml.{Estimator, Transformer}
import org.apache.spark.sql._
import org.apache.spark.{SparkContext, SparkEnv}

import com.databricks.spark.sql.perf._

class MLPipelineStageBenchmarkable(
    params: MLParams,
    test: BenchmarkAlgorithm,
    sqlContext: SQLContext)
  extends Benchmarkable with Serializable {

  import MLPipelineStageBenchmarkable._

  private var testData: DataFrame = null
  private var trainingData: DataFrame = null
  private var testDataCount: Option[Long] = None
  private val param = MLBenchContext(params, sqlContext)

  override val name = test.name

  override protected val executionMode: ExecutionMode = ExecutionMode.SparkPerfResults

  override protected[mllib] def beforeBenchmark(): Unit = {
    logger.info(s"$this beforeBenchmark")
    try {
      testData = test.testDataSet(param)
      testData.cache()
      testDataCount = Some(testData.count())
      trainingData = test.trainingDataSet(param)
      trainingData.cache()
      trainingData.count()
    } catch {
      case NonFatal(e) =>
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
        test.getPipelineStage(param) match {
          case est: Estimator[_] => est.fit(trainingData)
          case transformer: Transformer =>
            transformer.transform(trainingData)
            transformer
          case other: Any => throw new UnsupportedOperationException("Algorithm to benchmark must" +
            s" be an estimator or transformer, found ${other.getClass} instead.")
        }
      }
      logger.info(s"model: $model")
      val (scoreTrainTime, scoreTraining) = measureTime {
        test.score(param, trainingData, model)
      }
      val metricTrainingTime = MLMetric("training.time", trainingTime.toMillis, false)
      val metricTraining = MLMetric("training."+scoreTraining.metricName,
        scoreTraining.metricValue,
        scoreTraining.isLargerBetter)
      val (scoreTestTime, scoreTest) = measureTime {
        test.score(param, testData, model)
      }
      val metricTestTime = MLMetric("test.time", scoreTestTime.toMillis, false)
      val metricTest = MLMetric("test."+scoreTraining.metricName,
        scoreTraining.metricValue,
        scoreTraining.isLargerBetter)

      logger.info(s"$this doBenchmark: Trained model in ${trainingTime.toMillis / 1000.0}" +
        s" s, Scored training dataset in ${scoreTrainTime.toMillis / 1000.0} s," +
        s" test dataset in ${scoreTestTime.toMillis / 1000.0} s")

      val additionalTests = test.testAdditionalMethods(param, model).map {
        tuple =>
          val (additionalMethodTime, _) = measureTime { tuple._2() }
          MLMetric(tuple._1, additionalMethodTime.toMillis, false)
      }.toArray

      val mlMetrics = Array(metricTrainingTime, metricTraining, metricTestTime, metricTest) ++
        additionalTests
      val paramsMap = params.toMap
      val benchmarkId = name.split('.').last + "_" + paramsMap.hashCode.abs

      BenchmarkResult(
        name = name,
        mode = executionMode.toString,
        parameters = paramsMap,
        executionTime = Some(trainingTime.toMillis),
        mlResult = Some(mlMetrics),
        benchmarkId = Some(benchmarkId))
    } catch {
      case e: Exception =>
        BenchmarkResult(
          name = name,
          mode = executionMode.toString,
          parameters = params.toMap,
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

object MLPipelineStageBenchmarkable {
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

