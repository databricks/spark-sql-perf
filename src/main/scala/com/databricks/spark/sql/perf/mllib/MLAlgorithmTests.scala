package com.databricks.spark.sql.perf.mllib

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkContext
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, Dataset}

import com.databricks.spark.sql.perf._
import com.databricks.spark.sql.perf.mllib.data.DataGenerator


/**
 * Parent class for MLlib Estimator tests.
 */
abstract class EstimatorTest(
    protected val configuration: MLTestParameters) extends Benchmarkable {

  override val name: String = this.getClass.getSimpleName

  // This is currently ignored by MLlib tests.
  override protected val executionMode: ExecutionMode = ExecutionMode.ForeachResults

  // The following fields should be initialized by `beforeBenchmark()`
  protected var trainingData: DataFrame = _
  protected var testData: Option[DataFrame] = _
  protected var estimator: Estimator = _
  protected var evaluator: Option[Evaluator] = _

  /**
   * Prepare the training data.  It does not need to be cached or materialized by this method.
   * @return (training data, optional test data)
   */
  protected def getData: (DataFrame, Option[DataFrame])

  /** Prepare Estimator */
  protected def getEstimator: Estimator

  /** Prepare Evaluator.  Defaults to None.  */
  protected def getEvaluator: Evaluator = None

  final override protected def beforeBenchmark(): Unit = {
    (trainingData, testData) = getData
    trainingData.cache().count()
    testData.map(_.cache().count())
    estimator = getEstimator
    evaluator = getEvaluator

    // Process parameters
    setTestParameters(estimator)
  }

  protected def setTestParameters(estimator: Estimator): Unit = {

  }

  // includeBreakdown is not used by MLlib
  final override protected def doBenchmark(
      includeBreakdown: Boolean,
      description: String = "",
      messages: ArrayBuffer[String]): BenchmarkResult = {
    require(trainingData != null, "EstimatorTest tried to run without trainingData." +
      "  Data should be prepared in beforeBenchmark() method.")
    require(estimator != null, "EstimatorTest tried to run without Estimator." +
      "  Estimator should be prepared in beforeBenchmark() method.")
    try {
      var start = System.currentTimeMillis()
      val model = estimator.fit(trainingData)
      val trainingTime = (System.currentTimeMillis() - start).toDouble / 1000.0

      start = System.currentTimeMillis()
      val trainingPredictions = model.transform(trainingData)
      val trainingMetric = evaluator.evaluate(trainingPredictions)
      val trainingTransformTime = (System.currentTimeMillis() - start).toDouble / 1000.0

      start = System.currentTimeMillis()
      val testPredictions = model.transform(testData)
      val testMetric = evaluator.evaluate(testPredictions)
      val testTime = (System.currentTimeMillis() - start).toDouble / 1000.0

      val mlRes = MLResult(trainingTime = Some(trainingTime),
        trainingTransformTime = Some(trainingTransformTime), testTime = Some(testTime),
        trainingMetric = Some(trainingMetric), testMetric = Some(testMetric))
      BenchmarkResult(name, executionMode.toString, mlParameters = Some(configuration),
        mlResult = Some(mlRes))
    } catch {
      case e: Exception =>
        BenchmarkResult(
          name = name,
          mode = executionMode.toString,
          mlParameters = Some(configuration),
          failure = Some(Failure(e.getClass.getSimpleName, e.getMessage)))
    }
  }
}


abstract class ClassificationTest(configuration: MLTestParameters)
  extends EstimatorTest(configuration) {

  override protected def getData: (DataFrame, Option[DataFrame]) = {

  }

  override protected def getEvaluator: Evaluator = new RegressionEvaluator
}

class LogisticRegressionTest(configuration: MLTestParameters)
  extends ClassificationTest(configuration) {

  override protected def getEstimator: Estimator = new LogisticRegression().

  override def runTest(rdd: DataFrame): LogisticRegressionModel = {
    val lr = new LogisticRegression()
    lr.fit(rdd)
  }

  override def validate(model: LogisticRegressionModel, ds: DataFrame): Double = {
    calculateRMSE(model.transform(ds).select("label", model.getPredictionCol),
      conf.numExamples.get)
  }

  def trainingRDD(conf: MLTestParameters): DataFrame = ???
//    DataGenerator.generateBinaryLabeledPoints(session, conf)

  def testRDD(conf: MLTestParameters): DataFrame = ???
//    DataGenerator.generateBinaryLabeledPoints(session, conf)

}
