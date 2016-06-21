package com.databricks.spark.sql.perf.mllib

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.ml.{Estimator, Model, ModelBuilder}
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation._
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.DataFrame

import com.databricks.spark.sql.perf._
import com.databricks.spark.sql.perf.mllib.data.DataGenerator


/**
 * Parent class for MLlib Estimator tests.
 */
abstract class EstimatorTest(
    protected val conf: MLTestParameters) extends Benchmarkable {

  override val name: String = this.getClass.getSimpleName

  // This is currently ignored by MLlib tests.
  override protected val executionMode: ExecutionMode = ExecutionMode.ForeachResults

  // The following fields should be initialized by `beforeBenchmark()`
  protected var trainingData: DataFrame = _
  protected var testData: Option[DataFrame] = _
  protected var estimator: Estimator[_ <: Model[_]] = _
  protected var evaluator: Option[Evaluator] = _

  private def getSeed: Long = conf.seed.getOrElse(new java.util.Random().nextLong())
  protected val rng = new java.util.Random(getSeed)

  /**
   * Prepare the training data.  It does not need to be cached or materialized by this method.
   *
   * @return (training data, optional test data)
   */
  protected def getData: (DataFrame, Option[DataFrame])

  /** Prepare Estimator */
  protected def getEstimator: Estimator[_ <: Model[_]]

  /** Prepare Evaluator.  Defaults to None.  */
  protected def getEvaluator: Option[Evaluator] = None

  final override protected def beforeBenchmark(): Unit = {
    val (training, test) = getData
    trainingData = training
    testData = test
    trainingData.cache().count()
    testData.map(_.cache().count())
    estimator = getEstimator
    evaluator = getEvaluator
  }

  /**
   * Either evaluate the predictions, or count the predictions to materialize them.
   *
   * @return  Evaluation metric, or 0.0 if no evaluator is available
   */
  private def evaluateOrMaterialize(predictions: DataFrame): Double = evaluator match {
    case Some(eval) =>
      eval.evaluate(predictions)
    case None =>
      predictions.count()
      0.0
  }

  // includeBreakdown is not used by MLlib
  final override protected def doBenchmark(
      includeBreakdown: Boolean,
      description: String = "",
      messages: ArrayBuffer[String]): BenchmarkResult = {
    try {
      var start = System.currentTimeMillis()
      val model: Model[_] = estimator.fit(trainingData)
      val trainingTime = (System.currentTimeMillis() - start).toDouble / 1000.0

      start = System.currentTimeMillis()
      val trainingPredictions = model.transform(trainingData)
      val trainingMetric: Double = evaluateOrMaterialize(trainingPredictions)
      val trainingTransformTime = (System.currentTimeMillis() - start).toDouble / 1000.0

      val (testMetric, testTime) = testData match {
        case Some(data) =>
          start = System.currentTimeMillis()
          val testPredictions = model.transform(data)
          val testMetric = evaluateOrMaterialize(testPredictions)
          val testTime = (System.currentTimeMillis() - start).toDouble / 1000.0
          (testMetric, testTime)
        case None => (0.0, 0.0)
      }

      val mlRes = MLResult(trainingTime = Some(trainingTime),
        trainingTransformTime = Some(trainingTransformTime), testTime = Some(testTime),
        trainingMetric = Some(trainingMetric), testMetric = Some(testMetric))
      BenchmarkResult(name, executionMode.toString, mlParameters = Some(conf),
        mlResult = Some(mlRes))
    } catch {
      case e: Exception =>
        BenchmarkResult(
          name = name,
          mode = executionMode.toString,
          mlParameters = Some(conf),
          failure = Some(Failure(e.getClass.getSimpleName, e.getMessage)))
    }
  }
}


abstract class ClassificationTest(conf: MLTestParameters)
  extends EstimatorTest(conf) {

  override protected def getEvaluator: Option[Evaluator] =
    Some(new MulticlassClassificationEvaluator)
}


class LogisticRegressionTest(conf: MLTestParameters)
  extends ClassificationTest(conf) {

  private def generateModel(): LogisticRegressionModel = {
    val coefficients =
      Vectors.dense(Array.fill[Double](conf.numFeatures.get)(2 * rng.nextDouble() - 1))
    val intercept = 2 * rng.nextDouble - 1
    ModelBuilder.newLogisticRegressionModel(coefficients, intercept)
  }

  override protected def getData: (DataFrame, Option[DataFrame]) = {
    val trueModel = generateModel()
    val trainingData = DataGenerator.generateLabeledPoints(sqlContext, conf.numExamples.get,
      conf, trueModel, rng.nextLong())
    val testData = DataGenerator.generateLabeledPoints(sqlContext, conf.getNumTestExamples,
      conf, trueModel, rng.nextLong())
    (trainingData, Some(testData))
  }

  override protected def getEstimator: LogisticRegression = new LogisticRegression()
  // TODO: set stuff!
}

object LogisticRegressionTest {

  def mini: LogisticRegressionTest = {
    val conf = new MLTestParameters(numFeatures = Some(10), numExamples = Some(100),
      numPartitions = Some(4), numTestExamples = Some(100))
    new LogisticRegressionTest(conf)
  }

  def basic: LogisticRegressionTest = {
    val conf = new MLTestParameters(numFeatures = Some(10000), numExamples = Some(1000000),
      numPartitions = Some(128), numTestExamples = Some(1000000))
    new LogisticRegressionTest(conf)
  }
}
