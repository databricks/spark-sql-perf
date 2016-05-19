package com.databricks.spark.sql.perf.mllib

import com.databricks.spark.sql.perf._
import com.databricks.spark.sql.perf.mllib.data.DataGenerator
import org.apache.spark.SparkContext
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, Dataset}

import scala.collection.mutable.ArrayBuffer


/** Parent class for tests which run on a large dataset. */
abstract class RegressionAndClassificationTests[M](
    sc: SparkContext,
    private val baseName: String,
    private val configuration: MLTestParameters,
    override val executionMode: ExecutionMode) extends Benchmarkable {

  val session = SQLContext.getOrCreate(sc)

  import session.implicits._

  def runTest(rdd: DataFrame): M

  def validate(model: M, rdd: DataFrame): Double

  def trainingRDD(conf: MLTestParameters): DataFrame

  def testRDD(conf: MLTestParameters): DataFrame

  private lazy val ds = {
    trainingRDD(configuration)
  }

  private lazy val testDs = {
    testRDD(configuration)
  }

  final override val name: String = {
    val p1 = configuration.numExamples.map(x => s"_n$x").getOrElse("")
    val p2 = configuration.numFeatures.map(x => s"_f$x").getOrElse("")
    s"$baseName$p1$p2"
  }

  final override def doBenchmark(
      includeBreakdown: Boolean,
      description: String = "",
      messages: ArrayBuffer[String]): BenchmarkResult = {
    var start = System.currentTimeMillis()
    val model = runTest(ds)
    val trainingTime = (System.currentTimeMillis() - start).toDouble / 1000.0

    start = System.currentTimeMillis()
    val trainingMetric = validate(model, ds)
    val testTime = (System.currentTimeMillis() - start).toDouble / 1000.0

    val testMetric = validate(model, testDs)

    val mlRes = MLResult(trainingTime = Some(trainingTime), testTime = Some(testTime),
      trainingMetric = Some(trainingMetric), testMetric = Some(testMetric))
    BenchmarkResult(name, executionMode.toString, ml = Some(mlRes))
  }

  /**
   * For classification
   *
   * @param predictions RDD over (prediction, truth) for each instance
   * @return Percent correctly classified
   */
  def calculateAccuracy(predictions: RDD[(Double, Double)], numExamples: Long): Double = {
    predictions.map{case (pred, label) =>
      if (pred == label) 1.0 else 0.0
    }.sum() * 100.0 / numExamples
  }

  /**
   * For regression
   *
   * @param predictions RDD over (prediction, truth) for each instance
   * @return Root mean squared error (RMSE)
   */
  def calculateRMSE(predictions: Dataset[_], numExamples: Long): Double = {
    val error = predictions.rdd.map { case Row(pred: Double, label: Double) =>
      (pred - label) * (pred - label)
    } .sum()
    math.sqrt(error / numExamples)
  }
}

class LogisticRegressionTest(sc: SparkContext, conf: MLTestParameters, mode: ExecutionMode)
  extends RegressionAndClassificationTests[LogisticRegressionModel](sc, "logistic_regression",
    conf, mode) {

  override def runTest(rdd: DataFrame): LogisticRegressionModel = {
    val lr = new LogisticRegression()
    lr.fit(rdd)
  }

  override def validate(model: LogisticRegressionModel, ds: DataFrame): Double = {
    calculateRMSE(model.transform(ds).select("label", model.getPredictionCol),
      conf.numExamples.get)
  }

  def trainingRDD(conf: MLTestParameters): DataFrame =
    DataGenerator.generateBinaryLabeledPoints(session, conf)

  def testRDD(conf: MLTestParameters): DataFrame =
    DataGenerator.generateBinaryLabeledPoints(session, conf)

}


