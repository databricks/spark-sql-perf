package com.databricks.spark.sql.perf.mllib

import com.databricks.spark.sql.perf._
import org.apache.spark.SparkContext
import org.apache.spark.ml.classification.{ClassificationModel, RandomForestClassificationModel}
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

class MLLib(@transient sqlContext: SQLContext)
  extends Benchmark(sqlContext) with Serializable {

  def this() = this(SQLContext.getOrCreate(SparkContext.getOrCreate()))
}

case class ClassificationContext[Param](
    commonParams: MLTestParameters,
    extraParams: Param,
    sqlContext: SQLContext)

/**
 * The description of a benchmark for doing classification using the pipeline API.
 *
 * @tparam Param some parameters that describe a full experiment
 * @tparam Model a model that is being trained on
 */
trait ClassificationPipelineDescription[Param, Model] {

  def trainingDataSet(ctx: ClassificationContext[Param]): DataFrame

  def testDataSet(ctx: ClassificationContext[Param]): DataFrame

  @throws[Exception]("if training fails")
  def train(ctx: ClassificationContext[Param],
            trainingSet: DataFrame): Model

  @throws[Exception]("if scoring fails")
  def score(ctx: ClassificationContext[Param],
                testSet: DataFrame, model: Model): Double
}

class MLClassificationBenchmarkable[Param, Model](
    extraParam: Param,
    commonParam: MLTestParameters,
    test: ClassificationPipelineDescription[Param, Model],
    sqlContext: SQLContext)
  extends Benchmarkable with Serializable {

  private var testData: DataFrame = null
  private var trainingData: DataFrame = null
  val param = ClassificationContext(commonParam, extraParam, sqlContext)

  override val name = test.getClass.getCanonicalName

  override val executionMode: ExecutionMode = ExecutionMode.SparkPerfResults

  override def beforeBenchmark(): Unit = {
    // TODO(?) cache + prewarm the datasets
    testData = test.testDataSet(param)
    trainingData = test.trainingDataSet(param)
  }

  override def doBenchmark(
      includeBreakdown: Boolean,
      description: String,
      messages: ArrayBuffer[String]): BenchmarkResult = {
    try {
      val (trainingTime, model) = measureTime(test.train(param, trainingData))
      val (scoreTrainingTime, scoreTraining) = measureTime {
        test.score(param, trainingData, model)
      }
      val (scoreTestTime, scoreTest) = measureTime {
        test.score(param, testData, model)
      }

      val ml = MLResult(
        testParameters = Some(commonParam),
        extraTestParameters = ConversionUtils.caseClassToMap(extraParam),
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
          failure = Some(Failure(e.getClass.getSimpleName, e.getMessage)))
    }
  }
}



/**
 * A class for benchmarking MLlib Spark perf results.
 *
 * @param name  Test name
 * @param parameters  Parameters for test.  These are recorded here but set beforehand
 *                    (contained within the prepare, train, and test methods).
 * @param prepare  Prepare data.  Not timed.
 * @param train  Train the model.  Timed.
 * @param evaluateTrain  Compute training metric
 * @param evaluateTest  Compute test metric
 */
class MLlibSparkPerfExecution(
    override val name: String,
    parameters: Map[String, String],
    prepare: () => Unit,
    train: () => Unit,
    evaluateTrain: () => Option[Double],
    evaluateTest: () => Option[Double],
    description: String = "")
  extends Benchmarkable with Serializable {

  override def toString: String =
    s"""
       |== $name ==
       |$description
     """.stripMargin

  protected override val executionMode: ExecutionMode = ExecutionMode.SparkPerfResults

  protected override def beforeBenchmark(): Unit = { prepare() }

  protected override def doBenchmark(
      includeBreakdown: Boolean,
      description: String = "",
      messages: ArrayBuffer[String]): BenchmarkResult = {
    try {
      val trainingTime = measureTimeMs(train()) / 1e3
      val trainingMetric = evaluateTrain()
      val (testMetric, testTime) = {
        val startTime = System.nanoTime()
        val metric = evaluateTest()
        val endTime = System.nanoTime()
        (metric, (endTime - startTime).toDouble / 1e9)
      }

      val ml = MLResult(
        trainingTime = Some(trainingTime),
        trainingMetric = trainingMetric,
        testTime = Some(testTime),
        testMetric = testMetric)

      BenchmarkResult(
        name = name,
        mode = executionMode.toString,
        parameters = parameters,
        executionTime = Some(trainingTime),
        ml = Some(ml))
    } catch {
      case e: Exception =>
        BenchmarkResult(
          name = name,
          mode = executionMode.toString,
          parameters = parameters,
          failure = Some(Failure(e.getClass.getSimpleName, e.getMessage)))
    }
  }
}


object ConversionUtils {
  def caseClassToMap[X](cc: X): Map[String, String] = {
    // TODO(tjh) fix this
    Map.empty
  }
}