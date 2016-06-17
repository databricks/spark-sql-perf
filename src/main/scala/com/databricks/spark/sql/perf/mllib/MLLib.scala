package com.databricks.spark.sql.perf.mllib

import com.databricks.spark.sql.perf._
import org.apache.spark.SparkContext
import org.apache.spark.ml.classification.{ClassificationModel, RandomForestClassificationModel}
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

class MLLib(@transient sqlContext: SQLContext)
  extends Benchmark(sqlContext) with Serializable {

  def this() = this(SQLContext.getOrCreate(SparkContext.getOrCreate()))
}

object MLLib {
  def runDefault(runConfig: RunConfig): Unit = {
    val ml = new MLLib()
    val benchmarks = MLBenchmarks.benchmarkObjects
    ml.runExperiment(
      executionsToRun = benchmarks,
      resultLocation = "/test/results")
  }
}

case class ClassificationContext(
    commonParams: MLTestParameters,
    extraParams: ExtraMLTestParameters,
    sqlContext: SQLContext)

/**
 * The description of a benchmark for doing classification using the pipeline API.
 * @tparam Model a model that is being trained on
 */
trait ClassificationPipelineDescription[Model] {

  def trainingDataSet(ctx: ClassificationContext): DataFrame

  def testDataSet(ctx: ClassificationContext): DataFrame

  @throws[Exception]("if training fails")
  def train(ctx: ClassificationContext,
            trainingSet: DataFrame): Model

  @throws[Exception]("if scoring fails")
  def score(ctx: ClassificationContext,
                testSet: DataFrame, model: Model): Double
}

class MLClassificationBenchmarkable[Model](
    extraParam: ExtraMLTestParameters,
    commonParam: MLTestParameters,
    test: ClassificationPipelineDescription[Model],
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
          failure = Some(Failure(e.getClass.getSimpleName, e.getMessage)))
    }
  }
}



object OptionImplicits {
  // The following implicits are unrolled for safety:
  private def oX2X[A](x: Option[A]): A = x.get


  implicit def oD2D(x: Option[Double]): Double = oX2X(x)

  implicit def oS2S(x: Option[String]): String = oX2X(x)

  implicit def oI2I(x: Option[Int]): Int = oX2X(x)

  implicit def oL2L(x: Option[Long]): Long = oX2X(x)

  implicit def l2lo(x: Long): Option[Long] = Option(x)
  implicit def i2lo(x: Int): Option[Long] = Option(x.toLong)
  implicit def i2io(x: Int): Option[Int] = Option(x)
  implicit def d2do(x: Double): Option[Double] = Option(x)
  implicit def i2do(x: Int): Option[Double] = Option(x)
}