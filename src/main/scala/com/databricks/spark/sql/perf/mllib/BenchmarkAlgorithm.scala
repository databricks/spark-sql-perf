package com.databricks.spark.sql.perf.mllib

import org.apache.spark.ml.attribute.{NominalAttribute, NumericAttribute}
import org.apache.spark.ml.{Estimator, PipelineStage, Transformer}
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import com.databricks.spark.sql.perf._

/**
 * The description of a benchmark for an ML algorithm. It follows a simple, standard proceduce:
 *  - generate some test and training data
 *  - generate a model against the training data
 *  - score the model against the training data
 *  - score the model against the test data
 *
 * You should not assume that your implementation can carry state around. If some state is needed,
 * consider adding it to the context.
 *
 * It is assumed that the implementation is going to be an object.
 */
trait BenchmarkAlgorithm {

  def trainingDataSet(ctx: MLBenchContext): DataFrame

  def testDataSet(ctx: MLBenchContext): DataFrame

  /**
   * Create an [[Estimator]] or [[Transformer]] with params set from the given [[MLBenchContext]].
   */
  def getPipelineStage(ctx: MLBenchContext): PipelineStage

  /**
   * The unnormalized score of the training procedure on a dataset. The normalization is
   * performed by the caller.
   * This calls `count()` on the transformed data to attempt to materialize the result for
   * recording timing metrics.
   */
  @throws[Exception]("if scoring fails")
  def score(
      ctx: MLBenchContext,
      testSet: DataFrame,
      model: Transformer): MLMetric = {
    val output = model.transform(testSet)
    // We create a useless UDF to make sure the entire DataFrame is instantiated.
    val fakeUDF = udf { (_: Any) => 0 }
    val columns = testSet.columns
    output.select(sum(fakeUDF(struct(columns.map(col) : _*)))).first()
    MLMetric.Invalid
  }

  def name: String = {
    this.getClass.getCanonicalName.replace("$", "")
  }

  /**
   * Test additional methods for some algorithms.
   *
   * @param transformer The transformer which includes additional methods.
   * @return A map which key is the additional method name, and value is a function which runs
   *         the corresponding method.
   */
  def testAdditionalMethods(
      ctx: MLBenchContext,
      transformer: Transformer): Map[String, () => _] = Map.empty[String, () => _]
}

/**
 * Uses an evaluator to perform the scoring.
 */
trait ScoringWithEvaluator {
  self: BenchmarkAlgorithm =>

  protected def evaluator(ctx: MLBenchContext): Evaluator

  final override def score(
      ctx: MLBenchContext,
      testSet: DataFrame,
      model: Transformer): MLMetric = {
    val results = model.transform(testSet)
    val eval = evaluator(ctx)
    val metricName = if (eval.hasParam("metricName")) {
      val param = eval.getParam("metricName")
      eval.getOrDefault(param).toString
    } else {
      eval.getClass.getSimpleName
    }
    val metricValue = eval.evaluate(results)
    MLMetric(metricName, metricValue, eval.isLargerBetter)
  }
}

/**
 * Builds the training set for an initial dataset and an initial model. Useful for validating a
 * trained model against a given model.
 */
trait TrainingSetFromTransformer {
  self: BenchmarkAlgorithm =>

  protected def initialData(ctx: MLBenchContext): DataFrame

  protected def trueModel(ctx: MLBenchContext): Transformer

  final override def trainingDataSet(ctx: MLBenchContext): DataFrame = {
    val initial = initialData(ctx)
    val model = trueModel(ctx)
    val fCol = col("features")
    // Special case for the trees: we need to set the number of labels.
    // numClasses is set? We will add the number of classes to the final column.
    val lCol = ctx.params.numClasses match {
      case Some(numClasses) =>
        val labelAttribute = if (numClasses == 0) {
          NumericAttribute.defaultAttr.withName("label")
        } else {
          NominalAttribute.defaultAttr.withName("label").withNumValues(numClasses)
        }
        val labelMetadata = labelAttribute.toMetadata()
        col("prediction").as("label", labelMetadata)
      case None =>
        col("prediction").as("label")
    }
    model.transform(initial).select(fCol, lCol)
  }
}

/**
 * The test data is the same as the training data.
 */
trait TestFromTraining {
  self: BenchmarkAlgorithm =>

  final override def testDataSet(ctx: MLBenchContext): DataFrame = {
    // Copy the context with a new seed.
    val ctx2 = ctx.params.randomSeed match {
      case Some(x) =>
        // Also set the number of examples to the number of test examples.
        assert(ctx.params.numTestExamples.nonEmpty, "You must specify test examples")
        val p = ctx.params.copy(randomSeed = Some(x + 1), numExamples = ctx.params.numTestExamples)
        ctx.copy(params = p)
      case None =>
        // Making a full copy to reset the internal seed.
        ctx.copy()
    }
    self.trainingDataSet(ctx2)
  }
}

