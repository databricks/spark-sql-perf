package com.databricks.spark.sql.perf.mllib.classification

/*

import com.databricks.spark.sql.perf.mllib.data.DataGenerator
import com.databricks.spark.sql.perf.mllib.{ClassificationContext, ClassificationPipelineDescription}
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.ml.classification.LogisticRegression

case class LogisticRegressionTest(regParam: Double, tol: Double)

// TODO: Do we need this?  I'm not sure what this does beyond what is already done in MLAlgorithmTests.
object LogisticRegressionBenchmark
  extends ClassificationPipelineDescription[LogisticRegressionTest, LogisticRegressionModel] {

  type Param = LogisticRegressionTest
  type Model = LogisticRegressionModel

  def trainingDataSet(ctx: ClassificationContext[Param]): DataFrame = {
    DataGenerator.generateBinaryLabeledPoints(
      ctx.sqlContext,
      ctx.commonParams.numExamples.get,
      ctx.commonParams)
  }

  def testDataSet(ctx: ClassificationContext[Param]): DataFrame = {
    DataGenerator.generateBinaryLabeledPoints(
      ctx.sqlContext,
      ctx.commonParams.numTestExamples.get,
      ctx.commonParams)
  }

  @throws[Exception]("if training fails")
  def train(ctx: ClassificationContext[Param],
            trainingSet: DataFrame): Model = {
    import ctx.extraParams._
    val lr = new LogisticRegression()
        .setTol(tol)
        .setRegParam(regParam)
    lr.fit(trainingSet)
  }

  @throws[Exception]("if scoring fails")
  def score(ctx: ClassificationContext[Param],
            testSet: DataFrame, model: Model): Double = {
    val eval = model.evaluate(testSet)
    Utils.accuracy(eval.predictions)
  }

}

object Utils {
  def accuracy(set: DataFrame): Double = {
    val session = set.sqlContext.sparkSession
    import session.implicits._
    val counts = set.map { case Row(pred: Double, label: Double) =>
      if (pred == label) 1.0 else 0.0
    } .rdd.sum()
    100.0 * counts / set.count()
  }
}
*/
