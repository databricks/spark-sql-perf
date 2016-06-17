package com.databricks.spark.sql.perf.mllib.classification

import com.databricks.spark.sql.perf.ExtraMLTestParameters
import com.databricks.spark.sql.perf.mllib.data.DataGenerator
import com.databricks.spark.sql.perf.mllib.{ClassificationContext, ClassificationPipelineDescription}
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.ml.classification.LogisticRegression
import com.databricks.spark.sql.perf.mllib.OptionImplicits._


object LogisticRegressionBenchmark
  extends ClassificationPipelineDescription[LogisticRegressionModel] {

  type Param = ExtraMLTestParameters
  type Model = LogisticRegressionModel

  def trainingDataSet(ctx: ClassificationContext): DataFrame = {
    DataGenerator.generateBinaryLabeledPoints(
      ctx.sqlContext,
      ctx.commonParams.numExamples.get,
      ctx.commonParams)
  }

  def testDataSet(ctx: ClassificationContext): DataFrame = {
    DataGenerator.generateBinaryLabeledPoints(
      ctx.sqlContext,
      ctx.commonParams.numTestExamples.get,
      ctx.commonParams)
  }

  def train(ctx: ClassificationContext,
            trainingSet: DataFrame): Model = {
    import ctx.extraParams._
    val lr = new LogisticRegression()
        .setTol(tol)
        .setRegParam(regParam)
    lr.fit(trainingSet)
  }

  def score(ctx: ClassificationContext,
            testSet: DataFrame,
            model: Model): Double = {
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
