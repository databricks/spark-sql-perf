package com.databricks.spark.sql.perf.mllib.classification

import org.apache.spark.ml.ModelBuilder
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator, Evaluator}
import org.apache.spark.sql.DataFrame

import com.databricks.spark.sql.perf.mllib.OptionImplicits._
import com.databricks.spark.sql.perf.mllib._
import com.databricks.spark.sql.perf.mllib.data.DataGenerator


object DecisionTreeClassification extends BenchmarkAlgorithm
  with TestFromTraining with TrainingSetFromTransformer with ScoringWithEvaluator {

  override protected def initialData(ctx: MLBenchContext) = {
    import ctx.params._
    DataGenerator.generateFeatures(ctx.sqlContext, numExamples, ctx.seed(), numPartitions,
      numFeatures)
  }

  override protected def trueModel(ctx: MLBenchContext): Transformer = {
    //val rng = ctx.newGenerator()
    val numFeatures = ctx.params.numFeatures.get
    val fourthFeatures = numFeatures / 4
    val featureArity: Array[Int] =
      Array.fill[Int](fourthFeatures)(2) ++ // low-arity categorical
        Array.fill[Int](fourthFeatures)(20) ++ // high-arity categorical
        Array.fill[Int](numFeatures - 2 * fourthFeatures)(0) // continuous
    ModelBuilder.newDecisionTreeClassificationModel(ctx.params.depth.get, ctx.params.numClasses.get,
      featureArity, ctx.seed())
  }

  override def train(ctx: MLBenchContext, trainingSet: DataFrame): Transformer = {
    logger.info(s"$this: train: trainingSet=${trainingSet.schema}")
    import ctx.params._
    new DecisionTreeClassifier()
      .setMaxDepth(depth.get)
      .fit(trainingSet)
  }

  override protected def evaluator(ctx: MLBenchContext): Evaluator =
    new MulticlassClassificationEvaluator()
}
