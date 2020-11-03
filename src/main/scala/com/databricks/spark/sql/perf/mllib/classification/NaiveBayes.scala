package com.databricks.spark.sql.perf.mllib.classification

import org.apache.spark.ml
import org.apache.spark.ml.{ModelBuilderSSP, PipelineStage, Transformer}
import org.apache.spark.ml.evaluation.{Evaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.linalg.{DenseMatrix, Vectors}

import com.databricks.spark.sql.perf.mllib.OptionImplicits._
import com.databricks.spark.sql.perf.mllib._
import com.databricks.spark.sql.perf.mllib.data.DataGenerator

/** Object containing methods used in performance tests for (multinomial) NaiveBayesModels */
object NaiveBayes extends BenchmarkAlgorithm
  with TestFromTraining with TrainingSetFromTransformer with ScoringWithEvaluator {

  override protected def initialData(ctx: MLBenchContext) = {
    import ctx.params._
    val rng = ctx.newGenerator()
    // Max possible arity of a feature in generated training/test data for NaiveBayes models
    val maxFeatureArity = 20
    // All features for Naive Bayes must be categorical, i.e. have arity >= 2
    val featureArity = 0.until(numFeatures).map(_ => 2 + rng.nextInt(maxFeatureArity - 2)).toArray
    DataGenerator.generateMixedFeatures(
      ctx.sqlContext,
      numExamples,
      ctx.seed(),
      numPartitions,
      featureArity)
  }

  override protected def trueModel(ctx: MLBenchContext): Transformer = {
    import ctx.params._
    val rng = ctx.newGenerator()
    // pi = log of class priors, whose dimension is C (number of classes)
    // theta = log of class conditional probabilities, whose dimension is C (number of classes)
    // by D (number of features)
    val unnormalizedProbs = 0.until(numClasses).map(_ => rng.nextDouble() + 1e-5).toArray
    val logProbSum = math.log(unnormalizedProbs.sum)
    val piArray = unnormalizedProbs.map(prob => math.log(prob) - logProbSum)

    // For class i, set the class-conditional probability of feature i to 0.7, and split up the
    // remaining probability mass across the other features
    val currClassProb = 0.7
    val thetaArray = Array.tabulate(numClasses) { i: Int =>
      val baseProbMass = (1 - currClassProb) / (numFeatures - 1)
      val probs = Array.fill[Double](numFeatures)(baseProbMass)
      probs(i) = currClassProb
      probs
    }.map(_.map(math.log))

    // Initialize new Naive Bayes model
    val pi = Vectors.dense(piArray)
    val theta = new DenseMatrix(numClasses, numFeatures, thetaArray.flatten, true)

    ModelBuilderSSP.newNaiveBayesModel(pi, theta)
  }

  override def getPipelineStage(ctx: MLBenchContext): PipelineStage = {
    import ctx.params._
    new ml.classification.NaiveBayes()
      .setSmoothing(smoothing)
  }

  override protected def evaluator(ctx: MLBenchContext): Evaluator =
    new MulticlassClassificationEvaluator()
}
