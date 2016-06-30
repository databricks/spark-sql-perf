package com.databricks.spark.sql.perf.mllib.data

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.mllib.random._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, DataFrame}


object DataGenerator {

  def generateContinuousFeatures(
      sql: SQLContext,
      numExamples: Long,
      seed: Long,
      numPartitions: Int,
      numFeatures: Int): DataFrame = {
    val featureArity = Array.fill[Int](numFeatures)(0)
    val rdd: RDD[Vector] = RandomRDDs.randomRDD(sql.sparkContext,
      new FeaturesGenerator(featureArity), numExamples, numPartitions, seed)
    sql.createDataFrame(rdd.map(Tuple1.apply)).toDF("features")
  }

  /**
   * Generate a mix of continuous and categorical features.
   * @param featureArity  Array of length numFeatures, where 0 indicates a continuous feature and
   *                      a value > 0 indicates a categorical feature with that arity.
   */
  def generateMixedFeatures(
      sql: SQLContext,
      numExamples: Long,
      seed: Long,
      numPartitions: Int,
      featureArity: Array[Int]): DataFrame = {
    val rdd: RDD[Vector] = RandomRDDs.randomRDD(sql.sparkContext,
      new FeaturesGenerator(featureArity), numExamples, numPartitions, seed)
    sql.createDataFrame(rdd.map(Tuple1.apply)).toDF("features")
  }
}


/**
 * Generator for a feature vector which can include a mix of categorical and continuous features.
 * @param featureArity  Length numFeatures, where 0 indicates continuous feature and > 0
 *                      indicates a categorical feature of that arity.
 */
class FeaturesGenerator(val featureArity: Array[Int])
  extends RandomDataGenerator[Vector] {

  featureArity.foreach { arity =>
    require(arity >= 0, s"FeaturesGenerator given categorical arity = $arity, " +
      s"but arity should be >= 0.")
  }

  val numFeatures = featureArity.length

  private val rng = new java.util.Random()

  /**
   * Generates vector with features in the order given by [[featureArity]]
   */
  override def nextValue(): Vector = {
    val arr = new Array[Double](numFeatures)
    var j = 0
    while (j < featureArity.length) {
      if (featureArity(j) == 0)
        arr(j) = 2 * rng.nextDouble() - 1 // centered uniform data
      else
        arr(j) = rng.nextInt(featureArity(j))
      j += 1
    }
    Vectors.dense(arr)
  }

  override def setSeed(seed: Long) {
    rng.setSeed(seed)
  }

  override def copy(): FeaturesGenerator = new FeaturesGenerator(featureArity)
}
