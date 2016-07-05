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

  /**
   * Generate data from a Gaussian mixture model.
   * @param numCenters  Number of clusters in mixture
   */
  def generateGaussianMixtureData(
      sql: SQLContext,
      numCenters: Int,
      numExamples: Long,
      seed: Long,
      numPartitions: Int,
      numFeatures: Int): DataFrame = {
    val rdd: RDD[Vector] = RandomRDDs.randomRDD(sql.sparkContext,
      new GaussianMixtureDataGenerator(numCenters, numFeatures, seed), numExamples, numPartitions,
      seed)
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


/**
 * Generate data from a Gaussian mixture model.
 */
class GaussianMixtureDataGenerator(
    val numCenters: Int,
    val numFeatures: Int,
    val seed: Long) extends RandomDataGenerator[Vector] {

  private val rng = new java.util.Random(seed)
  private val rng2 = new java.util.Random(seed + 24)
  private val scale_factors = Array.fill(numCenters)(rng.nextInt(20) - 10)

  // Have a random number of points around a cluster
  private val concentrations: Seq[Double] = {
    val rand = Array.fill(numCenters)(rng.nextDouble())
    val randSum = rand.sum
    val scaled = rand.map(x => x / randSum)

    (1 to numCenters).map{i =>
      scaled.slice(0, i).sum
    }
  }

  private val centers = (0 until numCenters).map{i =>
    Array.fill(numFeatures)((2 * rng.nextDouble() - 1) * scale_factors(i))
  }

  override def nextValue(): Vector = {
    val pick_center_rand = rng2.nextDouble()
    val center = centers(concentrations.indexWhere(p => pick_center_rand <= p))
    Vectors.dense(Array.tabulate(numFeatures)(i => center(i) + rng2.nextGaussian()))
  }

  override def setSeed(seed: Long) {
    rng.setSeed(seed)
    rng2.setSeed(seed + 24)
  }

  override def copy(): GaussianMixtureDataGenerator =
    new GaussianMixtureDataGenerator(numCenters, numFeatures, seed)
}
