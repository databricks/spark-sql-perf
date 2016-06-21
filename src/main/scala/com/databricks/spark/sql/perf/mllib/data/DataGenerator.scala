package com.databricks.spark.sql.perf.mllib.data

import com.databricks.spark.sql.perf.MLTestParameters

import org.apache.spark.SparkContext
import org.apache.spark.ml.PredictionModel
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.mllib.random._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
 * Data generation utility.
 */
object DataGenerator {

  /**
   * Generate features data.
   * @return  DataFrame[features: Vector]
   *
   * TODO: Modify this, or create other methods to support categorical features.
   */
  def generateFeatures(
      sql: SQLContext,
      numExamples: Long,
      conf: MLTestParameters,
      seed: Long): DataFrame = {
    val categoricalArities = Array.empty[Int]
    val rdd: RDD[Vector] = RandomRDDs.randomRDD(sql.sparkContext,
      new FeaturesGenerator(categoricalArities, conf.numFeatures.get),
      numExamples, conf.numPartitions.get, seed)
    sql.createDataFrame(rdd.map(Tuple1.apply)).toDF("features")
  }

  /**
   * Generate a DataFrame[features: Vector, label: Double] where the label is produced using
   * the given model.
   */
  def generateLabeledPoints(
      sql: SQLContext,
      numExamples: Long,
      conf: MLTestParameters,
      model: PredictionModel[_, _],
      seed: Long): DataFrame = {
    val features = generateFeatures(sql, numExamples, conf, seed)
    model.setPredictionCol("label")
    model.transform(features)
  }
}


/**
 * Generator for a feature vector which can include a mix of categorical and continuous features.
 * @param categoricalArities  Specifies the number of categories for each categorical feature.
 * @param numContinuous  Number of continuous features.  Feature values are in range [0,1].
 */
class FeaturesGenerator(val categoricalArities: Array[Int], val numContinuous: Int)
  extends RandomDataGenerator[Vector] {

  categoricalArities.foreach { arity =>
    require(arity >= 2, s"FeaturesGenerator given categorical arity = $arity, " +
      s"but arity should be >= 2.")
  }

  val numFeatures = categoricalArities.length + numContinuous

  private val rng = new java.util.Random()

  /**
   * Generates vector with categorical features first, and continuous features in [0,1] second.
   */
  override def nextValue(): Vector = {
    // Feature ordering matches getCategoricalFeaturesInfo.
    val arr = new Array[Double](numFeatures)
    var j = 0
    while (j < categoricalArities.length) {
      arr(j) = rng.nextInt(categoricalArities(j))
      j += 1
    }
    while (j < numFeatures) {
      arr(j) = rng.nextDouble()
      j += 1
    }
    Vectors.dense(arr)
  }

  override def setSeed(seed: Long) {
    rng.setSeed(seed)
  }

  override def copy(): FeaturesGenerator = new FeaturesGenerator(categoricalArities, numContinuous)

  /**
   * @return categoricalFeaturesInfo Map storing arity of categorical features.
   *                                 E.g., an entry (n -> k) indicates that feature n is categorical
   *                                 with k categories indexed from 0: {0, 1, ..., k-1}.
   */
  def getCategoricalFeaturesInfo: Map[Int, Int] = {
    // Categorical features are indexed from 0 because of the implementation of nextValue().
    categoricalArities.zipWithIndex.map(_.swap).toMap
  }
}
