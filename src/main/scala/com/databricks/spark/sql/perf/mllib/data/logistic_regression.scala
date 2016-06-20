package com.databricks.spark.sql.perf.mllib.data

import com.databricks.spark.sql.perf.MLTestParameters
import org.apache.spark.SparkContext
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.mllib.random._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, DataFrame}

object DataGenerator {

  def generateBinaryLabeledPoints(
      sql: SQLContext,
      numPoints: Long,
      conf: MLTestParameters): DataFrame = {
    val threshold = 0.5
    val numPartitions = 10
    val rdd = RandomRDDs.randomRDD(sql.sparkContext,
      new BinaryLabeledDataGenerator(conf.numFeatures.get, threshold),
      conf.numExamples.get, numPartitions, conf.randomSeed.get).map { p =>
      p.label -> Vectors.dense(p.features.toArray) }
    sql.createDataFrame(rdd).toDF("label", "features")
  }

  def generateFeatures(
      sql: SQLContext,
      numExamples: Long,
      seed: Long,
      numPartitions: Int,
      numFeatures: Int): DataFrame = {
    val categoricalArities = Array.empty[Int]
    val rdd: RDD[Vector] = RandomRDDs.randomRDD(sql.sparkContext,
            new FeaturesGenerator(categoricalArities, numFeatures),
           numExamples, numPartitions, seed)
    sql.createDataFrame(rdd.map(Tuple1.apply)).toDF("features")
  }
}

class BinaryLabeledDataGenerator(
    private val numFeatures: Int,
    private val threshold: Double) extends RandomDataGenerator[LabeledPoint] {

  private val rng = new java.util.Random()

  override def nextValue(): LabeledPoint = {
    val y = if (rng.nextDouble() < threshold) 0.0 else 1.0
    val x = Array.fill[Double](numFeatures) {
      if (rng.nextDouble() < threshold) 0.0 else 1.0
    }
    ???
//    LabeledPoint(y, Vectors.dense(x))
  }

  override def setSeed(seed: Long) {
    rng.setSeed(seed)
  }

  override def copy(): BinaryLabeledDataGenerator =
    new BinaryLabeledDataGenerator(numFeatures, threshold)

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
      // Generating some centered data
      arr(j) = 2 * rng.nextDouble() - 1
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