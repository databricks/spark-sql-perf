package com.databricks.spark.sql.perf.mllib.data

import com.databricks.spark.sql.perf.MLTestParameters
import org.apache.spark.SparkContext
import org.apache.spark.ml.linalg.{Vectors => NewVectors}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.random._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, DataFrame}

object DataGenerator {

  def generateBinaryLabeledPoints(sql: SQLContext, conf: MLTestParameters): DataFrame = {
    val threshold = 0.5
    val numPartitions = 10
    val rdd = RandomRDDs.randomRDD(sql.sparkContext,
      new BinaryLabeledDataGenerator(conf.numFeatures.get, threshold),
      conf.numExamples.get, numPartitions, conf.randomSee.get).map { p =>
      p.label -> NewVectors.dense(p.features.toArray) }
    sql.createDataFrame(rdd).toDF("label", "features")
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
    LabeledPoint(y, Vectors.dense(x))
  }

  override def setSeed(seed: Long) {
    rng.setSeed(seed)
  }

  override def copy(): BinaryLabeledDataGenerator =
    new BinaryLabeledDataGenerator(numFeatures, threshold)

}