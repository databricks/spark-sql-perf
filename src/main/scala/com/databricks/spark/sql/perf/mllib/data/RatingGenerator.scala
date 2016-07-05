package com.databricks.spark.sql.perf.mllib.data

import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.mllib.random.RandomDataGenerator

import scala.collection.mutable

class RatingGenerator(
    private val numUsers: Int,
    private val numProducts: Int,
    private val implicitPrefs: Boolean) extends RandomDataGenerator[Rating[Int]] {

  private val rng = new java.util.Random()

  private val observed = new mutable.HashMap[(Int, Int), Boolean]()

  override def nextValue(): Rating[Int] = {
    var tuple = (rng.nextInt(numUsers),rng.nextInt(numProducts))
    while (observed.getOrElse(tuple,false)){
      tuple = (rng.nextInt(numUsers),rng.nextInt(numProducts))
    }
    observed += (tuple -> true)

    val rating = if (implicitPrefs) rng.nextInt(2)*1.0 else rng.nextDouble()*5

    new Rating(tuple._1, tuple._2, rating.toFloat)
  }

  override def setSeed(seed: Long) {
    rng.setSeed(seed)
  }

  override def copy(): RatingGenerator =
    new RatingGenerator(numUsers, numProducts, implicitPrefs)
}
