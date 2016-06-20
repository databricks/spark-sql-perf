package com.databricks.spark.sql.perf.mllib

import java.util.Random

import com.databricks.spark.sql.perf.{ExtraMLTestParameters, MLTestParameters}
import org.apache.spark.sql.SQLContext


case class ClassificationContext(
    commonParams: MLTestParameters,
    extraParams: ExtraMLTestParameters,
    sqlContext: SQLContext) {

  // Some seed fixed for the context.
  private val internalSeed: Int  = {
    commonParams.randomSeed.getOrElse {
      new java.util.Random().nextInt()
    }
  }

  /**
   * A fixed seed for this class. This function will always return the same value.
   *
   * @return
   */
  def seed(): Int = internalSeed

  /**
   * Creates a new generator. The generator will always start with the same state.
   *
   * @return
   */
  def newGenerator(): Random = new Random((seed()))
}
