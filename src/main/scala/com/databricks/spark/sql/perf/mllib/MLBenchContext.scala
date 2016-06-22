package com.databricks.spark.sql.perf.mllib

import java.util.Random

import com.databricks.spark.sql.perf.{MLParams}
import org.apache.spark.sql.SQLContext


/**
 * All the information required to run a test.
 *
 * @param params
 * @param sqlContext
 */
case class MLBenchContext(
    params: MLParams,
    sqlContext: SQLContext) {

  // Some seed fixed for the context.
  private val internalSeed: Long  = {
    params.randomSeed.map(_.toLong).getOrElse {
      throw new Exception("You need te specify the random seed")
    }
  }

  /**
   * A fixed seed for this class. This function will always return the same value.
   *
   * @return
   */
  def seed(): Long = internalSeed

  /**
   * Creates a new generator. The generator will always start with the same state.
   *
   * @return
   */
  def newGenerator(): Random = new Random(seed())
}
