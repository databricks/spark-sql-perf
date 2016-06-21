package com.databricks.spark.sql.perf.mllib

import java.util.Random

import com.databricks.spark.sql.perf.{MLParams}
import org.apache.spark.sql.SQLContext


/**
 * All the information required to run a test.
 *
 * @param extraParams
 * @param sqlContext
 */
case class MLBenchContext(
    extraParams: MLParams,
    sqlContext: SQLContext) {

  // Some seed fixed for the context.
  private val internalSeed: Long  = {
    extraParams.randomSeed.map(_.toLong).getOrElse {
      new java.util.Random().nextLong()
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
