package com.databricks.spark.sql

import org.apache.spark.sql.SparkSession

import com.databricks.spark.sql.perf.tpcds.TPCDSTables

/**
 * Gen TPCDS data.
 * To run this:
 * {{{
 *   build/sbt "test:runMain <this class> <dsdgenDir> <scaleFactor> <location> <format>"
 * }}}
 */
object GenTPCDSData {
  def main(args: Array[String]): Unit = {
    val dsdgenDir = args(0) // location of dsdgen
    val scaleFactor = args(1)
    val location = args(2)
    val format = args(3)

    val spark = SparkSession
      .builder()
      .appName("Gen TPC-DS data")
      .master("local[*]")
      .getOrCreate()

    val tables = new TPCDSTables(spark.sqlContext,
      dsdgenDir = dsdgenDir,
      scaleFactor = scaleFactor,
      useDoubleForDecimal = false,
      useStringForDate = false)

    tables.genData(
      location = location,
      format = format,
      overwrite = true,
      partitionTables = true,
      clusterByPartitionColumns = true,
      filterOutNullPartitionValues = false,
      tableFilter = "",
      numPartitions = 100)
  }
}