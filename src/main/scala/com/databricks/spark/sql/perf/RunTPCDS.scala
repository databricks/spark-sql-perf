/*
 * Copyright 2015 Databricks Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.databricks.spark.sql.perf

import com.databricks.spark.sql.perf.tpcds.{TPCDS, Tables}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}


case class RunTPCDSConfig(
    generateInput: Boolean = true,
    dsdgenDir: String = "",
    scaleFactor: Int = 1,
    inputDir: String = "",
    tableFilter: String = "",
    databaseName: String = "",
    format: String = "parquet",
    partitionTables: Boolean = false,
    clusterByPartitionColumns: Boolean = false,
    filter: Option[String] = None,
    iterations: Int = 3)

/**
  * Runs a benchmark and prints the results to the screen.
  * Configs:
  * spark.sql.perf.results
  *
  * spark.sql.perf.generate.input
  * spark.sql.perf.dsdgen.dir
  * spark.sql.perf.scale.factor
  * spark.sql.perf.input.dir
  * spark.sql.perf.table.filter
  *
  * spark.sql.database.name
  * spark.sql.perf.format
  * spark.sql.perf.partition.tables
  * spark.sql.perf.cluster.partition.columns
  *
  * spark.sql.perf.filter
  * spark.sql.perf.iterations
  */
object RunTPCDS {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(getClass.getName)

    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    val generateInput = sqlContext.getConf("spark.sql.perf.generate.input", "false").toBoolean
    val dsdgenDir = sqlContext.getConf("spark.sql.perf.dsdgen.dir")
    val scaleFactor = sqlContext.getConf("spark.sql.perf.scale.factor", "1").toInt
    val inputDir = sqlContext.getConf("spark.sql.perf.input.dir")
    val filter = sqlContext.getConf("spark.sql.perf.filter", "")
    val iterations = sqlContext.getConf("spark.sql.perf.iterations", "3").toInt
    val tableFilter = sqlContext.getConf("spark.sql.perf.table.filter", "")

    val config = RunTPCDSConfig(
      generateInput = generateInput,
      dsdgenDir = dsdgenDir,
      scaleFactor = scaleFactor,
      tableFilter = tableFilter,
      inputDir = inputDir,
      filter = Some(filter),
      iterations = iterations)

    createTable(sqlContext, config)
    run(sqlContext, config)
  }

  def createTable(sqlContext: SQLContext, config: RunTPCDSConfig): Unit = {
    val conf = sqlContext.getAllConfs
    val tables = new Tables(sqlContext, config.dsdgenDir, config.scaleFactor)
    if (config.generateInput) {
      tables.genData(
        config.inputDir, config.format, true, config.partitionTables, true, config.clusterByPartitionColumns, true,
        config.tableFilter)
    }
    if (!config.databaseName.isEmpty) {
      tables.createExternalTables(config.inputDir, config.format, config.databaseName, true)
    } else {
      tables.createTemporaryTables(config.inputDir, config.format)
    }
  }

  def run(sqlContext: SQLContext, config: RunTPCDSConfig): Unit = {
    import sqlContext.implicits._

    val benchmark = new TPCDS(sqlContext = sqlContext)

    println("== All TPCDS Queries names == ")
    benchmark.all.foreach(x => println(x.name))

    val allQueries = config.filter.map { f =>
      benchmark.all.filter(_.name matches f)
    } getOrElse {
      benchmark.all
    }

    println("== QUERY LIST ==")
    allQueries.foreach(println)

    val experiment = benchmark.runExperiment(
      executionsToRun = allQueries,
      iterations = config.iterations)

    println("== STARTING EXPERIMENT ==")
    experiment.waitForFinish(1000 * 60 * 30)

    experiment.getCurrentRuns()
      .withColumn("result", explode($"results"))
      .select("result.*")
      .groupBy("name")
      .agg(
        min($"executionTime") as 'minTimeMs,
        max($"executionTime") as 'maxTimeMs,
        avg($"executionTime") as 'avgTimeMs,
        stddev($"executionTime") as 'stdDev)
      .orderBy("name")
      .show(1000, truncate = false)
    println(s"""Results: sqlContext.read.json("${experiment.resultPath}")""")
  }
}
