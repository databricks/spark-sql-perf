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

import java.net.InetAddress

import scala.util.Try

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

case class RunConfig(
    benchmarkName: String = null,
    filter: Option[String] = None,
    iterations: Int = 3,
    baseline: Option[Long] = None)

/**
 * Runs a benchmark locally and prints the results to the screen.
 */
object RunBenchmark {
  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[RunConfig]("spark-sql-perf") {
      head("spark-sql-perf", "0.2.0")
      opt[String]('b', "benchmark")
        .action { (x, c) => c.copy(benchmarkName = x) }
        .text("the name of the benchmark to run")
        .required()
      opt[String]('f', "filter")
        .action((x, c) => c.copy(filter = Some(x)))
        .text("a filter on the name of the queries to run")
      opt[Int]('i', "iterations")
        .action((x, c) => c.copy(iterations = x))
        .text("the number of iterations to run")
      opt[Long]('c', "compare")
          .action((x, c) => c.copy(baseline = Some(x)))
          .text("the timestamp of the baseline experiment to compare with")
      help("help")
        .text("prints this usage text")
    }

    parser.parse(args, RunConfig()) match {
      case Some(config) =>
        run(config)
      case None =>
        System.exit(1)
    }
  }

  def run(config: RunConfig): Unit = {
    val conf = new SparkConf()
        .setMaster("local[*]")
        .setAppName(getClass.getName)

    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = SQLContext.getOrCreate(sc)

    sqlContext.setConf("spark.sql.perf.results", new java.io.File("performance").toURI.toString)
    val benchmark = Try {
      Class.forName(config.benchmarkName)
          .newInstance()
          .asInstanceOf[Benchmark]
    } getOrElse {
      Class.forName("com.databricks.spark.sql.perf." + config.benchmarkName)
          .newInstance()
          .asInstanceOf[Benchmark]
    }

    val allQueries = config.filter.map { f =>
      benchmark.allQueries.filter(_.name contains f)
    } getOrElse {
      benchmark.allQueries
    }

    println("== QUERY LIST ==")
    allQueries.foreach(println)

    val experiment = benchmark.runExperiment(
      executionsToRun = allQueries,
      iterations = config.iterations,
      tags = Map(
        "runtype" -> "local",
        "host" -> InetAddress.getLocalHost().getHostName()))

    println("== STARTING EXPERIMENT ==")
    experiment.waitForFinish(1000 * 60 * 30)

    presentFindings(sqlContext, experiment)

    if (config.baseline.isDefined) {
      compareResults(
        sqlContext,
        config.baseline.get,
        experiment.timestamp,
        benchmark.resultsLocation,
        experiment.comparisonResultPath)
    }
  }

  def presentFindings(
      sqlContext: SQLContext,
      experiment: Benchmark.ExperimentStatus): Unit = {

    import sqlContext.implicits._
    sqlContext.setConf("spark.sql.shuffle.partitions", "1")
    val runResults = experiment.getCurrentRuns()
        .withColumn("result", explode($"results"))
        .select("result.*")
        .groupBy("name")
        .agg(
          min($"parsingTime") as 'minParsingTimeMs,
          max($"parsingTime") as 'maxParsingTimeMs,
          avg($"parsingTime") as 'avgParsingTimeMs,
          stddev($"parsingTime") as 'stdParsingDev,

          min($"analysisTime") as 'minAnalysisTimeMs,
          max($"analysisTime") as 'maxAnalysisTimeMs,
          avg($"analysisTime") as 'avgAnalysisTimeMs,
          stddev($"analysisTime") as 'stdAnalysisDev,

          min($"optimizationTime") as 'minOptimizationTimeMs,
          max($"optimizationTime") as 'maxOptimizationTimeMs,
          avg($"optimizationTime") as 'avgOptimizationTimeMs,
          stddev($"optimizationTime") as 'stdOptimizationDev,

          min($"planningTime") as 'minPlanningTimeMs,
          max($"planningTime") as 'maxPlanningTimeMs,
          avg($"planningTime") as 'avgPlanningTimeMs,
          stddev($"planningTime") as 'stdPlanningDev,

          min($"executionTime") as 'minExecutionTimeMs,
          max($"executionTime") as 'maxExecutionTimeMs,
          avg($"executionTime") as 'avgExecutionTimeMs,
          stddev($"executionTime") as 'stdExecutionDev)
        .orderBy("name")

    runResults.show(runResults.count.toInt, truncate = false)

    runResults
      .coalesce(1)
      .write
      .option("header", "true")
      .csv(experiment.csvResultPath)

    println(s"""CSV Results: ${experiment.csvResultPath}""")
  }

  def compareResults(
      sqlContext: SQLContext,
      baselineTimestamp: Long,
      targetTimestamp: Long,
      resultsLocation: String,
      outputLocation: String): Unit = {

    import sqlContext.implicits._
    val baselineTime = when($"timestamp" === baselineTimestamp, $"executionTime").otherwise(null)
    val targetTime = when($"timestamp" === targetTimestamp, $"executionTime").otherwise(null)

    val comparison = sqlContext.read.json(resultsLocation)
        .where(s"timestamp IN ($baselineTimestamp, $targetTimestamp)")
        .withColumn("result", explode($"results"))
        .select("timestamp", "result.*")
        .groupBy("name")
        .agg(
          avg(baselineTime) as 'baselineTimeMs,
          avg(targetTime) as 'targetTimeMs,
          stddev(baselineTime) as 'stddev)
        .withColumn(
          "percentChange", ($"baselineTimeMs" - $"targetTimeMs") / $"baselineTimeMs" * 100)
        .filter('targetTimeMs.isNotNull)

    comparison.show(comparison.count.toInt, truncate = false)

    val comparisonPath =
      s"$outputLocation/baselineTs=${baselineTimestamp}_targetTs=$targetTimestamp"

    comparison
      .coalesce(1)
      .write
      .option("header", "true")
      .csv(comparisonPath)

    println(s"""Comparison results: $comparisonPath""")
  }
}