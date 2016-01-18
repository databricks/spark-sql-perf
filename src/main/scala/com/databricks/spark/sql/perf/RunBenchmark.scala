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

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkContext, SparkConf}

case class RunConfig(
    benchmarkName: String = null)

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
    import sqlContext.implicits._

    sqlContext.setConf("spark.sql.perf.results", new java.io.File("performance").toURI.toString)
    val benchmark =
      Class.forName(config.benchmarkName)
          .newInstance()
          .asInstanceOf[Benchmark]
    val allQueries = benchmark.allQueries

    println("== QUERY LIST ==")
    allQueries.foreach(println)

    val experiment = benchmark.runExperiment(
      executionsToRun = allQueries,
      iterations = 3,
      tags = Map("runtype" -> "local"))

    println("== STARTING EXPERIMENT ==")
    println(s"saving results to: ${experiment.resultPath}")
    experiment.waitForFinish(1000 * 60 * 30)
    experiment.getCurrentRuns()
        .withColumn("result", explode($"results"))
        .select("result.*")
        .groupBy("name")
        .agg(
          min($"executionTime"),
          max($"executionTime"),
          avg($"executionTime"),
          stddev($"executionTime"))
        .orderBy("name")
        .show()
  }
}