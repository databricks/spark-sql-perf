package com.databricks.spark.sql.perf

import com.typesafe.scalalogging.slf4j.{LazyLogging => Logging}
import org.apache.spark.sql.SparkSession

case class BenchmarkCompareConfig(
  baseline: Long = 0,
  target: Long = 0,
  resultsLocation: String = null,
  outputLocation: String = null)

object BenchmarkCompare extends Logging {

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[BenchmarkCompareConfig]("benchmark-compare") {
      head("benchmark-compare", "0.2.0")
      opt[Long]('b', "baseline")
        .action { (x, c) => c.copy(baseline = x) }
        .text("the timestamp of the baseline experiment to compare with")
        .required()
      opt[Long]('t', "target")
        .action { (x, c) => c.copy(target = x) }
        .text("the timestamp of the target experiment to compare with")
        .required()
      opt[String]('r', "results_location")
        .action { (x, c) => c.copy(resultsLocation = x) }
        .text("location where to read the results from")
        .required()
      opt[String]('o', "output_location")
        .action { (x, c) => c.copy(outputLocation = x) }
        .text("location where to read the results from")
        .required()
    }

    parser.parse(args, BenchmarkCompareConfig()) match {
      case Some(config) =>
        run(config)
      case None =>
        System.exit(1)
    }
  }

  def run(conf: BenchmarkCompareConfig): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(getClass.getName)
      .getOrCreate()

    RunBenchmark.compareResults(
      spark.sqlContext,
      conf.baseline,
      conf.target,
      conf.resultsLocation,
      conf.outputLocation)
  }
}