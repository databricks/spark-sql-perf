package com.databricks.spark.sql.perf.mllib

import scala.language.implicitConversions

import com.databricks.spark.sql.perf._

import com.typesafe.scalalogging.slf4j.Logging

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}


class MLLib(@transient sqlContext: SQLContext)
  extends Benchmark(sqlContext) with Serializable {

  def this() = this(SQLContext.getOrCreate(SparkContext.getOrCreate()))
}

object MLLib extends Logging {
  def runDefault(runConfig: RunConfig): MLLib = {
    val ml = new MLLib()
    val benchmarks = MLBenchmarks.benchmarkObjects
    ml.runExperiment(
      executionsToRun = benchmarks,
      resultLocation = "/test/results")
    ml
  }

  /**
   * Runs all the experiments and blocks on completion
   *
   * @param yamlFile a file name
   * @return
   */
  def run(yamlFile: String = null, yamlConfig: String = null): DataFrame = {
    logger.info("Starting run")
    val conf: YamlConfig = Option(yamlFile).map(YamlConfig.readFile).getOrElse {
      require(yamlConfig != null)
      YamlConfig.readString(yamlConfig)
    }
    val sc = SparkContext.getOrCreate()
    sc.setLogLevel("INFO")
    val b = new com.databricks.spark.sql.perf.mllib.MLLib()
    val sqlContext = com.databricks.spark.sql.perf.mllib.MLBenchmarks.sqlContext
    val benchmarksDescriptions = conf.runnableBenchmarks
    val benchmarks = benchmarksDescriptions.map { mlb =>
      new MLClassificationBenchmarkable(mlb.extra, mlb.common, mlb.benchmark, sqlContext)
    }
    logger.info(s"${benchmarks.size} benchmarks identified:")
    val str = benchmarks.map(_.prettyPrint).mkString("\n")
    logger.info(str)
    logger.info("Starting experiments")
    val e = b.runExperiment(
      executionsToRun = benchmarks,
      resultLocation = conf.output)
    e.waitForFinish(conf.timeout.toSeconds.toInt)
    logger.info("Run finished")
    e.getCurrentResults()
  }
}
