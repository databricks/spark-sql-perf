package com.databricks.spark.sql.perf.mllib


import scala.io.Source
import scala.language.implicitConversions

import org.slf4j.LoggerFactory

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import com.databricks.spark.sql.perf._


class MLLib(sqlContext: SQLContext)
  extends Benchmark(sqlContext) with Serializable {

  def this() = this(SQLContext.getOrCreate(SparkContext.getOrCreate()))
}

object MLLib {

  /**
   * Runs a set of preprogrammed experiments and blocks on completion.
   *
   * @param runConfig a configuration that is av
   * @return
   */

  lazy val logger = LoggerFactory.getLogger(this.getClass.getName)

  def runDefault(runConfig: RunConfig): DataFrame = {
    val ml = new MLLib()
    val benchmarks = MLBenchmarks.benchmarkObjects
    val e = ml.runExperiment(
      executionsToRun = benchmarks)
    e.waitForFinish(1000 * 60 * 30)
    logger.info("Run finished")
    e.getCurrentResults()
  }

  private def getConfig(resourcePath: String): String = {
    val stream = getClass.getResourceAsStream(resourcePath)
    Source.fromInputStream(stream).mkString
  }

  val smallConfig: String = getConfig("config/mllib-small.yaml")
  val largeConfig: String = getConfig("config/mllib-large.yaml")

  /**
   * Entry point for running ML tests. Expects a single command-line argument: the path to
   * a YAML config file specifying which ML tests to run and their parameters.
   * @param args command line args
   */
  def main(args: Array[String]): Unit = {
    val configFile = args(0)
    run(yamlFile = configFile)
  }

  private[mllib] def getConf(yamlFile: String = null, yamlConfig: String = null): YamlConfig = {
    Option(yamlFile).map(YamlConfig.readFile).getOrElse {
      require(yamlConfig != null)
      YamlConfig.readString(yamlConfig)
    }
  }

  private[mllib] def getBenchmarks(conf: YamlConfig): Seq[MLPipelineStageBenchmarkable] = {
    val sqlContext = com.databricks.spark.sql.perf.mllib.MLBenchmarks.sqlContext
    val benchmarksDescriptions = conf.runnableBenchmarks
    benchmarksDescriptions.map { mlb =>
      new MLPipelineStageBenchmarkable(mlb.params, mlb.benchmark, sqlContext)
    }
  }

  /**
   * Runs all the experiments and blocks on completion
   *
   * @param yamlFile a file name
   * @return
   */
  def run(yamlFile: String = null, yamlConfig: String = null): DataFrame = {
    logger.info("Starting run")
    val conf = getConf(yamlFile, yamlConfig)
    val sparkConf = new SparkConf().setAppName("MLlib QA").setMaster("local[2]")
    val sc = SparkContext.getOrCreate(sparkConf)
    sc.setLogLevel("INFO")
    val b = new com.databricks.spark.sql.perf.mllib.MLLib()
    val benchmarks = getBenchmarks(conf)
    println(s"${benchmarks.size} benchmarks identified:")
    val str = benchmarks.map(_.prettyPrint).mkString("\n")
    println(str)
    logger.info("Starting experiments")
    val e = b.runExperiment(
      executionsToRun = benchmarks,
      iterations = 1, // If you want to increase the number of iterations, add more seeds
      resultLocation = conf.output,
      forkThread = false)
    e.waitForFinish(conf.timeout.toSeconds.toInt)
    logger.info("Run finished")
    e.getCurrentResults()
  }
}
