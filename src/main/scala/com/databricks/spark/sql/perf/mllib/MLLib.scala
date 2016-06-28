package com.databricks.spark.sql.perf.mllib

import com.databricks.spark.sql.perf._
import com.typesafe.scalalogging.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.slf4j.LoggerFactory

import scala.language.implicitConversions


class MLLib(@transient sqlContext: SQLContext)
  extends Benchmark(sqlContext) with Serializable {

  def this() = this(SQLContext.getOrCreate(SparkContext.getOrCreate()))
}

object MLLib {
  val logger = Logger(LoggerFactory.getLogger("MASTER_LOGGER"))


  /**
   * Runs a set of preprogrammed experiments and blocks on completion.
   *
   * @param runConfig a configuration that is av
   * @return
   */
  def runDefault(runConfig: RunConfig): DataFrame = {
    val ml = new MLLib()
    val benchmarks = MLBenchmarks.benchmarkObjects
    val e = ml.runExperiment(
      executionsToRun = benchmarks)
    e.waitForFinish(1000 * 60 * 30)
    logger.info("Run finished")
    e.getCurrentResults()
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
      new MLTransformerBenchmarkable(mlb.params, mlb.benchmark, sqlContext)
    }
    logger.info(s"${benchmarks.size} benchmarks identified:")
    val str = benchmarks.map(_.prettyPrint).mkString("\n")
    logger.info(str)
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
