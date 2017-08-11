package com.databricks.spark.sql.perf.mllib

import scala.language.implicitConversions
import com.databricks.spark.sql.perf._
import com.typesafe.scalalogging.slf4j.{LazyLogging => Logging}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}


class MLLib(@transient sqlContext: SQLContext)
  extends Benchmark(sqlContext) with Serializable {

  def this() = this(SQLContext.getOrCreate(SparkContext.getOrCreate()))
}

object MLLib extends Logging {

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


  def main(args: Array[String]): Unit = {
    val configFile = args(0)
    run(yamlFile = configFile)
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

    // Uncomment these lines to run locally:
    // val sparkConf = new SparkConf().setAppName("MLlib QA").setMaster("local[2]")
    // val sc = new SparkContext(sparkConf)
    val sc = SparkContext.getOrCreate()
    val b = new com.databricks.spark.sql.perf.mllib.MLLib()
    val sqlContext = com.databricks.spark.sql.perf.mllib.MLBenchmarks.sqlContext
    val benchmarksDescriptions = conf.runnableBenchmarks
    val benchmarks = benchmarksDescriptions.map { mlb =>
      new MLTransformerBenchmarkable(mlb.params, mlb.benchmark, sqlContext)
    }
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
