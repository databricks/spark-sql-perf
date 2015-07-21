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

import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.sql.SQLContext

/**
 * The configuration used for an iteration of an experiment.
 * @param sparkVersion The version of Spark.
 * @param scaleFactor The scale factor of the dataset.
 * @param sqlConf All configuration properties related to Spark SQL.
 * @param sparkConf All configuration properties of Spark.
 * @param defaultParallelism The default parallelism of the cluster.
 *                           Usually, it is the number of cores of the cluster.
 */
case class BenchmarkConfiguration(
    sparkVersion: String,
    scaleFactor: String,
    sqlConf: Map[String, String],
    sparkConf: Map[String,String],
    defaultParallelism: Int)

/**
 * The execution time of a subtree of the query plan tree of a specific query.
 * @param nodeName The name of the top physical operator of the subtree.
 * @param nodeNameWithArgs The name and arguments of the top physical operator of the subtree.
 * @param index The index of the top physical operator of the subtree
 *              in the original query plan tree. The index starts from 0
 *              (0 represents the top physical operator of the original query plan tree).
 * @param executionTime The execution time of the subtree.
 */
case class BreakdownResult(
    nodeName: String,
    nodeNameWithArgs: String,
    index: Int,
    executionTime: Double)

/**
 * The result of a query.
 * @param name The name of the query.
 * @param joinTypes The type of join operations in the query.
 * @param tables The tables involved in the query.
 * @param parsingTime The time used to parse the query.
 * @param analysisTime The time used to analyze the query.
 * @param optimizationTime The time used to optimize the query.
 * @param planningTime The time used to plan the query.
 * @param executionTime The time used to execute the query.
 * @param breakDown The breakdown results of the query plan tree.
 */
case class BenchmarkResult(
    name: String,
    joinTypes: Seq[String],
    tables: Seq[String],
    parsingTime: Double,
    analysisTime: Double,
    optimizationTime: Double,
    planningTime: Double,
    executionTime: Double,
    breakDown: Seq[BreakdownResult])

/**
 * A Variation represents a setting (e.g. the number of shuffle partitions and if tables
 * are cached in memory) that we want to change in a experiment run.
 * A Variation has three parts, `name`, `options`, and `setup`.
 * The `name` is the identifier of a Variation. `options` is a Seq of options that
 * will be used for a query. Basically, a query will be executed with every option
 * defined in the list of `options`. `setup` defines the needed action for every
 * option. For example, the following Variation is used to change the number of shuffle
 * partitions of a query. The name of the Variation is "shufflePartitions". There are
 * two options, 200 and 2000. The setup is used to set the value of property
 * "spark.sql.shuffle.partitions".
 *
 * {{{
 *   Variation("shufflePartitions", Seq("200", "2000")) {
 *     case num => sqlContext.setConf("spark.sql.shuffle.partitions", num)
 *   }
 * }}}
 */
case class Variation[T](name: String, options: Seq[T])(val setup: T => Unit)

/**
 * The performance results of all given queries for a single iteration.
 * @param timestamp The timestamp indicates when the entire experiment is started.
 * @param datasetName The name of dataset.
 * @param iteration The index number of the current iteration.
 * @param tags Tags of this iteration (variations are stored at here).
 * @param configuration Configuration properties of this iteration.
 * @param results The performance results of queries for this iteration.
 */
case class ExperimentRun(
    timestamp: Long,
    datasetName: String,
    iteration: Int,
    tags: Map[String, String],
    configuration: BenchmarkConfiguration,
    results: Seq[BenchmarkResult])

/**
 * The dataset of a benchmark.
 * @param sqlContext An existing SQLContext.
 * @param sparkVersion The version of Spark.
 * @param dataLocation The location of the dataset used by this experiment.
 * @param tables Tables that will be used in this experiment.
 * @param scaleFactor The scale factor of the dataset. For some benchmarks like TPC-H
 *                    and TPC-DS, the scale factor is a number roughly representing the
 *                    size of raw data files. For some other benchmarks, the scale factor
 *                    is a short string describing the scale of the dataset.
 */
abstract class Dataset(
    @transient sqlContext: SQLContext,
    sparkVersion: String,
    dataLocation: String,
    tables: Seq[Table],
    scaleFactor: String) extends Serializable {

  val datasetName: String

  @transient val sparkContext = sqlContext.sparkContext

  def createTablesForTest(tables: Seq[Table]): Seq[TableForTest]

  val tablesForTest: Seq[TableForTest] = createTablesForTest(tables)

  def checkData(): Unit = {
    tablesForTest.foreach { table =>
      val fs = FileSystem.get(new java.net.URI(table.outputDir), new Configuration())
      val exists = fs.exists(new Path(table.outputDir))
      val wasSuccessful = fs.exists(new Path(s"${table.outputDir}/_SUCCESS"))

      if (!wasSuccessful) {
        if (exists) {
          println(s"Table '${table.name}' not generated successfully, regenerating.")
        } else {
          println(s"Table '${table.name}' does not exist, generating.")
        }
        fs.delete(new Path(table.outputDir), true)
        table.generate()
      } else {
        println(s"Table ${table.name} already exists.")
      }
    }
  }

  def allStats = tablesForTest.map(_.stats).reduceLeft(_.unionAll(_))

  /**
   * Does necessary setup work such as data generation and transformation. It needs to be
   * called before running any query.
   */
  def setup(): Unit = {
    checkData()
    tablesForTest.foreach(_.createTempTable())
  }

  def currentConfiguration = BenchmarkConfiguration(
    sparkVersion = sparkVersion,
    scaleFactor = scaleFactor,
    sqlConf = sqlContext.getAllConfs,
    sparkConf = sparkContext.getConf.getAll.toMap,
    defaultParallelism = sparkContext.defaultParallelism)

  /**
   * Starts an experiment run with a given set of queries.
   * @param queries Queries to be executed.
   * @param resultsLocation The location of performance results.
   * @param includeBreakdown If it is true, breakdown results of a query will be recorded.
   *                         Setting it to true may significantly increase the time used to
   *                         execute a query.
   * @param iterations The number of iterations.
   * @param variations [[Variation]]s used in this run.
   * @param tags Tags of this run.
   * @return It returns a ExperimentStatus object that can be used to
   *         track the progress of this experiment run.
   */
  def runExperiment(
      queries: Seq[Query],
      resultsLocation: String,
      includeBreakdown: Boolean = false,
      iterations: Int = 3,
      variations: Seq[Variation[_]] = Seq(Variation("StandardRun", Seq("")) { _ => {} }),
      tags: Map[String, String] = Map.empty) = {

    val queriesToRun = queries.map(query => QueryForTest(query, includeBreakdown, sqlContext))

    class ExperimentStatus {
      val currentResults = new collection.mutable.ArrayBuffer[BenchmarkResult]()
      val currentRuns = new collection.mutable.ArrayBuffer[ExperimentRun]()
      val currentMessages = new collection.mutable.ArrayBuffer[String]()

      @volatile
      var currentQuery = ""

      def cartesianProduct[T](xss: List[List[T]]): List[List[T]] = xss match {
        case Nil => List(Nil)
        case h :: t => for(xh <- h; xt <- cartesianProduct(t)) yield xh :: xt
      }

      val timestamp = System.currentTimeMillis()
      val combinations = cartesianProduct(variations.map(l => (0 until l.options.size).toList).toList)
      val resultsFuture = future {
        val results = (1 to iterations).flatMap { i =>
          combinations.map { setup =>
            val currentOptions = variations.asInstanceOf[Seq[Variation[Any]]].zip(setup).map {
              case (v, idx) =>
                v.setup(v.options(idx))
                v.name -> v.options(idx).toString
            }

            val result = ExperimentRun(
              timestamp = timestamp,
              datasetName = datasetName,
              iteration = i,
              tags = currentOptions.toMap ++ tags,
              configuration = currentConfiguration,
              queriesToRun.flatMap { q =>
                val setup = s"iteration: $i, ${currentOptions.map { case (k, v) => s"$k=$v"}.mkString(", ")}"
                currentMessages += s"Running query ${q.name} $setup"

                currentQuery = q.name
                val singleResult = try q.benchmark(setup) :: Nil catch {
                  case e: Exception =>
                    currentMessages += s"Failed to run query ${q.name}: $e"
                    Nil
                }
                currentResults ++= singleResult
                singleResult
              })
            currentRuns += result

            result
          }
        }

        val resultsTable = sqlContext.createDataFrame(results)
        currentMessages +=  s"Results stored to: $resultsLocation/$timestamp"
        resultsTable.toJSON.coalesce(1).saveAsTextFile(s"$resultsLocation/$timestamp")
        resultsTable
      }

      /** Waits for the finish of the experiment. */
      def waitForFinish(timeoutInSeconds: Int) = {
        Await.result(resultsFuture, timeoutInSeconds.seconds)
      }

      /** Returns results from an actively running experiment. */
      def getCurrentResults() = {
        val tbl = sqlContext.createDataFrame(currentResults)
        tbl.registerTempTable("currentResults")
        tbl
      }

      /** Returns full iterations from an actively running experiment. */
      def getCurrentRuns() = {
        val tbl = sqlContext.createDataFrame(currentRuns)
        tbl.registerTempTable("currentRuns")
        tbl
      }

      def tail(n: Int = 5) = {
        currentMessages.takeRight(n).mkString("\n")
      }

      def status =
        if (resultsFuture.isCompleted) {
          if (resultsFuture.value.get.isFailure) "Failed" else "Successful"
        } else {
          "Running"
        }

      override def toString =
        s"""
           |=== $status Experiment ===
           |Permalink: table("allResults").where('timestamp === ${timestamp}L)
           |Queries: ${queriesToRun.map(_.name).map(n => if(n == currentQuery) s"|$n|" else n).mkString(" ")}
           |Iterations complete: ${currentRuns.size / combinations.size} / $iterations
           |Queries run: ${currentResults.size} / ${iterations * combinations.size * queriesToRun.size}
           |Run time: ${(System.currentTimeMillis() - timestamp) / 1000}s
           |
           |== Logs ==
           |${tail()}
         """.stripMargin
    }
    new ExperimentStatus
  }
}