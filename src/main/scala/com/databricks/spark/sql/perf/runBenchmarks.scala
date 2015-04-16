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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.sql.SQLContext

import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global

case class BenchmarkConfiguration(
    sparkVersion: String,
    scaleFactor: String,
    useDecimal: Boolean,
    sqlConf: Map[String, String],
    sparkConf: Map[String,String],
    cores: Int,
    collectResults: Boolean)

case class BenchmarkResult(
    name: String,
    joinTypes: Seq[String],
    tables: Seq[String],
    parsingTime: Double,
    analysisTime: Double,
    optimizationTime: Double,
    planningTime: Double,
    executionTime: Double)

case class Variation[T](name: String, options: Seq[T])(val setup: T => Unit)

case class ExperimentRun(
    timestamp: Long,
    experiment: String,
    iteration: Int,
    tags: Map[String, String],
    configuration: BenchmarkConfiguration,
    results: Seq[BenchmarkResult])

case class Benchmark(tables: Seq[Table])

abstract class Experiment(
    @transient sqlContext: SQLContext,
    sparkVersion: String,
    dataLocation: String,
    resultsLocation: String,
    tables: Seq[Table],
    scaleFactor: String,
    collectResults: Boolean) extends Serializable {

  val experiment: String

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

  def setupExperiment(): Unit = {
    checkData()
    tablesForTest.foreach(_.createTempTable())
  }

  def currentConfiguration = BenchmarkConfiguration(
    sparkVersion = sparkVersion,
    scaleFactor = scaleFactor,
    useDecimal = true,
    sqlConf = sqlContext.getAllConfs,
    sparkConf = sparkContext.getConf.getAll.toMap,
    cores = sparkContext.defaultMinPartitions,
    collectResults = collectResults)

  def runExperiment(
      queries: Seq[Query],
      iterations: Int = 3,
      variations: Seq[Variation[_]] = Seq(Variation("StandardRun", Seq("")) { _ => {} }),
      tags: Map[String, String] = Map.empty) = {

    val queriesToRun = queries.map(query => QueryForTest(query, collectResults, sqlContext))

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
              experiment = experiment,
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