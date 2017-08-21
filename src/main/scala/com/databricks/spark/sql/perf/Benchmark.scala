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

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._
import scala.language.implicitConversions
import scala.util.{Success, Try, Failure => SFailure}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.SparkContext

import com.databricks.spark.sql.perf.cpu._

/**
 * A collection of queries that test a particular aspect of Spark SQL.
 *
 * @param sqlContext An existing SQLContext.
 */
abstract class Benchmark(
    @transient val sqlContext: SQLContext)
  extends Serializable {

  import Benchmark._

  def this() = this(SQLContext.getOrCreate(SparkContext.getOrCreate()))

  val resultsLocation =
    sqlContext.getAllConfs.getOrElse(
      "spark.sql.perf.results",
      "/spark/sql/performance")

  protected def sparkContext = sqlContext.sparkContext

  protected implicit def toOption[A](a: A): Option[A] = Option(a)

  val buildInfo = Try(getClass.getClassLoader.loadClass("org.apache.spark.BuildInfo")).map { cls =>
    cls.getMethods
      .filter(_.getReturnType == classOf[String])
        .filterNot(_.getName == "toString")
        .map(m => m.getName -> m.invoke(cls).asInstanceOf[String])
        .toMap
  }.getOrElse(Map.empty)

  def currentConfiguration = BenchmarkConfiguration(
    sqlConf = sqlContext.getAllConfs,
    sparkConf = sparkContext.getConf.getAll.toMap,
    defaultParallelism = sparkContext.defaultParallelism,
    buildInfo = buildInfo)


  val codegen = Variation("codegen", Seq("on", "off")) {
    case "off" => sqlContext.setConf("spark.sql.codegen", "false")
    case "on" => sqlContext.setConf("spark.sql.codegen", "true")
  }

  val unsafe = Variation("unsafe", Seq("on", "off")) {
    case "off" => sqlContext.setConf("spark.sql.unsafe.enabled", "false")
    case "on" => sqlContext.setConf("spark.sql.unsafe.enabled", "true")
  }

  val tungsten = Variation("tungsten", Seq("on", "off")) {
    case "off" => sqlContext.setConf("spark.sql.tungsten.enabled", "false")
    case "on" => sqlContext.setConf("spark.sql.tungsten.enabled", "true")
  }

  /**
   * Starts an experiment run with a given set of executions to run.
   *
   * @param executionsToRun a list of executions to run.
   * @param includeBreakdown If it is true, breakdown results of an execution will be recorded.
   *                         Setting it to true may significantly increase the time used to
   *                         run an execution.
   * @param iterations The number of iterations to run of each execution.
   * @param variations [[Variation]]s used in this run.  The cross product of all variations will be
   *                   run for each execution * iteration.
   * @param tags Tags of this run.
   * @param timeout wait at most timeout milliseconds for each query, 0 means wait forever
   * @return It returns a ExperimentStatus object that can be used to
   *         track the progress of this experiment run.
   */
  def runExperiment(
      executionsToRun: Seq[Benchmarkable],
      includeBreakdown: Boolean = false,
      iterations: Int = 3,
      variations: Seq[Variation[_]] = Seq(Variation("StandardRun", Seq("true")) { _ => {} }),
      tags: Map[String, String] = Map.empty,
      timeout: Long = 0L,
      resultLocation: String = resultsLocation,
      forkThread: Boolean = true) = {

    new ExperimentStatus(executionsToRun, includeBreakdown, iterations, variations, tags,
      timeout, resultLocation, sqlContext, allTables, currentConfiguration, forkThread = forkThread)
  }


  import reflect.runtime._, universe._
  import reflect.runtime._
  import universe._

  @transient
  private val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)

  @transient
  val myType = runtimeMirror.classSymbol(getClass).toType

  def singleTables =
    myType.declarations
      .filter(m => m.isMethod)
      .map(_.asMethod)
      .filter(_.asMethod.returnType =:= typeOf[Table])
      .map(method => runtimeMirror.reflect(this).reflectMethod(method).apply().asInstanceOf[Table])

  def groupedTables =
    myType.declarations
      .filter(m => m.isMethod)
      .map(_.asMethod)
      .filter(_.asMethod.returnType =:= typeOf[Seq[Table]])
      .flatMap(method => runtimeMirror.reflect(this).reflectMethod(method).apply().asInstanceOf[Seq[Table]])

  @transient
  lazy val allTables: Seq[Table] = (singleTables ++ groupedTables).toSeq

  def singleQueries =
    myType.declarations
      .filter(m => m.isMethod)
      .map(_.asMethod)
      .filter(_.asMethod.returnType =:= typeOf[Benchmarkable])
      .map(method => runtimeMirror.reflect(this).reflectMethod(method).apply().asInstanceOf[Benchmarkable])

  def groupedQueries =
    myType.declarations
      .filter(m => m.isMethod)
      .map(_.asMethod)
      .filter(_.asMethod.returnType =:= typeOf[Seq[Benchmarkable]])
      .flatMap(method => runtimeMirror.reflect(this).reflectMethod(method).apply().asInstanceOf[Seq[Benchmarkable]])

  @transient
  lazy val allQueries = (singleQueries ++ groupedQueries).toSeq

  def html: String = {
    val singleQueries =
      myType.declarations
        .filter(m => m.isMethod)
        .map(_.asMethod)
        .filter(_.asMethod.returnType =:= typeOf[Query])
        .map(method => runtimeMirror.reflect(this).reflectMethod(method).apply().asInstanceOf[Query])
        .mkString(",")
    val queries =
      myType.declarations
      .filter(m => m.isMethod)
      .map(_.asMethod)
      .filter(_.asMethod.returnType =:= typeOf[Seq[Query]])
      .map { method =>
        val queries = runtimeMirror.reflect(this).reflectMethod(method).apply().asInstanceOf[Seq[Query]]
        val queryList = queries.map(_.name).mkString(", ")
        s"""
          |<h3>${method.name}</h3>
          |<ul>$queryList</ul>
        """.stripMargin
    }.mkString("\n")

    s"""
       |<h1>Spark SQL Performance Benchmarking</h1>
       |<h2>Available Queries</h2>
       |$singleQueries
       |$queries
     """.stripMargin
  }

  /** Factory object for benchmark queries. */
  case object Query {
    def apply(
        name: String,
        sqlText: String,
        description: String,
        executionMode: ExecutionMode = ExecutionMode.ForeachResults): Query = {
      new Query(name, sqlContext.sql(sqlText), description, Some(sqlText), executionMode)
    }

    def apply(
        name: String,
        dataFrameBuilder: => DataFrame,
        description: String): Query = {
      new Query(name, dataFrameBuilder, description, None, ExecutionMode.CollectResults)
    }
  }

  object RDDCount {
    def apply(
        name: String,
        rdd: RDD[_]) = {
      new SparkPerfExecution(
        name,
        Map.empty,
        () => Unit,
        () => rdd.count(),
        rdd.toDebugString)
    }
  }

  /** A class for benchmarking Spark perf results. */
  class SparkPerfExecution(
      override val name: String,
      parameters: Map[String, String],
      prepare: () => Unit,
      run: () => Unit,
      description: String = "")
    extends Benchmarkable {

    override def toString: String =
      s"""
         |== $name ==
         |$description
       """.stripMargin

    protected override val executionMode: ExecutionMode = ExecutionMode.SparkPerfResults

    protected override def beforeBenchmark(): Unit = { prepare() }

    protected override def doBenchmark(
        includeBreakdown: Boolean,
        description: String = "",
        messages: ArrayBuffer[String]): BenchmarkResult = {
      try {
        val timeMs = measureTimeMs(run())
        BenchmarkResult(
          name = name,
          mode = executionMode.toString,
          parameters = parameters,
          executionTime = Some(timeMs))
      } catch {
        case e: Exception =>
          BenchmarkResult(
            name = name,
            mode = executionMode.toString,
            parameters = parameters,
            failure = Some(Failure(e.getClass.getSimpleName, e.getMessage)))
      }
    }
  }
}

/**
 * A Variation represents a setting (e.g. the number of shuffle partitions or if tables
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

case class Table(
    name: String,
    data: Dataset[_])


object Benchmark {

  class ExperimentStatus(
      executionsToRun: Seq[Benchmarkable],
      includeBreakdown: Boolean,
      iterations: Int,
      variations: Seq[Variation[_]],
      tags: Map[String, String],
      timeout: Long,
      resultsLocation: String,
      sqlContext: SQLContext,
      allTables: Seq[Table],
      currentConfiguration: BenchmarkConfiguration,
      forkThread: Boolean = true) {
    val currentResults = new collection.mutable.ArrayBuffer[BenchmarkResult]()
    val currentRuns = new collection.mutable.ArrayBuffer[ExperimentRun]()
    val currentMessages = new collection.mutable.ArrayBuffer[String]()

    def logMessage(msg: String) = {
      println(msg)
      currentMessages += msg
    }

    // Stats for HTML status message.
    @volatile var currentExecution = ""
    @volatile var currentPlan = "" // for queries only
    @volatile var currentConfig = ""
    @volatile var failures = 0
    @volatile var startTime = 0L

    /** An optional log collection task that will run after the experiment. */
    @volatile var logCollection: () => Unit = () => {}


    def cartesianProduct[T](xss: List[List[T]]): List[List[T]] = xss match {
      case Nil => List(Nil)
      case h :: t => for(xh <- h; xt <- cartesianProduct(t)) yield xh :: xt
    }

    val timestamp = System.currentTimeMillis()
    val resultPath = s"$resultsLocation/timestamp=$timestamp"
    val combinations = cartesianProduct(variations.map(l => (0 until l.options.size).toList).toList)
    val resultsFuture = Future {

      // If we're running queries, create tables for them
      executionsToRun
        .collect { case query: Query => query }
        .flatMap { query =>
          try {
            query.newDataFrame().queryExecution.logical.collect {
              case UnresolvedRelation(t) => t.table
            }
          } catch {
            // ignore the queries that can't be parsed
            case e: Exception => Seq()
          }
        }
        .distinct
        .foreach { name =>
          try {
            sqlContext.table(name)
            logMessage(s"Table $name exists.")
          } catch {
            case ae: Exception =>
              val table = allTables
                .find(_.name == name)
              if (table.isDefined) {
                logMessage(s"Creating table: $name")
                table.get.data
                  .write
                  .mode("overwrite")
                  .saveAsTable(name)
              } else {
                // the table could be subquery
                logMessage(s"Couldn't read table $name and its not defined as a Benchmark.Table.")
              }
          }
        }

      // Run the benchmarks!
      val results: Seq[ExperimentRun] = (1 to iterations).flatMap { i =>
        combinations.map { setup =>
          val currentOptions = variations.asInstanceOf[Seq[Variation[Any]]].zip(setup).map {
            case (v, idx) =>
              v.setup(v.options(idx))
              v.name -> v.options(idx).toString
          }
          currentConfig = currentOptions.map { case (k,v) => s"$k: $v" }.mkString(", ")

          val res = executionsToRun.flatMap { q =>
            val setup = s"iteration: $i, ${currentOptions.map { case (k, v) => s"$k=$v"}.mkString(", ")}"
            logMessage(s"Running execution ${q.name} $setup")

            currentExecution = q.name
            currentPlan = q match {
              case query: Query =>
                try {
                  query.newDataFrame().queryExecution.executedPlan.toString()
                } catch {
                  case e: Exception =>
                    s"failed to parse: $e"
                }
              case _ => ""
            }
            startTime = System.currentTimeMillis()

            val singleResultT = Try {
              q.benchmark(includeBreakdown, setup, currentMessages, timeout,
                forkThread=forkThread)
            }

            singleResultT match {
              case Success(singleResult) =>
                singleResult.failure.foreach { f =>
                  failures += 1
                  logMessage(s"Execution '${q.name}' failed: ${f.message}")
                }
                singleResult.executionTime.foreach { time =>
                  logMessage(s"Execution time: ${time / 1000}s")
                }
                currentResults += singleResult
                singleResult :: Nil
              case SFailure(e) =>
                failures += 1
                logMessage(s"Execution '${q.name}' failed: ${e}")
                Nil
            }
          }

          val result = ExperimentRun(
            timestamp = timestamp,
            iteration = i,
            tags = currentOptions.toMap ++ tags,
            configuration = currentConfiguration,
            res)

          currentRuns += result

          result
        }
      }

      try {
        val resultsTable = sqlContext.createDataFrame(results)
        logMessage(s"Results written to table: 'sqlPerformance' at $resultPath")
        resultsTable
          .coalesce(1)
          .write
          .format("json")
          .save(resultPath)
      } catch {
        case e: Throwable => logMessage(s"Failed to write data: $e")
      }

      logCollection()
    }

    def scheduleCpuCollection(fs: FS) = {
      logCollection = () => {
        logMessage(s"Begining CPU log collection")
        try {
          val location = cpu.collectLogs(sqlContext, fs, timestamp)
          logMessage(s"cpu results recorded to $location")
        } catch {
          case e: Throwable =>
            logMessage(s"Error collecting logs: $e")
            throw e
        }
      }
    }

    def cpuProfile = new Profile(sqlContext, sqlContext.read.json(getCpuLocation(timestamp)))

    def cpuProfileHtml(fs: FS) = {
      s"""
         |<h1>CPU Profile</h1>
         |<b>Permalink:</b> <tt>sqlContext.read.json("${getCpuLocation(timestamp)}")</tt></br>
         |${cpuProfile.buildGraph(fs)}
         """.stripMargin
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

    def tail(n: Int = 20) = {
      currentMessages.takeRight(n).mkString("\n")
    }

    def status =
      if (resultsFuture.isCompleted) {
        if (resultsFuture.value.get.isFailure) "Failed" else "Successful"
      } else {
        "Running"
      }

    override def toString =
      s"""Permalink: table("sqlPerformance").where('timestamp === ${timestamp}L)"""


    def html: String = {
      val maybeQueryPlan: String =
        if (currentPlan.nonEmpty) {
          s"""
             |<h3>QueryPlan</h3>
             |<pre>
             |${currentPlan.replaceAll("\n", "<br/>")}
             |</pre>
            """.stripMargin
        } else {
          ""
        }
      s"""
         |<h2>$status Experiment</h2>
         |<b>Permalink:</b> <tt>sqlContext.read.json("$resultPath")</tt><br/>
         |<b>Iterations complete:</b> ${currentRuns.size / combinations.size} / $iterations<br/>
         |<b>Failures:</b> $failures<br/>
         |<b>Executions run:</b> ${currentResults.size} / ${iterations * combinations.size * executionsToRun.size}
         |<br/>
         |<b>Run time:</b> ${(System.currentTimeMillis() - timestamp) / 1000}s<br/>
         |
           |<h2>Current Execution: $currentExecution</h2>
         |Runtime: ${(System.currentTimeMillis() - startTime) / 1000}s<br/>
         |$currentConfig<br/>
         |$maybeQueryPlan
         |<h2>Logs</h2>
         |<pre>
         |${tail()}
         |</pre>
         """.stripMargin
    }
  }
}
