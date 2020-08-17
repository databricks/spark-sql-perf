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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.execution.SparkPlan


/** Holds one benchmark query and its metadata. */
class Query(
    override val name: String,
    buildDataFrame: => DataFrame,
    val description: String = "",
    val sqlText: Option[String] = None,
    override val executionMode: ExecutionMode = ExecutionMode.ForeachResults)
  extends Benchmarkable with Serializable {

  private implicit def toOption[A](a: A): Option[A] = Option(a)

  override def toString: String = {
    try {
      s"""
         |== Query: $name ==
         |${buildDataFrame.queryExecution.analyzed}
     """.stripMargin
    } catch {
      case e: Exception =>
        s"""
           |== Query: $name ==
           | Can't be analyzed: $e
           |
           | $description
         """.stripMargin
    }
  }

  lazy val tablesInvolved = buildDataFrame.queryExecution.logical collect {
    case r: UnresolvedRelation => r.tableName
  }

  def newDataFrame() = buildDataFrame

  protected override def doBenchmark(
      includeBreakdown: Boolean,
      description: String = "",
      messages: ArrayBuffer[String]): BenchmarkResult = {
    try {
      val dataFrame = buildDataFrame
      val queryExecution = dataFrame.queryExecution
      // We are not counting the time of ScalaReflection.convertRowToScala.
      val parsingTime = measureTimeMs {
        queryExecution.logical
      }
      val analysisTime = measureTimeMs {
        queryExecution.analyzed
      }
      val optimizationTime = measureTimeMs {
        queryExecution.optimizedPlan
      }
      val planningTime = measureTimeMs {
        queryExecution.executedPlan
      }

      val breakdownResults = if (includeBreakdown) {
        val depth = queryExecution.executedPlan.collect { case p: SparkPlan => p }.size
        val physicalOperators = (0 until depth).map(i => (i, queryExecution.executedPlan.p(i)))
        val indexMap = physicalOperators.map { case (index, op) => (op, index) }.toMap
        val timeMap = new mutable.HashMap[Int, Double]
        val maxFields = 999 // Maximum number of fields that will be converted to strings

        physicalOperators.reverse.map {
          case (index, node) =>
            messages += s"Breakdown: ${node.simpleString(maxFields)}"
            val newNode = buildDataFrame.queryExecution.executedPlan.p(index)
            val executionTime = measureTimeMs {
              newNode.execute().foreach((row: Any) => Unit)
            }
            timeMap += ((index, executionTime))

            val childIndexes = node.children.map(indexMap)
            val childTime = childIndexes.map(timeMap).sum
            messages += s"Breakdown time: $executionTime (+${executionTime - childTime})"

            BreakdownResult(
              node.nodeName,
              node.simpleString(maxFields).replaceAll("#\\d+", ""),
              index,
              childIndexes,
              executionTime,
              executionTime - childTime)
        }
      } else {
        Seq.empty[BreakdownResult]
      }

      // The executionTime for the entire query includes the time of type conversion from catalyst
      // to scala.
      // Note: queryExecution.{logical, analyzed, optimizedPlan, executedPlan} has been already
      // lazily evaluated above, so below we will count only execution time.
      var result: Option[Long] = None
      val executionTime = measureTimeMs {
        executionMode match {
          case ExecutionMode.CollectResults => dataFrame.collect()
          case ExecutionMode.ForeachResults => dataFrame.foreach { _ => ():Unit }
          case ExecutionMode.WriteParquet(location) =>
            dataFrame.write.parquet(s"$location/$name.parquet")
          case ExecutionMode.HashResults =>
            // SELECT SUM(CRC32(CONCAT_WS(", ", *))) FROM (benchmark query)
            val row =
              dataFrame
                .selectExpr(s"sum(crc32(concat_ws(',', *)))")
                .head()
            result = if (row.isNullAt(0)) None else Some(row.getLong(0))
        }
      }

      val joinTypes = dataFrame.queryExecution.executedPlan.collect {
        case k if k.nodeName contains "Join" => k.nodeName
      }

      BenchmarkResult(
        name = name,
        mode = executionMode.toString,
        joinTypes = joinTypes,
        tables = tablesInvolved,
        parsingTime = parsingTime,
        analysisTime = analysisTime,
        optimizationTime = optimizationTime,
        planningTime = planningTime,
        executionTime = executionTime,
        result = result,
        queryExecution = dataFrame.queryExecution.toString,
        breakDown = breakdownResults)
    } catch {
      case e: Exception =>
         BenchmarkResult(
           name = name,
           mode = executionMode.toString,
           failure = Failure(e.getClass.getName, e.getMessage))
    }
  }

  /** Change the ExecutionMode of this Query to HashResults, which is used to check the query result. */
  def checkResult: Query = {
    new Query(name, buildDataFrame, description, sqlText, ExecutionMode.HashResults)
  }
}
