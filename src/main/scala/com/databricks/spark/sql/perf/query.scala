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

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation

object x {



trait QuerySet {
  val sqlContext: SQLContext
  def sparkContext = sqlContext.sparkContext


  object Query {
    def apply(
        name: String,
        sqlText: String,
        description: String,
        collectResults: Boolean = true): Query = {
      new Query(name, sqlContext.sql(sqlText), description, collectResults, Some(sqlText))
    }

    def apply(
        name: String,
        dataFrameBuilder: => DataFrame,
        description: String): Query = {
      new Query(name, dataFrameBuilder, description, true, None)
    }
  }

  class Query(
      val name: String,
      dataFrameBuilder: => DataFrame,
      val description: String,
      val collectResults: Boolean,
      val sqlText: Option[String]) {

    val tablesInvolved = dataFrameBuilder.queryExecution.logical collect {
      case UnresolvedRelation(tableIdentifier, _) => {
        // We are ignoring the database name.
        tableIdentifier.last
      }
    }

    def benchmarkMs[A](f: => A): Double = {
      val startTime = System.nanoTime()
      val ret = f
      val endTime = System.nanoTime()
      (endTime - startTime).toDouble / 1000000
    }

    def benchmark(includeBreakdown: Boolean, description: String = "") = {
      try {
        val dataFrame = dataFrameBuilder
        sparkContext.setJobDescription(s"Query: $name, $description")
        val queryExecution = dataFrame.queryExecution
        // We are not counting the time of ScalaReflection.convertRowToScala.
        val parsingTime = benchmarkMs {
          queryExecution.logical
        }
        val analysisTime = benchmarkMs {
          queryExecution.analyzed
        }
        val optimizationTime = benchmarkMs {
          queryExecution.optimizedPlan
        }
        val planningTime = benchmarkMs {
          queryExecution.executedPlan
        }

        val breakdownResults = if (includeBreakdown) {
          val depth = queryExecution.executedPlan.treeString.split("\n").size
          val physicalOperators = (0 until depth).map(i => (i, queryExecution.executedPlan(i)))
          physicalOperators.map {
            case (index, node) =>
              val executionTime = benchmarkMs {
                node.execute().map(_.copy()).foreach(row => Unit)
              }
              BreakdownResult(node.nodeName, node.simpleString, index, executionTime)
          }
        } else {
          Seq.empty[BreakdownResult]
        }

        // The executionTime for the entire query includes the time of type conversion from catalyst to scala.
        val executionTime = if (collectResults) {
          benchmarkMs {
            dataFrame.rdd.collect()
          }
        } else {
          benchmarkMs {
            dataFrame.rdd.foreach { row => Unit }
          }
        }

        val joinTypes = dataFrame.queryExecution.executedPlan.collect {
          case k if k.nodeName contains "Join" => k.nodeName
        }

        BenchmarkResult(
          name = name,
          joinTypes = joinTypes,
          tables = tablesInvolved,
          parsingTime = parsingTime,
          analysisTime = analysisTime,
          optimizationTime = optimizationTime,
          planningTime = planningTime,
          executionTime = executionTime,
          breakdownResults)
      } catch {
        case e: Exception =>
          throw new RuntimeException(
            s"Failed to benchmark query $name", e)
      }
    }
  }

}