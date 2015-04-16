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

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation

case class Query(name: String, sqlText: String)

case class QueryForTest(
    query: Query,
    collectResults: Boolean,
    @transient sqlContext: SQLContext) {
  @transient val sparkContext = sqlContext.sparkContext

  val name = query.name

  def dataFrame = sqlContext.sql(query.sqlText)

  def joinTypes = dataFrame.queryExecution.executedPlan.collect {
    case k if k.nodeName contains "Join" => k.nodeName
  }

  val tablesInvolved = dataFrame.queryExecution.logical collect {
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

  def benchmark(description: String = "") = {
    try {
      sparkContext.setJobDescription(s"Query: ${query.name}, $description")
      val queryExecution = dataFrame.queryExecution
      // We are not counting the time of ScalaReflection.convertRowToScala.
      val execution = if (collectResults) {
        benchmarkMs { queryExecution.toRdd.map(_.copy()).collect() }
      } else {
        benchmarkMs { queryExecution.toRdd.map(_.copy()).foreach {row => Unit } }
      }

      BenchmarkResult(
        name = query.name,
        joinTypes = joinTypes,
        tables = tablesInvolved,
        parsingTime = benchmarkMs { queryExecution.logical },
        analysisTime = benchmarkMs { queryExecution.analyzed },
        optimizationTime = benchmarkMs { queryExecution.optimizedPlan },
        planningTime = benchmarkMs { queryExecution.executedPlan },
        executionTime = execution)
    } catch {
      case e: Exception =>
        throw new RuntimeException(
          s"Failed to benchmark query ${query.name}\n${dataFrame.queryExecution}", e)
    }
  }
}
