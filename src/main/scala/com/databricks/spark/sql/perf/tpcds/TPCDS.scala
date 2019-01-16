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

package com.databricks.spark.sql.perf.tpcds

import scala.collection.mutable
import com.databricks.spark.sql.perf._
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
 * TPC-DS benchmark's dataset.
 *
 * @param sqlContext An existing SQLContext.
 */
class TPCDS(@transient sqlContext: SQLContext)
  extends Benchmark(sqlContext)
  with ImpalaKitQueries
  with SimpleQueries
  with Tpcds_1_4_Queries
  with Tpcds_2_4_Queries
  with Serializable {

  def this() = this(SparkSession.builder.getOrCreate().sqlContext)

  /*
  def setupBroadcast(skipTables: Seq[String] = Seq("store_sales", "customer")) = {
    val skipExpr = skipTables.map(t => !('tableName === t)).reduceLeft[Column](_ && _)
    val threshold =
      allStats
        .where(skipExpr)
        .select(max('sizeInBytes))
        .first()
        .getLong(0)
    val setQuery = s"SET spark.sql.autoBroadcastJoinThreshold=$threshold"

    println(setQuery)
    sql(setQuery)
  }
  */

  /**
   * Simple utilities to run the queries without persisting the results.
   */
  def explain(queries: Seq[Query], showPlan: Boolean = false): Unit = {
    val succeeded = mutable.ArrayBuffer.empty[String]
    queries.foreach { q =>
      println(s"Query: ${q.name}")
      try {
        val df = sqlContext.sql(q.sqlText.get)
        if (showPlan) {
          df.explain()
        } else {
          df.queryExecution.executedPlan
        }
        succeeded += q.name
      } catch {
        case e: Exception =>
          println("Failed to plan: " + e)
      }
    }
    println(s"Planned ${succeeded.size} out of ${queries.size}")
    println(succeeded.map("\"" + _ + "\""))
  }

  def run(queries: Seq[Query], numRows: Int = 1, timeout: Int = 0): Unit = {
    val succeeded = mutable.ArrayBuffer.empty[String]
    queries.foreach { q =>
      println(s"Query: ${q.name}")
      val start = System.currentTimeMillis()
      val df = sqlContext.sql(q.sqlText.get)
      var failed = false
      val jobgroup = s"benchmark ${q.name}"
      val t = new Thread("query runner") {
        override def run(): Unit = {
          try {
            sqlContext.sparkContext.setJobGroup(jobgroup, jobgroup, true)
            df.show(numRows)
          } catch {
            case e: Exception =>
              println("Failed to run: " + e)
              failed = true
          }
        }
      }
      t.setDaemon(true)
      t.start()
      t.join(timeout)
      if (t.isAlive) {
        println(s"Timeout after $timeout seconds")
        sqlContext.sparkContext.cancelJobGroup(jobgroup)
        t.interrupt()
      } else {
        if (!failed) {
          succeeded += q.name
          println(s"   Took: ${System.currentTimeMillis() - start} ms")
          println("------------------------------------------------------------------")
        }
      }
    }
    println(s"Ran ${succeeded.size} out of ${queries.size}")
    println(succeeded.map("\"" + _ + "\""))
  }
}



