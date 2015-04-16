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

import com.databricks.spark.sql.perf._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.parquet.TPCDSTableForTest
import org.apache.spark.sql.{Column, SQLContext}

class TPCDS (
    @transient sqlContext: SQLContext,
    sparkVersion: String,
    dataLocation: String,
    dsdgenDir: String,
    resultsLocation: String,
    tables: Seq[Table],
    scaleFactor: String,
    collectResults: Boolean)
  extends Experiment(
    sqlContext,
    sparkVersion,
    dataLocation,
    resultsLocation,
    tables,
    scaleFactor,
    collectResults) with Serializable {
  import sqlContext._
  import sqlContext.implicits._

  override val experiment = "tpcds"

  def baseDir = s"$dataLocation/scaleFactor=$scaleFactor/useDecimal=true"

  override def createTablesForTest(tables: Seq[Table]): Seq[TableForTest] = {
    tables.map(table =>
      TPCDSTableForTest(table, baseDir, scaleFactor.toInt, dsdgenDir, sqlContext))
  }

  override def setupExperiment(): Unit = {
    super.setupExperiment()
    setupBroadcast()
  }

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
}

