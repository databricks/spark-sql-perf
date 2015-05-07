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

/**
 * TPC-DS benchmark's dataset.
 * @param sqlContext An existing SQLContext.
 * @param sparkVersion The version of Spark.
 * @param dataLocation The location of the dataset used by this experiment.
 * @param dsdgenDir The location of dsdgen in every worker machine.
 * @param resultsLocation The location of performance results.
 * @param tables Tables that will be used in this experiment.
 * @param scaleFactor The scale factor of the dataset. For some benchmarks like TPC-H
 *                    and TPC-DS, the scale factor is a number roughly representing the
 *                    size of raw data files. For some other benchmarks, the scale factor
 *                    is a short string describing the scale of the dataset.
 */
class TPCDS (
    @transient sqlContext: SQLContext,
    sparkVersion: String,
    dataLocation: String,
    dsdgenDir: String,
    tables: Seq[Table],
    scaleFactor: String,
    userSpecifiedBaseDir: Option[String] = None)
  extends Dataset(
    sqlContext,
    sparkVersion,
    dataLocation,
    tables,
    scaleFactor) with Serializable {
  import sqlContext._
  import sqlContext.implicits._

  override val datasetName = "tpcds"

  lazy val baseDir =
    userSpecifiedBaseDir.getOrElse(s"$dataLocation/scaleFactor=$scaleFactor/useDecimal=true")

  override def createTablesForTest(tables: Seq[Table]): Seq[TableForTest] = {
    tables.map(table =>
      TPCDSTableForTest(table, baseDir, scaleFactor.toInt, dsdgenDir, sqlContext))
  }

  override def setup(): Unit = {
    super.setup()
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

