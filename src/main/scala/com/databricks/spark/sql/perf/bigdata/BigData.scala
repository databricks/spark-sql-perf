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

package com.databricks.spark.sql.perf.bigdata

import com.databricks.spark.sql.perf._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.parquet.TPCDSTableForTest
import org.apache.spark.sql.{Column, SQLContext}

class BigData (
    @transient sqlContext: SQLContext,
    sparkVersion: String,
    dataLocation: String,
    tables: Seq[Table],
    scaleFactor: String)
  extends Dataset(
    sqlContext,
    sparkVersion,
    dataLocation,
    tables,
    scaleFactor) with Serializable {
  import sqlContext._
  import sqlContext.implicits._

  override val datasetName = "bigDataBenchmark"

  override def createTablesForTest(tables: Seq[Table]): Seq[TableForTest] = {
    tables.map(table =>
      BigDataTableForTest(table, dataLocation, scaleFactor, sqlContext))
  }
}

