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

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{OutputCommitter, TaskAttemptContext, RecordWriter, Job}
import org.apache.spark.SerializableWritable
import org.apache.spark.sql.{SQLContext, Column}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveMetastoreTypes
import org.apache.spark.sql.types._
import parquet.hadoop.ParquetOutputFormat
import parquet.hadoop.util.ContextUtil

abstract class TableType
case object UnpartitionedTable extends TableType
case class PartitionedTable(partitionColumn: String) extends TableType

case class Table(name: String, tableType: TableType, fields: StructField*)

abstract class TableForTest(
    table: Table,
    baseDir: String,
    @transient sqlContext: SQLContext) extends Serializable {

  val schema = StructType(table.fields)

  val name = table.name

  val outputDir = s"$baseDir/parquet/${name}"

  def fromCatalog = sqlContext.table(name)

  def stats =
    fromCatalog.select(
      lit(name) as "tableName",
      count("*") as "numRows",
      lit(fromCatalog.queryExecution.optimizedPlan.statistics.sizeInBytes.toLong) as "sizeInBytes")

  def createTempTable(): Unit = {
    sqlContext.sql(
      s"""
          |CREATE TEMPORARY TABLE ${name}
          |USING org.apache.spark.sql.parquet
          |OPTIONS (
          |  path '${outputDir}'
          |)
        """.stripMargin)
  }

  def generate(): Unit
}
