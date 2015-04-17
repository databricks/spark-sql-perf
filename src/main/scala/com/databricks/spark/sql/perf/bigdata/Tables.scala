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

// This is a hack until parquet has better support for partitioning.

import java.text.SimpleDateFormat
import java.util.Date

import com.databricks.spark.sql.perf._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat => NewFileOutputFormat}
import org.apache.hadoop.mapreduce.{Job, OutputCommitter, RecordWriter, TaskAttemptContext}
import org.apache.spark.SerializableWritable
import org.apache.spark.mapreduce.SparkHadoopMapReduceUtil
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Column, ColumnName, SQLContext}
import parquet.hadoop.ParquetOutputFormat
import parquet.hadoop.util.ContextUtil

import scala.sys.process._

case class BigDataTableForTest(
    table: Table,
    baseDir: String,
    scaleFactor: String,
    @transient sqlContext: SQLContext)
  extends TableForTest(table, baseDir, sqlContext) with Serializable {

  @transient val sparkContext = sqlContext.sparkContext

  override def generate(): Unit =
    throw new UnsupportedOperationException(
      "Generate data for BigDataBenchmark has not been implemented")
}

case class Tables(sqlContext: SQLContext) {
  import sqlContext.implicits._

  val tables = Seq(
    Table("rankings",
      UnpartitionedTable,
      'pageURL     .string,
      'pageRank    .int,
      'avgDuration .int),

    Table("uservisits",
      UnpartitionedTable,
      'sourceIP     .string,
      'destURL      .string,
      'visitDate    .string,
      'adRevenue    .double,
      'userAgent    .string,
      'countryCode  .string,
      'languageCode .string,
      'searchWord   .string,
      'duration     .int),

    Table("documents",
      UnpartitionedTable,
      'line     .string)
  )
}
