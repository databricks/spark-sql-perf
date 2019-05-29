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

import java.util.concurrent.LinkedBlockingQueue

import scala.collection.immutable.Stream
import scala.sys.process._

import org.slf4j.LoggerFactory

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, SaveMode}


/**
 * Using ProcessBuilder.lineStream produces a stream, that uses
 * a LinkedBlockingQueue with a default capacity of Integer.MAX_VALUE.
 *
 * This causes OOM if the consumer cannot keep up with the producer.
 *
 * See scala.sys.process.ProcessBuilderImpl.lineStream
 */
object BlockingLineStream {
  // See scala.sys.process.Streamed
  private final class BlockingStreamed[T](
    val process:   T => Unit,
    val    done: Int => Unit,
    val  stream:  () => Stream[T]
  )

  // See scala.sys.process.Streamed
  private object BlockingStreamed {
    // scala.process.sys.Streamed uses default of Integer.MAX_VALUE,
    // which causes OOMs if the consumer cannot keep up with producer.
    val maxQueueSize = 65536

    def apply[T](nonzeroException: Boolean): BlockingStreamed[T] = {
      val q = new LinkedBlockingQueue[Either[Int, T]](maxQueueSize)

      def next(): Stream[T] = q.take match {
        case Left(0) => Stream.empty
        case Left(code) =>
          if (nonzeroException) scala.sys.error("Nonzero exit code: " + code) else Stream.empty
        case Right(s) => Stream.cons(s, next())
      }

      new BlockingStreamed((s: T) => q put Right(s), code => q put Left(code), () => next())
    }
  }

  // See scala.sys.process.ProcessImpl.Spawn
  private object Spawn {
    def apply(f: => Unit): Thread = apply(f, daemon = false)
    def apply(f: => Unit, daemon: Boolean): Thread = {
      val thread = new Thread() { override def run() = { f } }
      thread.setDaemon(daemon)
      thread.start()
      thread
    }
  }

  def apply(command: Seq[String]): Stream[String] = {
    val streamed = BlockingStreamed[String](true)
    val process = command.run(BasicIO(false, streamed.process, None))
    Spawn(streamed.done(process.exitValue()))
    streamed.stream()
  }
}

trait DataGenerator extends Serializable {
  def generate(
    sparkContext: SparkContext,
    name: String,
    partitions: Int,
    scaleFactor: String): RDD[String]
}


abstract class Tables(sqlContext: SQLContext, scaleFactor: String,
    useDoubleForDecimal: Boolean = false, useStringForDate: Boolean = false)
    extends Serializable {

  def dataGenerator: DataGenerator
  def tables: Seq[Table]

  private val log = LoggerFactory.getLogger(getClass)

  def sparkContext = sqlContext.sparkContext

  case class Table(name: String, partitionColumns: Seq[String], fields: StructField*) {
    val schema = StructType(fields)

    def nonPartitioned: Table = {
      Table(name, Nil, fields : _*)
    }

    /**
     *  If convertToSchema is true, the data from generator will be parsed into columns and
     *  converted to `schema`. Otherwise, it just outputs the raw data (as a single STRING column).
     */
    def df(convertToSchema: Boolean, numPartition: Int) = {
      val generatedData = dataGenerator.generate(sparkContext, name, numPartition, scaleFactor)
      val rows = generatedData.mapPartitions { iter =>
        iter.map { l =>
          if (convertToSchema) {
            val values = l.split("\\|", -1).dropRight(1).map { v =>
              if (v.equals("")) {
                // If the string value is an empty string, we turn it to a null
                null
              } else {
                v
              }
            }
            Row.fromSeq(values)
          } else {
            Row.fromSeq(Seq(l))
          }
        }
      }

      if (convertToSchema) {
        val stringData =
          sqlContext.createDataFrame(
            rows,
            StructType(schema.fields.map(f => StructField(f.name, StringType))))

        val convertedData = {
          val columns = schema.fields.map { f =>
            col(f.name).cast(f.dataType).as(f.name)
          }
          stringData.select(columns: _*)
        }

        convertedData
      } else {
        sqlContext.createDataFrame(rows, StructType(Seq(StructField("value", StringType))))
      }
    }

    def convertTypes(): Table = {
      val newFields = fields.map { field =>
        val newDataType = field.dataType match {
          case decimal: DecimalType if useDoubleForDecimal => DoubleType
          case date: DateType if useStringForDate => StringType
          case other => other
        }
        field.copy(dataType = newDataType)
      }

      Table(name, partitionColumns, newFields:_*)
    }

    def genData(
      location: String,
      format: String,
      overwrite: Boolean,
      clusterByPartitionColumns: Boolean,
      filterOutNullPartitionValues: Boolean,
      numPartitions: Int): Unit = {
      val mode = if (overwrite) SaveMode.Overwrite else SaveMode.Ignore

      val data = df(format != "text", numPartitions)
      val tempTableName = s"${name}_text"
      data.createOrReplaceTempView(tempTableName)

      val writer = if (partitionColumns.nonEmpty) {
        if (clusterByPartitionColumns) {
          val columnString = data.schema.fields.map { field =>
            field.name
          }.mkString(",")
          val partitionColumnString = partitionColumns.mkString(",")
          val predicates = if (filterOutNullPartitionValues) {
            partitionColumns.map(col => s"$col IS NOT NULL").mkString("WHERE ", " AND ", "")
          } else {
            ""
          }

          val query =
            s"""
               |SELECT
               |  $columnString
               |FROM
               |  $tempTableName
               |$predicates
               |DISTRIBUTE BY
               |  $partitionColumnString
            """.stripMargin
          val grouped = sqlContext.sql(query)
          println(s"Pre-clustering with partitioning columns with query $query.")
          log.info(s"Pre-clustering with partitioning columns with query $query.")
          grouped.write
        } else {
          data.write
        }
      } else {
        // treat non-partitioned tables as "one partition" that we want to coalesce
        if (clusterByPartitionColumns) {
          // in case data has more than maxRecordsPerFile, split into multiple writers to improve datagen speed
          // files will be truncated to maxRecordsPerFile value, so the final result will be the same
          val numRows = data.count
          val maxRecordPerFile = util.Try(sqlContext.getConf("spark.sql.files.maxRecordsPerFile").toInt).getOrElse(0)

          println(s"Data has $numRows rows clustered $clusterByPartitionColumns for $maxRecordPerFile")
          log.info(s"Data has $numRows rows clustered $clusterByPartitionColumns for $maxRecordPerFile")

          if (maxRecordPerFile > 0 && numRows > maxRecordPerFile) {
            val numFiles = (numRows.toDouble/maxRecordPerFile).ceil.toInt
            println(s"Coalescing into $numFiles files")
            log.info(s"Coalescing into $numFiles files")
            data.coalesce(numFiles).write
          } else {
            data.coalesce(1).write
          }
        } else {
          data.write
        }
      }
      writer.format(format).mode(mode)
      if (partitionColumns.nonEmpty) {
        writer.partitionBy(partitionColumns : _*)
      }
      println(s"Generating table $name in database to $location with save mode $mode.")
      log.info(s"Generating table $name in database to $location with save mode $mode.")
      writer.save(location)
      sqlContext.dropTempTable(tempTableName)
    }

    def createExternalTable(location: String, format: String, databaseName: String,
      overwrite: Boolean, discoverPartitions: Boolean = true): Unit = {

      val qualifiedTableName = databaseName + "." + name
      val tableExists = sqlContext.tableNames(databaseName).contains(name)
      if (overwrite) {
        sqlContext.sql(s"DROP TABLE IF EXISTS $databaseName.$name")
      }
      if (!tableExists || overwrite) {
        println(s"Creating external table $name in database $databaseName using data stored in $location.")
        log.info(s"Creating external table $name in database $databaseName using data stored in $location.")
        sqlContext.createExternalTable(qualifiedTableName, location, format)
      }
      if (partitionColumns.nonEmpty && discoverPartitions) {
        println(s"Discovering partitions for table $name.")
        log.info(s"Discovering partitions for table $name.")
        sqlContext.sql(s"ALTER TABLE $databaseName.$name RECOVER PARTITIONS")
      }
    }

    def createTemporaryTable(location: String, format: String): Unit = {
      println(s"Creating temporary table $name using data stored in $location.")
      log.info(s"Creating temporary table $name using data stored in $location.")
      sqlContext.read.format(format).load(location).createOrReplaceTempView(name)
    }

    def analyzeTable(databaseName: String, analyzeColumns: Boolean = false): Unit = {
      println(s"Analyzing table $name.")
      log.info(s"Analyzing table $name.")
      sqlContext.sql(s"ANALYZE TABLE $databaseName.$name COMPUTE STATISTICS")
      if (analyzeColumns) {
        val allColumns = fields.map(_.name).mkString(", ")
        println(s"Analyzing table $name columns $allColumns.")
        log.info(s"Analyzing table $name columns $allColumns.")
        sqlContext.sql(s"ANALYZE TABLE $databaseName.$name COMPUTE STATISTICS FOR COLUMNS $allColumns")
      }
    }
  }

  def genData(
      location: String,
      format: String,
      overwrite: Boolean,
      partitionTables: Boolean,
      clusterByPartitionColumns: Boolean,
      filterOutNullPartitionValues: Boolean,
      tableFilter: String = "",
      numPartitions: Int = 100): Unit = {
    var tablesToBeGenerated = if (partitionTables) {
      tables
    } else {
      tables.map(_.nonPartitioned)
    }

    if (!tableFilter.isEmpty) {
      tablesToBeGenerated = tablesToBeGenerated.filter(_.name == tableFilter)
      if (tablesToBeGenerated.isEmpty) {
        throw new RuntimeException("Bad table name filter: " + tableFilter)
      }
    }

    tablesToBeGenerated.foreach { table =>
      val tableLocation = s"$location/${table.name}"
      table.genData(tableLocation, format, overwrite, clusterByPartitionColumns,
        filterOutNullPartitionValues, numPartitions)
    }
  }

  def createExternalTables(location: String, format: String, databaseName: String,
      overwrite: Boolean, discoverPartitions: Boolean, tableFilter: String = ""): Unit = {

    val filtered = if (tableFilter.isEmpty) {
      tables
    } else {
      tables.filter(_.name == tableFilter)
    }

    sqlContext.sql(s"CREATE DATABASE IF NOT EXISTS $databaseName")
    filtered.foreach { table =>
      val tableLocation = s"$location/${table.name}"
      table.createExternalTable(tableLocation, format, databaseName, overwrite, discoverPartitions)
    }
    sqlContext.sql(s"USE $databaseName")
    println(s"The current database has been set to $databaseName.")
    log.info(s"The current database has been set to $databaseName.")
  }

  def createTemporaryTables(location: String, format: String, tableFilter: String = ""): Unit = {
    val filtered = if (tableFilter.isEmpty) {
      tables
    } else {
      tables.filter(_.name == tableFilter)
    }
    filtered.foreach { table =>
      val tableLocation = s"$location/${table.name}"
      table.createTemporaryTable(tableLocation, format)
    }
  }

  def analyzeTables(databaseName: String, analyzeColumns: Boolean = false, tableFilter: String = ""): Unit = {
    val filtered = if (tableFilter.isEmpty) {
      tables
    } else {
      tables.filter(_.name == tableFilter)
    }
    filtered.foreach { table =>
      table.analyzeTable(databaseName, analyzeColumns)
    }
  }


}
