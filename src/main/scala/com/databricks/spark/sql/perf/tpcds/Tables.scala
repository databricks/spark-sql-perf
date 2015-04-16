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

package org.apache.spark.sql.parquet // This is a hack until parquet has better support for partitioning.

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.mapreduce.SparkHadoopMapReduceUtil

import scala.sys.process._


import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat => NewFileOutputFormat}

import com.databricks.spark.sql.perf._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.{OutputCommitter, TaskAttemptContext, RecordWriter, Job}
import org.apache.spark.SerializableWritable
import org.apache.spark.sql.{Column, ColumnName, SQLContext}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import parquet.hadoop.ParquetOutputFormat
import parquet.hadoop.util.ContextUtil


case class TPCDSTableForTest(
    table: Table,
    baseDir: String,
    scaleFactor: Int,
    dsdgenDir: String,
    @transient sqlContext: SQLContext,
    maxRowsPerPartitions: Int = 20 * 1000 * 1000)
  extends TableForTest(table, baseDir, sqlContext) with Serializable with SparkHadoopMapReduceUtil {

  @transient val sparkContext = sqlContext.sparkContext

  val dsdgen = s"$dsdgenDir/dsdgen"

  override def generate(): Unit = {
    val partitions = table.tableType match {
      case PartitionedTable(_) => scaleFactor
      case _ => 1
    }

    val generatedData = {
      sparkContext.parallelize(1 to partitions, partitions).flatMap { i =>
        val localToolsDir = if (new java.io.File(dsdgen).exists) {
          dsdgenDir
        } else if (new java.io.File(s"/$dsdgen").exists) {
          s"/$dsdgenDir"
        } else {
          sys.error(s"Could not find dsdgen at $dsdgen or /$dsdgen. Run install")
        }

        val parallel = if (partitions > 1) s"-parallel $partitions -child $i" else ""
        val commands = Seq(
          "bash", "-c",
          s"cd $localToolsDir && ./dsdgen -table ${table.name} -filter Y -scale $scaleFactor $parallel")
        println(commands)
        commands.lines
      }
    }

    generatedData.setName(s"${table.name}, sf=$scaleFactor, strings")

    val rows = generatedData.mapPartitions { iter =>
      val currentRow = new GenericMutableRow(schema.fields.size)
      iter.map { l =>
        (0 until schema.fields.length).foreach(currentRow.setNullAt)
        l.split("\\|", -1).zipWithIndex.dropRight(1).foreach { case (f, i) => currentRow(i) = f}
        currentRow: Row
      }
    }

    val stringData =
      sqlContext.createDataFrame(
        rows,
        StructType(schema.fields.map(f => StructField(f.name, StringType))))

    val convertedData = {
      val columns = schema.fields.map { f =>
        val columnName = new ColumnName(f.name)
        columnName.cast(f.dataType).as(f.name)
      }
      stringData.select(columns: _*)
    }

    table.tableType match {
      // This is an awful hack... spark sql parquet should support this natively.
      case PartitionedTable(partitioningColumn) =>
        sqlContext.setConf("spark.sql.planner.externalSort", "true")
        val output = convertedData.queryExecution.analyzed.output
        val job = new Job(sqlContext.sparkContext.hadoopConfiguration)

        val writeSupport =
          if (schema.fields.map(_.dataType).forall(_.isPrimitive)) {
            classOf[org.apache.spark.sql.parquet.MutableRowWriteSupport]
          } else {
            classOf[org.apache.spark.sql.parquet.RowWriteSupport]
          }

        ParquetOutputFormat.setWriteSupportClass(job, writeSupport)

        val conf = new SerializableWritable(ContextUtil.getConfiguration(job))
        org.apache.spark.sql.parquet.RowWriteSupport.setSchema(schema.toAttributes, conf.value)

        val partColumnAttr =
          BindReferences.bindReference[Expression](
            output.find(_.name == partitioningColumn).get,
            output)


        // TODO: clusterBy would be faster than orderBy
        val orderedConvertedData =
          convertedData.filter(new Column(partitioningColumn) isNotNull).orderBy(Column(partitioningColumn) asc)
        orderedConvertedData.queryExecution.toRdd.foreachPartition { iter =>
          var writer: RecordWriter[Void, Row] = null
          val getPartition = new InterpretedMutableProjection(Seq(partColumnAttr))
          var currentPartition: Row = null
          var hadoopContext: TaskAttemptContext = null
          var committer: OutputCommitter = null

          var rowCount = 0
          var partition = 0

          while (iter.hasNext) {
            val currentRow = iter.next()

            rowCount += 1
            if (rowCount >= maxRowsPerPartitions) {
              rowCount = 0
              partition += 1
              println(s"Starting partition $partition")
              if (writer != null) {
                writer.close(hadoopContext)
                committer.commitTask(hadoopContext)
              }
              writer = null
            }

            if ((getPartition(currentRow) != currentPartition || writer == null) &&
              !getPartition.currentValue.isNullAt(0)) {
              rowCount = 0
              currentPartition = getPartition.currentValue.copy()
              if (writer != null) {
                writer.close(hadoopContext)
                committer.commitTask(hadoopContext)
              }

              val job = new Job(conf.value)
              val keyType = classOf[Void]
              job.setOutputKeyClass(keyType)
              job.setOutputValueClass(classOf[Row])
              NewFileOutputFormat.setOutputPath(
                job,
                new Path(s"$outputDir/$partitioningColumn=${currentPartition(0)}"))
              val wrappedConf = new SerializableWritable(job.getConfiguration)
              val formatter = new SimpleDateFormat("yyyyMMddHHmm")
              val jobtrackerID = formatter.format(new Date())
              val stageId = partition

              val attemptNumber = 1
              /* "reduce task" <split #> <attempt # = spark task #> */
              val attemptId = newTaskAttemptID(jobtrackerID, partition, isMap = false, partition, attemptNumber)
              hadoopContext = newTaskAttemptContext(wrappedConf.value, attemptId)
              val format = new ParquetOutputFormat[Row]
              committer = format.getOutputCommitter(hadoopContext)
              committer.setupTask(hadoopContext)
              writer = format.getRecordWriter(hadoopContext)

            }
            if (!getPartition.currentValue.isNullAt(0)) {
              writer.write(null, currentRow)
            }
          }
          if (writer != null) {
            writer.close(hadoopContext)
            committer.commitTask(hadoopContext)
          }
        }
        val fs = FileSystem.get(new java.net.URI(outputDir), new Configuration())
        fs.create(new Path(s"$outputDir/_SUCCESS")).close()
      case _ => convertedData.saveAsParquetFile(outputDir)
    }
  }
}

case class Tables(sqlContext: SQLContext) {
  import sqlContext.implicits._

  val tables = Seq(
    /* This is another large table that we don't build yet.
    Table("inventory",
      PartitionedTable("inv_date_sk"),
      'inv_date_sk          .int,
      'inv_item_sk          .int,
      'inv_warehouse_sk     .int,
      'inv_quantity_on_hand .int),*/
    Table("store_sales",
      PartitionedTable("ss_sold_date_sk"),
      'ss_sold_date_sk      .int,
      'ss_sold_time_sk      .int,
      'ss_item_sk           .int,
      'ss_customer_sk       .int,
      'ss_cdemo_sk          .int,
      'ss_hdemo_sk          .int,
      'ss_addr_sk           .int,
      'ss_store_sk          .int,
      'ss_promo_sk          .int,
      'ss_ticket_number     .int,
      'ss_quantity          .int,
      'ss_wholesale_cost    .decimal(7,2),
      'ss_list_price        .decimal(7,2),
      'ss_sales_price       .decimal(7,2),
      'ss_ext_discount_amt  .decimal(7,2),
      'ss_ext_sales_price   .decimal(7,2),
      'ss_ext_wholesale_cost.decimal(7,2),
      'ss_ext_list_price    .decimal(7,2),
      'ss_ext_tax           .decimal(7,2),
      'ss_coupon_amt        .decimal(7,2),
      'ss_net_paid          .decimal(7,2),
      'ss_net_paid_inc_tax  .decimal(7,2),
      'ss_net_profit        .decimal(7,2)),
    Table("customer",
      UnpartitionedTable,
      'c_customer_sk             .int,
      'c_customer_id             .string,
      'c_current_cdemo_sk        .int,
      'c_current_hdemo_sk        .int,
      'c_current_addr_sk         .int,
      'c_first_shipto_date_sk    .int,
      'c_first_sales_date_sk     .int,
      'c_salutation              .string,
      'c_first_name              .string,
      'c_last_name               .string,
      'c_preferred_cust_flag     .string,
      'c_birth_day               .int,
      'c_birth_month             .int,
      'c_birth_year              .int,
      'c_birth_country           .string,
      'c_login                   .string,
      'c_email_address           .string,
      'c_last_review_date        .string),
    Table("customer_address",
      UnpartitionedTable,
      'ca_address_sk             .int,
      'ca_address_id             .string,
      'ca_street_number          .string,
      'ca_street_name            .string,
      'ca_street_type            .string,
      'ca_suite_number           .string,
      'ca_city                   .string,
      'ca_county                 .string,
      'ca_state                  .string,
      'ca_zip                    .string,
      'ca_country                .string,
      'ca_gmt_offset             .decimal(5,2),
      'ca_location_type          .string),
    Table("customer_demographics",
      UnpartitionedTable,
      'cd_demo_sk                .int,
      'cd_gender                 .string,
      'cd_marital_status         .string,
      'cd_education_status       .string,
      'cd_purchase_estimate      .int,
      'cd_credit_rating          .string,
      'cd_dep_count              .int,
      'cd_dep_employed_count     .int,
      'cd_dep_college_count      .int),
    Table("date_dim",
      UnpartitionedTable,
      'd_date_sk                 .int,
      'd_date_id                 .string,
      'd_date                    .string,
      'd_month_seq               .int,
      'd_week_seq                .int,
      'd_quarter_seq             .int,
      'd_year                    .int,
      'd_dow                     .int,
      'd_moy                     .int,
      'd_dom                     .int,
      'd_qoy                     .int,
      'd_fy_year                 .int,
      'd_fy_quarter_seq          .int,
      'd_fy_week_seq             .int,
      'd_day_name                .string,
      'd_quarter_name            .string,
      'd_holiday                 .string,
      'd_weekend                 .string,
      'd_following_holiday       .string,
      'd_first_dom               .int,
      'd_last_dom                .int,
      'd_same_day_ly             .int,
      'd_same_day_lq             .int,
      'd_current_day             .string,
      'd_current_week            .string,
      'd_current_month           .string,
      'd_current_quarter         .string,
      'd_current_year            .string),
    Table("household_demographics",
      UnpartitionedTable,
      'hd_demo_sk                .int,
      'hd_income_band_sk         .int,
      'hd_buy_potential          .string,
      'hd_dep_count              .int,
      'hd_vehicle_count          .int),
    Table("item",
      UnpartitionedTable,
      'i_item_sk                 .int,
      'i_item_id                 .string,
      'i_rec_start_date          .string,
      'i_rec_end_date            .string,
      'i_item_desc               .string,
      'i_current_price           .decimal(7,2),
      'i_wholesale_cost          .decimal(7,2),
      'i_brand_id                .int,
      'i_brand                   .string,
      'i_class_id                .int,
      'i_class                   .string,
      'i_category_id             .int,
      'i_category                .string,
      'i_manufact_id             .int,
      'i_manufact                .string,
      'i_size                    .string,
      'i_formulation             .string,
      'i_color                   .string,
      'i_units                   .string,
      'i_container               .string,
      'i_manager_id              .int,
      'i_product_name            .string),
    Table("promotion",
      UnpartitionedTable,
      'p_promo_sk                .int,
      'p_promo_id                .string,
      'p_start_date_sk           .int,
      'p_end_date_sk             .int,
      'p_item_sk                 .int,
      'p_cost                    .decimal(15,2),
      'p_response_target         .int,
      'p_promo_name              .string,
      'p_channel_dmail           .string,
      'p_channel_email           .string,
      'p_channel_catalog         .string,
      'p_channel_tv              .string,
      'p_channel_radio           .string,
      'p_channel_press           .string,
      'p_channel_event           .string,
      'p_channel_demo            .string,
      'p_channel_details         .string,
      'p_purpose                 .string,
      'p_discount_active         .string),
    Table("store",
      UnpartitionedTable,
      's_store_sk                .int,
      's_store_id                .string,
      's_rec_start_date          .string,
      's_rec_end_date            .string,
      's_closed_date_sk          .int,
      's_store_name              .string,
      's_number_employees        .int,
      's_floor_space             .int,
      's_hours                   .string,
      's_manager                 .string,
      's_market_id               .int,
      's_geography_class         .string,
      's_market_desc             .string,
      's_market_manager          .string,
      's_division_id             .int,
      's_division_name           .string,
      's_company_id              .int,
      's_company_name            .string,
      's_street_number           .string,
      's_street_name             .string,
      's_street_type             .string,
      's_suite_number            .string,
      's_city                    .string,
      's_county                  .string,
      's_state                   .string,
      's_zip                     .string,
      's_country                 .string,
      's_gmt_offset              .decimal(5,2),
      's_tax_precentage          .decimal(5,2)),
    Table("time_dim",
      UnpartitionedTable,
      't_time_sk                 .int,
      't_time_id                 .string,
      't_time                    .int,
      't_hour                    .int,
      't_minute                  .int,
      't_second                  .int,
      't_am_pm                   .string,
      't_shift                   .string,
      't_sub_shift               .string,
      't_meal_time               .string))
}
