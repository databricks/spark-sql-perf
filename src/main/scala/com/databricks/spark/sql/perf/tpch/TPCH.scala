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

package com.databricks.spark.sql.perf.tpch
import scala.sys.process._

import com.databricks.spark.sql.perf.{Benchmark, BlockingLineStream, DataGenerator, Table, Tables}
import com.databricks.spark.sql.perf.ExecutionMode.CollectResults
import org.apache.commons.io.IOUtils

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}

class DBGEN(dbgenDir: String, params: Seq[String]) extends DataGenerator {
  val dbgen = s"$dbgenDir/dbgen"
  def generate(sparkContext: SparkContext,name: String, partitions: Int, scaleFactor: String) = {
    val smallTables = Seq("nation", "region")
    val numPartitions = if (partitions > 1 && !smallTables.contains(name)) partitions else 1
    val generatedData = {
      sparkContext.parallelize(1 to numPartitions, numPartitions).flatMap { i =>
        val localToolsDir = if (new java.io.File(dbgen).exists) {
          dbgenDir
        } else if (new java.io.File(s"/$dbgenDir").exists) {
          s"/$dbgenDir"
        } else {
          sys.error(s"Could not find dbgen at $dbgen or /$dbgenDir. Run install")
        }
        val parallel = if (numPartitions > 1) s"-C $partitions -S $i" else ""
        val shortTableNames = Map(
          "customer" -> "c",
          "lineitem" -> "L",
          "nation" -> "n",
          "orders" -> "O",
          "part" -> "P",
          "region" -> "r",
          "supplier" -> "s",
          "partsupp" -> "S"
        )
        val paramsString = params.mkString(" ")
        val commands = Seq(
          "bash", "-c",
          s"cd $localToolsDir && ./dbgen -q $paramsString -T ${shortTableNames(name)} -s $scaleFactor $parallel")
        println(commands)
        BlockingLineStream(commands)
      }.repartition(numPartitions)
    }

    generatedData.setName(s"$name, sf=$scaleFactor, strings")
    generatedData
  }
}

class TPCHTables(
    sqlSession: SparkSession
    sqlContext: SQLContext,
    dbgenDir: String,
    scaleFactor: String,
    useDoubleForDecimal: Boolean = false,
    useStringForDate: Boolean = false,
    generatorParams: Seq[String] = Nil)
    extends Tables(sqlSession, sqlContext, scaleFactor, useDoubleForDecimal, useStringForDate) {
  import sqlContext.implicits._

  val dataGenerator = new DBGEN(dbgenDir, generatorParams)

  val tables = Seq(
    Table("part",
      partitionColumns = "p_brand" :: Nil,
      'p_partkey.long,
      'p_name.string,
      'p_mfgr.string,
      'p_brand.string,
      'p_type.string,
      'p_size.int,
      'p_container.string,
      'p_retailprice.decimal(12, 2),
      'p_comment.string
    ),
    Table("supplier",
      partitionColumns = Nil,
      's_suppkey.long,
      's_name.string,
      's_address.string,
      's_nationkey.long,
      's_phone.string,
      's_acctbal.decimal(12, 2),
      's_comment.string
    ),
    Table("partsupp",
      partitionColumns = Nil,
      'ps_partkey.long,
      'ps_suppkey.long,
      'ps_availqty.int,
      'ps_supplycost.decimal(12, 2),
      'ps_comment.string
    ),
    Table("customer",
      partitionColumns = "c_mktsegment" :: Nil,
      'c_custkey.long,
      'c_name.string,
      'c_address.string,
      'c_nationkey.long,
      'c_phone.string,
      'c_acctbal.decimal(12, 2),
      'c_mktsegment.string,
      'c_comment.string
    ),
    Table("orders",
      partitionColumns = "o_orderdate" :: Nil,
      'o_orderkey.long,
      'o_custkey.long,
      'o_orderstatus.string,
      'o_totalprice.decimal(12, 2),
      'o_orderdate.date,
      'o_orderpriority.string,
      'o_clerk.string,
      'o_shippriority.int,
      'o_comment.string
    ),
    Table("lineitem",
      partitionColumns = "l_shipdate" :: Nil,
      'l_orderkey.long,
      'l_partkey.long,
      'l_suppkey.long,
      'l_linenumber.int,
      'l_quantity.decimal(12, 2),
      'l_extendedprice.decimal(12, 2),
      'l_discount.decimal(12, 2),
      'l_tax.decimal(12, 2),
      'l_returnflag.string,
      'l_linestatus.string,
      'l_shipdate.date,
      'l_commitdate.date,
      'l_receiptdate.date,
      'l_shipinstruct.string,
      'l_shipmode.string,
      'l_comment.string
    ),
    Table("nation",
      partitionColumns = Nil,
      'n_nationkey.long,
      'n_name.string,
      'n_regionkey.long,
      'n_comment.string
    ),
    Table("region",
      partitionColumns = Nil,
      'r_regionkey.long,
      'r_name.string,
      'r_comment.string
    )
  ).map(_.convertTypes())
}

class TPCH(@transient sqlContext: SQLContext)
  extends Benchmark(sqlContext) {

  val queries = (1 to 22).map { q =>
    val queryContent: String = IOUtils.toString(
      getClass().getClassLoader().getResourceAsStream(s"tpch/queries/$q.sql"))
    Query(s"Q$q", queryContent, description = "TPCH Query",
      executionMode = CollectResults)
  }
  val queriesMap = queries.map(q => q.name.split("-").get(0) -> q).toMap
}
