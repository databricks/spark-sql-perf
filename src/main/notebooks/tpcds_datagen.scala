// Databricks notebook source
// MAGIC %md
// MAGIC This notebook generates the TPCDS data using the spark-sql-perf library.

// COMMAND ----------

// MAGIC %python
// MAGIC # IMPORTANT: UPDATE THIS TO THE NUMBER OF WORKER INSTANCES ON THE CLUSTER YOU RUN!!!
// MAGIC num_workers=20

// COMMAND ----------

// Mount the S3 bucket to generate TPCDS data to.
//dbutils.fs.mount("s3a://tpc-benchmarks/", "/mnt/performance-datasets", "sse-s3")

// COMMAND ----------

// IMPORTANT: SET PARAMETERS!!!
// TPCDS Scale factor
val scaleFactor = "1"

// data format.
val format = "parquet"
// If false, float type will be used instead of decimal.
val useDecimal = true
// If false, string type will be used instead of date.
val useDate = true
// If true, rows with nulls in partition key will be thrown away.
val filterNull = false
// If true, partitions will be coalesced into a single file during generation.
val shuffle = true

// s3/dbfs path to generate the data to.
val rootDir = s"/mnt/performance-datasets/tpcds/sf$scaleFactor-$format/useDecimal=$useDecimal,useDate=$useDate,filterNull=$filterNull"
// name of database to be created.
val databaseName = s"tpcds_sf${scaleFactor}" +
  s"""_${if (useDecimal) "with" else "no"}decimal""" +
  s"""_${if (useDate) "with" else "no"}date""" +
  s"""_${if (filterNull) "no" else "with"}nulls"""

// COMMAND ----------

// Create the table schema with the specified parameters.
import com.databricks.spark.sql.perf.tpcds.TPCDSTables
val tables = new TPCDSTables(sqlContext, dsdgenDir = "/tmp/tpcds-kit/tools", scaleFactor = scaleFactor, useDoubleForDecimal = !useDecimal, useStringForDate = !useDate)

// COMMAND ----------

// MAGIC %md
// MAGIC The following two cells are a hack to install dsgen on all executor nodes. **Make sure that the second cell returns empty!!**

// COMMAND ----------

// MAGIC %python
// MAGIC import os
// MAGIC import subprocess
// MAGIC import time
// MAGIC import socket
// MAGIC # First, install a modified version of dsdgen on the cluster.
// MAGIC def install(x):
// MAGIC   p = '/tmp/install.sh'
// MAGIC   if (os.path.exists('/tmp/tpcds-kit/tools/dsdgen')): 
// MAGIC     time.sleep(1)
// MAGIC     return "", ""
// MAGIC   with open(p, 'w') as f:    
// MAGIC     f.write("""#!/bin/bash
// MAGIC     sudo apt-get update
// MAGIC     sudo apt-get -y --force-yes install gcc make flex bison byacc git
// MAGIC 
// MAGIC     cd /tmp/
// MAGIC     git clone https://github.com/databricks/tpcds-kit.git
// MAGIC     cd tpcds-kit/tools/
// MAGIC     make -f Makefile.suite
// MAGIC     /tmp/tpcds-kit/tools/dsdgen -h
// MAGIC     """)
// MAGIC   os.chmod(p, 555)
// MAGIC   p = subprocess.Popen([p], stdin=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
// MAGIC   out, err = p.communicate()
// MAGIC   return socket.gethostname(), out, err
// MAGIC 
// MAGIC # NOTE: Update this for your cluster.
// MAGIC # make sure there is at least one job per node
// MAGIC sc.range(0, num_workers, 1, num_workers).map(install).collect()

// COMMAND ----------

// MAGIC %python
// MAGIC # Checks that dsdgen is on all the nodes. If this result is not empty, rerun the previous step until this is the case.
// MAGIC import os
// MAGIC import socket
// MAGIC import time
// MAGIC def fileThere(x):
// MAGIC   time.sleep(0.1)
// MAGIC   return socket.gethostname(), os.path.exists('/tmp/tpcds-kit/tools/dsdgen'), 
// MAGIC   
// MAGIC sc.range(0, num_workers, 1, num_workers).map(fileThere).filter(lambda x: not x[1]).collect()

// COMMAND ----------

// MAGIC %md
// MAGIC Generate data

// COMMAND ----------

// Data generation tuning:

import org.apache.spark.deploy.SparkHadoopUtil
// Limit the memory used by parquet writer
SparkHadoopUtil.get.conf.set("parquet.memory.pool.ratio", "0.1")
// Compress with snappy:
sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
// TPCDS has around 2000 dates.
spark.conf.set("spark.sql.shuffle.partitions", "2000")
// Don't write too huge files.
sqlContext.setConf("spark.sql.files.maxRecordsPerFile", "20000000")

val dsdgen_partitioned=10000 // recommended for SF10000+.
val dsdgen_nonpartitioned=10 // small tables do not need much parallelism in generation.

// COMMAND ----------

// val tableNames = Array("") // Array("") = generate all.
//val tableNames = Array("call_center", "catalog_page", "catalog_returns", "catalog_sales", "customer", "customer_address", "customer_demographics", "date_dim", "household_demographics", "income_band", "inventory", "item", "promotion", "reason", "ship_mode", "store", "store_returns", "store_sales", "time_dim", "warehouse", "web_page", "web_returns", "web_sales", "web_site") // all tables

// generate all the small dimension tables
val nonPartitionedTables = Array("call_center", "catalog_page", "customer", "customer_address", "customer_demographics", "date_dim", "household_demographics", "income_band", "item", "promotion", "reason", "ship_mode", "store",  "time_dim", "warehouse", "web_page", "web_site")
nonPartitionedTables.foreach { t => {
  tables.genData(
      location = rootDir,
      format = format,
      overwrite = true,
      partitionTables = true,
      clusterByPartitionColumns = shuffle,
      filterOutNullPartitionValues = filterNull,
      tableFilter = t,
      numPartitions = dsdgen_nonpartitioned)
}}
println("Done generating non partitioned tables.")

// leave the biggest/potentially hardest tables to be generated last.
val partitionedTables = Array("inventory", "web_returns", "catalog_returns", "store_returns", "web_sales", "catalog_sales", "store_sales") 
partitionedTables.foreach { t => {
  tables.genData(
      location = rootDir,
      format = format,
      overwrite = true,
      partitionTables = true,
      clusterByPartitionColumns = shuffle,
      filterOutNullPartitionValues = filterNull,
      tableFilter = t,
      numPartitions = dsdgen_partitioned)
}}
println("Done generating partitioned tables.")

// COMMAND ----------

// MAGIC %md
// MAGIC Create database

// COMMAND ----------

sql(s"drop database if exists $databaseName cascade")
sql(s"create database $databaseName")

// COMMAND ----------

sql(s"use $databaseName")

// COMMAND ----------

tables.createExternalTables(rootDir, format, databaseName, overwrite = true, discoverPartitions = true)

// COMMAND ----------

// MAGIC %md
// MAGIC Analyzing tables is needed only if cbo is to be used.

// COMMAND ----------

tables.analyzeTables(databaseName, analyzeColumns = true)
