// Databricks notebook source exported at Fri, 11 Sep 2015 06:27:08 UTC
import com.databricks.spark.sql.perf._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types._

sqlContext.read.load("/databricks/spark/sql/sqlPerformanceCompact").registerTempTable("sqlPerformanceCompact")

val timestampWindow = Window.partitionBy("sparkVersion").orderBy($"timestamp".desc)

val normalizeVersion = udf((v: String) => v.stripSuffix("-SNAPSHOT"))

class RecentRuns(prefix: String, inputFunction: DataFrame => DataFrame) {
  val inputData = table("sqlPerformanceCompact") 
    .where($"tags.runtype" === "daily")
    .withColumn("sparkVersion", normalizeVersion($"configuration.sparkVersion"))
    .withColumn("startTime", from_unixtime($"timestamp" / 1000))
    .withColumn("database", $"tags.database")
    .withColumn("jobname", $"tags.jobname")
  
  val recentRuns = inputFunction(inputData)
    .withColumn("runId", denseRank().over(timestampWindow))
    .withColumn("runId", -$"runId")
    .filter($"runId" >= -10) 

  val baseData = recentRuns
    .withColumn("result", explode($"results"))
    .withColumn("day", concat(month($"startTime"), lit("-"), dayofmonth($"startTime"))) 
    .withColumn("runtimeSeconds", runtime / 1000)
    .withColumn("queryName", $"result.name")

  baseData.registerTempTable(s"${prefix}_baseData")

  val smoothed = baseData
    .where($"iteration" !== 1)
    .groupBy("runId", "sparkVersion", "database", "queryName")
    .agg(callUDF("percentile", $"runtimeSeconds".cast("long"), lit(0.5)).as('medianRuntimeSeconds))
    .orderBy($"runId", $"sparkVersion")

  smoothed.registerTempTable(s"${prefix}_smoothed")
}

val tpcds1500 = new RecentRuns("tpcds1500", _.filter($"database" === "tpcds1500"))

// COMMAND ----------

display(tpcds1500.recentRuns.select($"runId", $"sparkVersion", $"startTime", $"timestamp").distinct)

// COMMAND ----------

class GeometricMean extends org.apache.spark.sql.expressions.UserDefinedAggregateFunction {
  def bufferSchema: StructType = StructType(
    StructField("count", LongType) ::
    StructField("product", DoubleType) :: Nil
  )
  
  def dataType: DataType = DoubleType
  
  def deterministic: Boolean = true
  
  def evaluate(buffer: org.apache.spark.sql.Row): Any = {
    math.pow(buffer.getDouble(1), 1.toDouble / buffer.getLong(0))
  }
  
  def initialize(buffer: org.apache.spark.sql.expressions.MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 1.0
  }
  
  def inputSchema: org.apache.spark.sql.types.StructType = 
    StructType(StructField("value", DoubleType) :: Nil)
  
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Long](0) + buffer2.getAs[Long](0)
    buffer1(1) = buffer1.getAs[Double](1) * buffer2.getAs[Double](1)
  }
  
  def update(buffer: MutableAggregationBuffer,input: Row): Unit = {
    buffer(0) = buffer.getAs[Long](0) + 1
    buffer(1) = buffer.getAs[Double](1) * input.getAs[Double](0)
  }
}

val gm = new GeometricMean
sqlContext.udf.register("gm", gm)

// COMMAND ----------

// MAGIC %python spark_versions = ["1.6.0", "1.5.0", "1.4.1"]
// MAGIC 
// MAGIC def plot_single(df, column, height = 30):
// MAGIC   for v in spark_versions:
// MAGIC     runtimes = df[df['sparkVersion'] == v][column]
// MAGIC     padded = ([0.0] * (10 - len(runtimes))) + list(runtimes)
// MAGIC     plt.plot(range(10), padded)
// MAGIC 
// MAGIC   plt.xticks(range(10), list(df[df['sparkVersion'] == v]['runId']))
// MAGIC 
// MAGIC   plt.legend(spark_versions, loc='lower left')
// MAGIC   plt.ylim([0,height])

// COMMAND ----------

// MAGIC %md ## TPCDS - 20 Queries, Total Runtime (Smoothed to reduce outliers)
// MAGIC  -  time for each query = median of 4 iterations
// MAGIC  - dropping 1 warmup iteration

// COMMAND ----------

// MAGIC %python
// MAGIC import matplotlib.pyplot as plt
// MAGIC import numpy as np
// MAGIC from pyspark.sql.functions import *
// MAGIC 
// MAGIC df = table("tpcds1500_smoothed") \
// MAGIC   .groupBy("runId", "sparkVersion") \
// MAGIC   .agg(sum("medianRuntimeSeconds").alias("totalRunTime")) \
// MAGIC   .toPandas()
// MAGIC 
// MAGIC fig = plt.figure(figsize=(12, 2))
// MAGIC plot_single(df, "totalRunTime", height=1000)
// MAGIC display(fig)

// COMMAND ----------

org.apache.spark.SPARK_VERSION

// COMMAND ----------

// MAGIC %python df['sparkVersion'].unique()

// COMMAND ----------

// MAGIC %md ## TPCDS - 20 Queries Geometric Mean
// MAGIC  - time for each query = median of 4 iterations
// MAGIC  - dropping 1 warmup iteration

// COMMAND ----------

// MAGIC %python
// MAGIC import matplotlib.pyplot as plt
// MAGIC from pyspark.sql.functions import *
// MAGIC 
// MAGIC df = table("tpcds1500_smoothed").groupBy("runId", "sparkVersion").agg(expr("gm(medianRuntimeSeconds) AS score")).toPandas()
// MAGIC 
// MAGIC fig = plt.figure(figsize=(12, 2))
// MAGIC plot_single(df, "score")
// MAGIC display(fig)

// COMMAND ----------

// MAGIC %python
// MAGIC import matplotlib.pyplot as plt
// MAGIC from math import ceil, floor
// MAGIC 
// MAGIC def plot_queries(df, queries):
// MAGIC   num_queries = len(queries)
// MAGIC   rows = int(ceil(num_queries / 2))
// MAGIC 
// MAGIC   for idx, name in enumerate(queries):
// MAGIC     data = df[df['queryName'] == name]
// MAGIC     plt.subplot(rows, 2 , idx)
// MAGIC     plt.title(name)
// MAGIC 
// MAGIC 
// MAGIC     plot_single(data, 'medianRuntimeSeconds', 200)

// COMMAND ----------

// MAGIC %md # Interactive Queries

// COMMAND ----------

// MAGIC %python
// MAGIC fig = plt.figure(figsize=(15, 20))
// MAGIC plot_queries(table("tpcds1500_smoothed").toPandas(), ["q19", "q42", "q52", "q55", "q63", "q68", "q73", "q98"])
// MAGIC display(fig)

// COMMAND ----------

// MAGIC %md #Reporting Queries

// COMMAND ----------

// MAGIC %python
// MAGIC fig = plt.figure(figsize=(15, 20))
// MAGIC plot_queries(table("tpcds1500_smoothed").toPandas(), ["q3","q7","q27","q43", "q53", "q89"])
// MAGIC display(fig)

// COMMAND ----------

// MAGIC %md # Deep Analytics

// COMMAND ----------

// MAGIC %python
// MAGIC fig = plt.figure(figsize=(15, 20))
// MAGIC plot_queries(table("tpcds1500_smoothed").toPandas(), ["q34", "q46", "q59", "q65",  "q79", "ss_max"])
// MAGIC display(fig)

// COMMAND ----------

val joins = new RecentRuns("joins", _.filter($"jobname" === "joins.daily"))

// COMMAND ----------

display(table("joins_smoothed").groupBy("runId", "sparkVersion").agg(sum("medianRuntimeSeconds").alias("runtime")))

// COMMAND ----------

// MAGIC %python
// MAGIC df = table("joins_smoothed").groupBy("runId", "sparkVersion").agg(sum("medianRuntimeSeconds").alias("runtime")).toPandas()
// MAGIC 
// MAGIC fig = plt.figure(figsize=(12, 2))
// MAGIC plot_single(df, "runtime", 2500)
// MAGIC display(fig)

// COMMAND ----------

// MAGIC %python
// MAGIC df = table("joins_smoothed").groupBy("runId", "sparkVersion").agg(expr("gm(medianRuntimeSeconds) AS score")).toPandas()
// MAGIC 
// MAGIC fig = plt.figure(figsize=(12, 2))
// MAGIC plot_single(df, "score", 200)
// MAGIC display(fig)

// COMMAND ----------

// MAGIC %python
// MAGIC fig = plt.figure(figsize=(15, 20))
// MAGIC df = table("joins_smoothed").orderBy("queryName").toPandas()
// MAGIC plot_queries(df, list(df['queryName'].unique()))
// MAGIC display(fig)

// COMMAND ----------

// MAGIC %md # Query Plan Breakdown

// COMMAND ----------

val result = tpcds1500.baseData.where($"queryName" === getArgument("queryName", "ss_max")).groupBy("sparkVersion").agg(first($"result.queryExecution")).orderBy("sparkVersion").collect().map {
  case Row(version: String, queryExecution: String) =>
    s"""
     |<h1>$version</h1>
     |<pre>$queryExecution</pre>
     """.stripMargin

}.mkString("<br/>")

displayHTML(result)

// COMMAND ----------

