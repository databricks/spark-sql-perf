package com.databricks.spark.sql.perf.tpch

import com.databricks.spark.sql.perf.Benchmark
import com.databricks.spark.sql.perf.ExecutionMode.CollectResults
import org.apache.commons.io.IOUtils

import org.apache.spark.sql.SQLContext

class TPCH(@transient sqlContext: SQLContext)
  extends Benchmark(sqlContext) {


  val queries = (1 to 22).map { q =>
    val queryContent: String = IOUtils.toString(
      getClass().getClassLoader().getResourceAsStream(s"tpch/queries/$q.sql"))
    Query(s"Q$q", queryContent, description = "TPCH Query",
      executionMode = CollectResults)
  }
  val queriesMap = queries.map(q => q.name.split("-").get(0) -> q).toMap

  val queriesSkew = (1 to 22).map { q =>
    val queryContent: String = IOUtils.toString(
      getClass().getClassLoader().getResourceAsStream(s"tpch/queries.skew/$q.sql"))
    Query(s"Q$q.skew", queryContent, description = "TPCH Query with Skew",
      executionMode = CollectResults)
  }
  val queriesSkewMap = queriesSkew.map(q => q.name.split("-").get(0) -> q).toMap
}
