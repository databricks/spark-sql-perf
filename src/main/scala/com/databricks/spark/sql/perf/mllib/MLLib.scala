package com.databricks.spark.sql.perf.mllib

import com.databricks.spark.sql.perf.Benchmark
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

class MLLib(@transient sqlContext: SQLContext)
  extends Benchmark(sqlContext) with Serializable {

  def this() = this(SQLContext.getOrCreate(SparkContext.getOrCreate()))
}
