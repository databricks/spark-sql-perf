package com.databricks.spark.sql.perf

import org.apache.spark.sql.execution.SparkPlan

private[perf] trait BenchmarkableListener {

  /** Called after a query in a [[Benchmarkable]] is planned **/
  def onQueryPlanned(plan: SparkPlan): Unit = {
  }
}
