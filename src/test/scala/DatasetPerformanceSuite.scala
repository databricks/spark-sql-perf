package com.databricks.spark.sql.perf

import com.databricks.spark.sql.perf.{Benchmark, DatasetPerformance}
import org.apache.spark.sql.hive.test.TestHive
import org.scalatest.FunSuite

class DatasetPerformanceSuite extends FunSuite {
  test("run benchmark") {
    val benchmark = new DatasetPerformance(TestHive) {
      override val numLongs = 100
    }
    import benchmark._

    val exp = runExperiment(allBenchmarks)
    exp.waitForFinish(10000)
  }
}
