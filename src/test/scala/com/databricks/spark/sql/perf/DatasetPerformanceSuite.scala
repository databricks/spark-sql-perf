package com.databricks.spark.sql.perf

import org.scalatest.FunSuite

class DatasetPerformanceSuite extends FunSuite {
  ignore("run benchmark") {
    val benchmark = new DatasetPerformance() {
      override val numLongs = 100
    }
    import benchmark._

    val exp = runExperiment(allBenchmarks)
    exp.waitForFinish(10000)
  }
}
