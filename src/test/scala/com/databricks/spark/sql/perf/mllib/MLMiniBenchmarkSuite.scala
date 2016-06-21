package com.databricks.spark.sql.perf.mllib

import org.scalatest.FunSuite


class MLMiniBenchmarkSuite extends FunSuite {
  test("run benchmark") {
    val benchmark = new MLMiniBenchmark
    val exp = benchmark.runExperiment(benchmark.all)
    exp.waitForFinish(10000)
  }
}
