package com.databricks.spark.sql.perf.mllib

import org.scalatest.FunSuite

class MLLibTest extends FunSuite {

  test("test MlLib benchmarks with mllib-small.yaml.") {
    val results = MLLib.run(yamlConfig = MLLib.smallConfig)
    val failures = results.na.drop(Seq("failure"))
    if (failures.count > 0) {
      val failed = failures.select("name").collect.map(_.getString(0)).mkString(", ")
      fail(s"There were failures in the following benchmarks: $failed.")
    }
  }

}
