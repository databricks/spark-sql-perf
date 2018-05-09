package com.databricks.spark.sql.perf.mllib

import org.scalatest.FunSuite

import org.apache.spark.sql.Row

class MLLibTest extends FunSuite {

  test("test MlLib benchmarks with mllib-small.yaml.") {
    val results = MLLib.run(yamlConfig = MLLib.smallConfig)
    val failures = results.na.drop(Seq("failure"))
    if (failures.count() > 0) {
      failures.select("name", "failure.*").collect().foreach {
        case Row(name: String, error: String, message: String) =>
          println(
            s"""There as a failure in the benchmark for $name:
               |  $error ${message.replace("\n", "\n  ")}
             """.stripMargin)
      }
      fail("Unable to run all benchmarks successfully, see console output for more info.")
    }
  }
}
