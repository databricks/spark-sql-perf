package com.databricks.spark.sql.perf.mllib

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row

class MLLibSuite extends FunSuite with BeforeAndAfterAll {

  private var sc: SparkContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val sparkConf = new SparkConf().setAppName("MLlib QA").setMaster("local[2]")
    sc = SparkContext.getOrCreate(sparkConf)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    if (sc != null) {
      sc.stop()
    }
    // To avoid RPC rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")
    sc = null
  }

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

  test("test before & after benchmark methods for pipeline benchmarks.") {
    val benchmarks = MLLib.getBenchmarks(MLLib.getConf(yamlConfig = MLLib.smallConfig))
    benchmarks.foreach { b =>
      b.beforeBenchmark()
      b.afterBenchmark(sc)
    }
  }
}
