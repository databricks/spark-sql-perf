package com.databricks.spark.sql.perf.mllib

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.log4j.{Level, Logger}

import org.apache.spark.sql.{Row, SparkSession}

class MLLibSuite extends FunSuite with BeforeAndAfterAll {

  private var sparkSession: SparkSession = _
  var savedLevels: Map[String, Level] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkSession = SparkSession.builder.master("local[2]").appName("MLlib QA").getOrCreate()

    // Travis limits the size of the log file produced by a build. Because we do run a small
    // version of all the ML benchmarks in this suite, we produce a ton of logs. Here we set the
    // log level to ERROR, just for this suite, to avoid displeasing travis.
    savedLevels = Seq("akka", "org", "com.databricks").map { name =>
      val logger = Logger.getLogger(name)
      val curLevel = logger.getLevel
      logger.setLevel(Level.ERROR)
      name -> curLevel
    }.toMap
  }

  override def afterAll(): Unit = {
    savedLevels.foreach { case (name, level) =>
      Logger.getLogger(name).setLevel(level)
    }
    try {
      if (sparkSession != null) {
        sparkSession.stop()
      }
      // To avoid RPC rebinding to the same port, since it doesn't unbind immediately on shutdown
      System.clearProperty("spark.driver.port")
      sparkSession = null
    } finally {
      super.afterAll()
    }
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

  test("test before benchmark methods for pipeline benchmarks.") {
    val benchmarks = MLLib.getBenchmarks(MLLib.getConf(yamlConfig = MLLib.smallConfig))
    benchmarks.foreach { b =>
      b.beforeBenchmark()
    }
  }
}
