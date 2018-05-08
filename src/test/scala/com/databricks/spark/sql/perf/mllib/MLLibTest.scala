package com.databricks.spark.sql.perf.mllib

import org.scalatest.FunSuite

class MLLibTest extends FunSuite {

  test("test MlLib benchmarks with mllib-small.yaml.") {
    MLLib.run(MLLib.smallConfig)
  }

}
